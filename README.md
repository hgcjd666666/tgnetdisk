# tgnetdisk

把 Telegram 当网盘：文件存在 TG 频道的消息里，对外只有一个 **WebDAV** 入口。
任何支持 WebDAV 的客户端（rclone、Windows 资源管理器、macOS Finder、Cyberduck、
手机文件管理器）挂上来就能读写，服务器自己负责切片、秒传、缓存和索引。

整体文件大小不限：大文件按 `SLICE_MB`（默认 2000MB）切成多个分片消息，
单条消息受 TG 的 4000 part × 512KB 限制，所以切片别超过 2047MB。

## 怎么工作的

**分片消息**：TG 里每条消息是一个分片，文件名 `<hash>.<序号>`，hash 是内容的
SHAKE-128 128bit 摘要。

```
/docs/a.bin                      ← 索引里的一条记录
    └─ hash = 3f2a…cf81
        ├─ 消息 101  3f2a…cf81.0
        └─ 消息 102  3f2a…cf81.1
```

目录只是索引里的路径，TG 端是平铺的。同一份内容被多个路径引用时只存一份
（**秒传**）：PUT 收完字节算出 hash，命中就只记一条路径，一个字节都不发。

**索引**：本地 SQLite（`tgnd.db`），四张表：

```
content(hash PK, size, parts)          一份内容一行，秒传靠它一对多
files  (path PK, hash, mtime, status)  status=1 表示字节还在本地上传暂存里
dirs   (path PK, mtime)
meta   (k PK, v)
```

本地库是权威：写操作只改本地，后台空闲 `SNAPSHOT_SEC`（默认 10）秒后把索引
压成 `tgnd.db.xz` **原地替换**置顶消息（TG 上永远只有一份快照）。

**两把锁**

| 锁 | 位置 | 作用 |
|---|---|---|
| 本地 | `env` 文件的 flock | 本机只允许一个实例，拿不到按 1/2/4/8/16/32 秒重试 |
| TG | 置顶快照的 caption `lock:<token>` | 全局只允许一个实例，启动时拿、全程持有 |

锁失效（文件被删/inode 变了、caption 被别人改了）→ 停止接受新请求、
等在途任务提交、退出。caption 被清空（`lock:null`）时先尝试重新获取，不退出。

**上传**：PUT 边收边写暂存、边算 hash，收完立刻返回。能秒传就秒传，否则登记
成 `status=1`，字节由后台 worker 切片送上 TG，传完把暂存 `rename` 进读缓存。
`Content-Range` 支持断点续传（切开多次 PUT，收齐才提交）。

**缓存**：`CACHE_MAX_GB`（默认 16G）限制 `$CACHE` 总量。

```
$CACHE/blobs/<hash>      读缓存，LRU 按 mtime 淘汰
$CACHE/staging/<hash>    上传中，只占预算、不参与淘汰
```

读的时候先查 `blobs`，再查 `staging`（上传中的文件照样能读），最后才回 TG 取。

## 安装

需要 Python 3.11+：

```bash
pip install telethon aiohttp
```

需要一个能登录的 Telegram **用户账号** session（bot 拿不到历史消息）。
`TG_SESSION` 可以是字符串 session，也可以是 `.session` 文件路径。

### 配置

复制 `env.example` 为 `env` 并填好。最小配置：

```
TG_API_ID=...
TG_API_HASH=...
TG_SESSION=...
TG_CHAT=-100...
DAV_USER=admin
DAV_PASS=...
```

不填 `TG_CHAT` 时会自动找（找不到就建）一个叫 `tgnd` 的私有群。

### 运行

```bash
python main.py
```

前后台随意，systemd 示例见 `tgnetdisk.service`（`Restart=on-failure`）。

客户端：

```bash
# rclone
rclone config create tg webdav url=http://IP:8090/ vendor=other user=admin pass=...
rclone mount tg: /mnt/tg

# Windows：映射网络驱动器 → http://IP:8090/
# macOS：连接服务器 → http://IP:8090/
```

## WebDAV 支持

| 方法 | 说明 |
|---|---|
| `OPTIONS` | `DAV: 1`、`MS-Author-Via: DAV` |
| `PROPFIND` | `Depth: 0/1`；`infinity` 回 403 + `propfind-finite-depth` |
| `GET` / `HEAD` | `Range`（单区间 206/416）、`ETag`、`If-None-Match`、`If-Modified-Since`、`If-Range` |
| `PUT` | 支持 `Content-Range` 断点续传、`X-Content-Hash` 秒传、`X-OC-Mtime` |
| `DELETE` / `MKCOL` | 429/409 按 RFC |
| `MOVE` / `COPY` | `Destination`、`Overwrite`、412/409 |
| `PROPPATCH` | 空 207（客户端设置属性不会报错） |
| `LOCK` | 不支持，回 405 |

## 秒传（非标准扩展）

内容已经在库里时，客户端不必再传字节：把 hash 报上来，服务端验过货就直接建路径。

第一次（每种内容只做一次）正常 PUT，从响应里抄下 ETag：

```
PUT /big.iso
→ 201, ETag: "8f3a5c0e9b7d2146ad3f88c0b1e24f60-5368709120"
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ hash     ^^^^^^^^^^ 字节数
```

以后同内容换个路径：

```
PUT /copy.iso                         # 空 body，一个字节都不传
X-Content-Hash: 8f3a5c0e9b7d2146ad3f88c0b1e24f60
X-Content-Length: 5368709120          # 可选，给了就和库里的尺寸核对
→ 201, ETag: "8f3a5c0e9b7d2146ad3f88c0b1e24f60-5368709120"
```

- 只认「内容在库里且真能读出来」：已经上了 TG，或者本地缓存里还留着完整字节（后台上传还没跑完）。两者都没有就回 `412`，客户端老老实实传一遍。
- `412` 的 body 区分 `missing`（没见过这个 hash）/ `size`（尺寸对不上）/ `unreadable`（有记录但字节读不出来），响应头回显 `X-Content-Hash`。
- hash 是 SHAKE-128 取前 16 字节的小写 hex，32 个字符。客户端算不出来就从 ETag 里抄。
- 带 body 的 PUT 忽略这个头，一切照旧；空 body 且不带这个头，仍然是建一个 0 字节文件。
- 目标已存在回 `204`，不存在回 `201`，目标是目录回 `409`，只读模式回 `403`。
- 秒传只登记路径，不复制字节：两条路径共用一份内容，删掉一条另一条不受影响。
- 命中「上传中」的内容时，路径立刻可用（本地暂存能读），上 TG 是后台的事，和普通 PUT 完全一致。

## 断点续传（非标准扩展）

只有 `Content-Range` 一种用法，文件总长必须一开始就告诉服务端。

```
PUT /big.iso
Content-Range: bytes 0-10485759/5368709120      # 前 10 MiB
→ 308, X-Upload-Offset: 10485760

PUT /big.iso
Content-Range: bytes 10485760-20971519/5368709120
→ 308, X-Upload-Offset: 20971520

... 最后一块 ...

Content-Range: bytes 5368709119-5368709119/5368709120
→ 201，此刻才算写完
```

- offset 必须**严格等于**服务端已收长度，不连续就回 `400` + 真实的 `X-Upload-Offset`。
- 想先问「传了多长」：`PUT` 空 body + `Content-Range: bytes */<总长>`（或 `bytes */*`），回 `308` + `X-Upload-Offset`，**不写任何字节**。
- 忘了总长、想重来：`Content-Range: bytes 0-N/总长` 会从头覆盖这次上传。
- 收尾前不碰目标路径、不碰 TG，字节只躺在 `$CACHE/staging/.cr-*`。
- 传到一半断线：已收字节留在磁盘上，重连后用 `bytes */总长` 查到真实 offset 接着传。
- **普通 PUT（不带 `Content-Range`）断线就全丢**——没有总长和 offset，服务端无从拼起，只能从头传。要抗断线就必须走 `Content-Range`。
- 客户端不回来了：临时文件在下次启动时清掉；进程内超过一小时没人认领的也自动回收。
- 限制：续传状态在进程内存里，**服务重启后续传作废**（客户端会收到 `400` + `X-Upload-Offset: 0`，只能从头发）。

## 安全

- 不开 `DAV_PASS` 就等于**任何能连到这个端口的人都能读写删**。绑 `0.0.0.0` 前务必设置，或者把 `DAV_BIND` 改成 `127.0.0.1` 走隧道。
- Basic 认证走明文 HTTP，公网部署请上 TLS 反代。
- 单账号、无按路径隔离：拿到密码的人拥有全部权限。
- 能读那个 TG 群的人等于能读全部内容。
- 500 响应只回 `internal error`，不回显异常内容。

## 已知限制

- **速度**：`send_file` 实测 380–690 KB/s（香港 → DC4），瓶颈在 TG。
- **0 字节文件**：TG 不收空文件，只登记索引。
- **切片大小**：`SLICE_MB` 超过 2047 会在 `SaveBigFilePartRequest` 上报 `FilePartsInvalidError`。
- **快照滞后**：崩溃/断电最多丢 `SNAPSHOT_SEC` 秒内的快照推送（改动还在本地库，下次启动会补推）。
- **孤儿分片**：上传到一半进程被杀，TG 上会留下没有索引引用的分片消息，`--reindex` 能扫回来。
- **秒传信 hash**：服务端不校验内容，只核对尺寸。hash 是 SHAKE-128 前 128 位，碰错的概率可以忽略；但客户端把 hash 传错就会建出一条指向别的文件的路径。
- **续传不跨重启**：续传状态在进程内存里，服务重启后重新上传。

## 重建索引

```bash
python main.py --reindex
```

扫频道里全部消息，按文件名重建 `content` 表，并把已经传完但标记还是
`status=1` 的记录改成完成。索引彻底丢了的时候用它兜底。

重建时没有任何路径引用的内容会挂到 `/lost+found/` 下：

- 分片序号连续（0,1,2…）→ `/lost+found/<hash>`，一份完整文件
- 序号不连续（比如只剩 .0 和 .2）→ 每个分片单独一条 `/lost+found/<hash>.<序号>`，
  各自能下载到那一片的真实字节。序号直接写在名字里（`.2` 就是原来的第 2 片），
  配合文件大小就能判断它是单独一片还是拼出来的整份。不会把缺片的后缀硬接成错位的数据流。

## 测试

```bash
python tests.py          # 内存版 Telegram，零网络，89 组用例
python tests.py -k 缓存   # 按名字过滤
```

覆盖的是「协议语义 + 数据层边界」，重点在容易错的地方：

- 路径：`..` 逃逸、NFC 归一、反斜杠、控制字符、`%` `_` 这些 LIKE 通配符、
  特殊字符目录的搬移/删除不能波及邻居
- 索引：覆盖文件↔目录、脏数据恢复、批量、dump/restore 坏库拒绝、老库迁移只跑一次
- 缓存：预算解析、LRU 淘汰顺序、预留额度在异常路径上也要还回去、点文件不参与淘汰
- 摘要与分片：增量哈希跨缓冲区一致、并发读不能错位、`SliceReader` 边界、
  `hash.序号` 解析的畸形输入、缺片时绝不把半截内容写进缓存
- 锁：本地 flock 的 inode 语义、TG 锁的 own/idle/other/gone 四态、被别人抢走要停
- WebDAV：Range 矩阵（含 `bytes=-0`、多区间、越界）、`If-Range` 校验器、
  弱 ETag 与 `*`、PROPFIND 的 Depth 与 XML、MOVE/COPY 的全部拒绝分支、
  认证在方法分发之前、非法路径 400、秒传的全部参数组合、断点续传的状态回收
- 并发：同路径并发写、同内容并发读（单飞）、并发搬移同一源
```
