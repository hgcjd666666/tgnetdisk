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
| `PUT` | 支持 `Content-Range` 断点续传、`X-OC-Mtime` |
| `DELETE` / `MKCOL` | 429/409 按 RFC |
| `MOVE` / `COPY` | `Destination`、`Overwrite`、412/409 |
| `PROPPATCH` | 空 207（客户端设置属性不会报错） |
| `LOCK` | 不支持，回 405 |

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
python tests.py          # 内存版 Telegram，零网络
python tests.py -k 缓存   # 按名字过滤
```
