# tgnetdisk

把 Telegram 当无限容量网盘：文件全存在 TG 频道里，对外提供 **WebDAV** 和 **命令行** 两种入口。

不限制文件格式，**整体文件大小不限**——大文件按 2047MB 切成多个分片消息存，能乱序上传、能续传。真正的限制只有一个：单个分片不能超过 2GB（普通账号的 TG 限额），所以切片大小别调超过 2047。

## 怎么工作的

**文件消息**：TG 里每条消息就是一个分片，文件名 = `<md5>.<分片序号>`，caption 留空。

```
/backup/music/周杰伦/稻香.flac   ← 索引里的一条记录
    └─ md5 = 3f2a…cf81
        ├─ 消息 101  3f2a…cf81.0
        ├─ 消息 102  3f2a…cf81.1
        └─ 消息 103  3f2a…cf81.2
```

目录只是索引里的路径，TG 端是平铺的。同一个 md5 被多个路径引用时只存一份，也就是**秒传**——上传前先算摘要，命中就直接记一条路径，一个字节都不传。

**索引**：本地 SQLite（`tgnd.db`），存 `路径 ↔ hash ↔ 分片消息 id`。本地库是权威：写操作只改本地文件，后台在**空闲 `SNAPSHOT_SEC`（默认 10）秒**后把它 `lzma` 压成 `tgnd.db.xz` 传到 TG 置顶（保留最近 `SNAPSHOT_KEEP` 份）。连写 100 个文件也只在安静下来后推一次，没有新改动就不重复上传。

**互斥（`TGND_LOCK`）**：
- `file`（默认，单设备）：flock 一个 `<db>.lock`。同机的 server 与 cli 本来就是同一个索引文件，进程间互斥用文件锁就够了，写操作不产生任何 TG 往返。
- `tg`（多设备）：锁挂在 TG 置顶快照的 caption 上（`lock:<sha256(盐+token)>`，空闲是 `lock:null`）。写前抢锁、写完推快照并放锁，抢不到每 1.5 秒重试、60 秒超时。跨设备写**必须**用它，否则两边各推各的快照会互相覆盖。
- `none`：只靠 sqlite 自己串行，测试用。

**两阶段上传**：先只改本地索引把文件登记成「上传中」（毫秒级），**立即返回**，字节由后台 worker 慢慢送上去，传完再提交。所以传输期间 WebDAV 照样能读写。

## 安装

需要 Python 3.11+，依赖只有三个：

```bash
pip install telethon aiohttp python-socks
```

还需要一个能登录的 Telegram **用户账号** session（bot 拿不到历史消息，`GetHistoryRequest` 会报 `BotMethodInvalidError`）。

### 配置

复制 `env.example` 为 `env`：

```ini
TG_API_ID=2040
TG_API_HASH=b18441a1ff607e10a989891a5462e627
TG_SESSION=            # StringSession 字符串，或 session 文件路径
TG_CHAT=-100xxxxxxxxxx # 存储用的频道/群 id，账号得是管理员
TG_DEVICE=tgnd-52      # 设备名，多节点各用各的
PORT=8090
DAV_USER=admin
DAV_PASS=改成你的密码
SLICE_MB=2000          # 分片大小，2000 是 4000 part × 512KB 的卡线值
TG_PROXY=              # 可选，http://127.0.0.1:7890
CACHE_MAX_GB=0         # 本地读缓存上限（GB），0 关闭；超限按 LRU 淘汰
```

**同一个账号在不同网络出口跑，必须各自登录一次拿独立 session**。两边共用一个 session 会让 Telegram 判定为泄漏，直接吊销整个 auth key（`AuthKeyDuplicatedError`）。

### 起服务

```bash
python server.py          # WebDAV，监听 $PORT
```

系统服务示例见 `tgnetdisk.service`。

## 命令行

```bash
python cli.py ls /backup                       # 列目录
python cli.py ll /backup                       # 带大小和时间
python cli.py mkdir /backup/new
python cli.py put 本地文件 /backup/name.bin       # 上传
python cli.py put --slice 1900 大文件 /backup/big.bin         # 指定分片大小，无单位按 MB
python cli.py put --slice 1900 --offset 1024 a.bin /backup/big.bin  # 续传：从第 1GB 起，按 1900MB 切片
python cli.py get /backup/big.bin ./下载名
python cli.py rm /backup/big.bin                 # -r 递归
python cli.py mv /backup/a /backup/b
python cli.py cp /backup/a /backup/b
python cli.py stat /backup/big.bin               # 看 md5、分片数、消息 id
```

### 批量上传

```bash
python cli.py pushdir /data/music /backup/music              # 目录内容传到远端
python cli.py pushdir -x cache -x '*.tmp' /data/music /backup/music   # 排除
```

会先登记全部文件（一次持锁），再后台逐个上传。已存在的同名同 md5 文件自动跳过（`--no-skip` 关掉）。日志一行一个文件，可以 `nohup` 挂着跑。

### 维护

```bash
python cli.py reindex        # 重新扫 TG 重建索引（索引丢了/换机器时用），无主消息会归入 /lost+found
```

## WebDAV

用支持 WebDAV 的客户端挂 `http://IP:8090/`，Basic 认证走 `DAV_USER` / `DAV_PASS`。

```bash
# 命令行验证
curl -u admin:密码 -X PROPFIND -H 'Depth: 1' http://IP:8090/
curl -u admin:密码 -T 本地文件 http://IP:8090/远端路径
curl -u admin:密码 -r 1000000-1000099 http://IP:8090/远端路径   # Range，回 206
```

`Accept-Ranges: bytes`，支持单区间 `Range`：播放器拖进度条时只拉那一段，不会从头重下。多区间不实现（按整份返回 200，RFC 9110 允许）；起点越界回 416 + `Content-Range: bytes */大小`。分段偏移在 `iter_content` 里按每个分片的 `document.size` 累加定位到「哪一片 + 片内第几字节」，再对那一片 `iter_download(offset=…)`。

Windows 的「映射网络驱动器」、macOS 的「连接服务器」、安卓的 Solid Explorer / RCX 都能直连。

默认（`TGND_LOCK=file`）写操作只改本地索引，毫秒级返回，不做任何 TG 往返；索引推迟到空闲 `SNAPSHOT_SEC` 秒后由后台推上去。`TGND_LOCK=tg`（多设备）时锁被别的节点占着，写操作会返回 `503` + `Retry-After: 3`，不会卡住客户端。

## 测试

```bash
python tests.py            # 内存版 Telegram，全部用例零网络
python tests.py --real     # 连真 TG（慢）
python tests.py --full     # 连锁和快照一起测
python tests.py -k 秒传     # 按名字过滤
```

`FakeClient` 实现了 `send_file / edit_message / get_messages / iter_messages / delete_messages / iter_download` 等接口，文件放内存，所以整个测试不碰网络。

## 还有个小工具

```bash
python hashdir.py /path/to/dir -o out.json -j 8 -a sha256
```

并行算目录里所有文件的哈希，输出 `{root, algo, count, total, files:{相对路径:{size, hash, mtime}}}`，用来对比两边拷贝是否一致。可用算法：`sha256` `xxh3_128` `xxh3_64` `md5` `blake2b` `sha1` `sha512` `sha3_256`。

## 本地缓存与后台上传

**读缓存**（`CACHE_MAX_GB`，默认 `0` 关闭）：读过的文件按内容身份留一份在 `cache/blobs/`（`TGND_CACHE` 改目录）。缓存键 = `<索引 hash>-<大小>-<分片消息 id 指纹>`。分片指纹不能省：`put --offset` 原地覆盖同样长度的块时，hash 和大小都可能不变、只有分片消息换了，不带指纹就会命中旧内容的缓存。淘汰按 mtime 升序（命中即刷新）= LRU，写入前先削出空间，另有 60 秒巡检兜底。命中的条目仍校验长度，不符就丢掉重下；同一份内容同时只让一个协程回 TG 下载，其他人等它落盘后直接读缓存，不会 N 个客户端拉 N 遍。缓存是纯目录、零内存状态、无锁，CLI 和 server 共用同一个目录，谁都不依赖谁活着。

**Range 边看边缓存**：播放器拖进度条时，Range 的字节一边发给客户端一边按块对齐写进 `cache/blobs/<hash>-<size>.part`。这是个稀疏文件——写过的区间就是已缓存，「填到哪了」直接问文件系统（`lseek(SEEK_DATA/SEEK_HOLE)`），不另外存进度。请求结束后后台按 `FILL_RATE_KB` 把剩下的洞补齐（有客户端正在读的时候自动降到 1/8，吃空闲带宽），补完校验 hash，通过了再打洞（整块全 0 的区间用 `FALLOC_FL_PUNCH_HOLE` 挖掉，逻辑大小不变、物理占用变小），最后去掉 `.part` 后缀变成正式条目。
块对齐不是多余的：`SEEK_HOLE` 的粒度是文件系统块（`statvfs().f_frsize`，ext4 上 4KB），Range 边界不对齐时只写半个块，整块都会被当成已缓存、块内没写的部分是 0，补洞跳过它，hash 必然对不上。所以只把**完全落在请求范围内的完整块**写进缓存，不足一块的边缘直接丢掉——客户端已经收到了，不为此多下载任何字节，下次补齐时连着那个块一起拉。
正在填的 `.part` 照算进容量预算，但不参与 LRU 淘汰（删了等于扔进度），超过 `PART_TTL`（6 小时）没动过才清掉。

**后台上传**：WebDAV 的 `PUT` 不再等 TG。请求体落 `cache/staging/<hash>` 后立刻返回 `201`/`204`，后台按 `UPLOAD_QUEUE_WORKERS` 路把字节送上去。队列不存路径，每次都从索引里现扫 `status=1`——`MOVE` 改了路径、`DELETE` 删了条目、同名覆盖顶掉旧内容都自动跟上；重启时用 `pending()` + staging 重建队列，没人认领的 staging 会被清掉。失败按指数退避一直重试（封顶 300 秒一次），网络恢复后自己继续，不需要重启。

**「上传中」是多设备语义**：`status=1` 表示字节还在这台机器的本地 staging。本机（收到 PUT 的那台）看得到也读得到；别的机器拉快照后看到 `status=1`、本地却没有那份字节，就当作**文件不存在**——列表里不显示、`GET`/`HEAD`/`PROPFIND` 都回 404，`PUT` 同名回 409（不让两台设备同时传同一个路径）。超过 `GHOST_TTL`（24 小时）没动静的记录当作对方早就不在了，允许接管。上传完成时，如果预算装得下，字节直接 `rename` 进 `cache/blobs/` 当读缓存（`TGND_CACHE_ADOPT=0` 可关），刚传完的文件第一次读不用再从 TG 下回来。

**分片级复用**：重传同一个文件时，TG 上已经存在的 slice（含 `sync` 收进 `/lost+found` 的残片）只要字节数和这次该片应有的长度一致，就直接复用、只补缺口。换了 `--slice` 大小就对不上，自动放弃复用。`send_file` 建消息失败时会拿同一个已上传的 ref 重试，不把整个 slice 重传一遍。

**删除与覆盖**：索引里一次摘掉整棵子树（单个事务），TG 消息回收在锁外分批做，删大目录不会抱着写锁等网络；删掉/覆盖掉的记录，对应的本地缓存条目也一并回收（没有其他路径引用时才删）。

**`tgnd get` 命中缓存**：大于 `GET_LINK_MIN_MB`（默认 16MB）的用硬链接 + `chmod 444`，零拷贝且缓存条目（共 inode）变只读，改不动就不会污染；小文件直接复制。

## WebDAV 语义

- `GET`/`HEAD` 带 `ETag`（`<hash>-<大小>`）、`Last-Modified`、`Accept-Ranges`；支持 `If-None-Match` / `If-Modified-Since`（304）和 `If-Range`。
- `Range` 只支持单区间，返回 206 + `Content-Range`；语法错的 `Range` 按 RFC 忽略、回整份，只有起点超出文件才回 416。
- `PROPFIND` 只支持 `Depth: 0/1`，`infinity` 回 403 + `propfind-finite-depth`（整库拼 XML 会让服务端和客户端一起卡死）。
- 只宣告 `DAV: 1`，不实现锁（`LOCK` 回 405，也不再谎报 `supportedlock`）。
- `MOVE`/`COPY` 的目标是已存在的集合、或资源类型不一致时回 409，不会先删目标再操作。
- `PUT` 到不存在的父目录会自动建目录；`PUT` 撞上别的设备的上传中记录回 409。

## 安全

- **认证是 HTTP Basic，明文**。默认监听 `0.0.0.0`；不设 `DAV_PASS` 就等于谁都能读写删（启动时会打警告）。放公网前要么套一层 TLS 反代，要么把 `DAV_BIND` 改成 `127.0.0.1` 只走本机/隧道。
- **单账号**：拿到账号密码的人拥有全部权限，没有按路径的隔离。存储群是私有 megagroup，能读那个群的人等于能读全部内容。
- 路径是 TG 索引里的虚拟路径，不是本机文件系统路径，`..` 会被规范化掉；缓存/上传临时文件都在 `TGND_CACHE` 下，文件名是 hash，不受请求路径控制。
- 并发下载有上限（`MAX_STREAMS`，默认 64，超了回 503），单目录 `PROPFIND` / 目录页有条目上限（`PROPFIND_MAX_ENTRIES`，默认 50000，超了回 507）。
- 内部异常只写日志，500 响应不回显 `str(ex)`。
- `X-OC-Mtime` 只接受 1970..2038 之间的秒数，inf/nan/超大整数直接忽略。

## 已知限制

- **单个分片 2000MB**：TG 对「一个文件」的限制其实是 4000 个 part × 512KB，2000MB 正好卡线；`SLICE_MB` 超过 2047 会在 `SaveBigFilePartRequest` 上报 `FilePartsInvalidError`（2047MB = 4094 个 part）。整体文件大小不受影响，靠切分片绕开。
- **0 字节文件**：TG 不收空文件，只登记索引，不产生消息。
- **速度**：`send_file` 实测 380–690 KB/s（香港 → DC4），瓶颈在 TG，不在本地。
- **共享账号别乱开**：同一个 session 在两个 IP 上同时用会被吊销 auth key。
- **索引的权威在本地**：写操作立即生效在本地库，TG 上的快照是备份，最多滞后 `SNAPSHOT_SEC` 秒（默认 10）。进程被杀 / 磁盘丢了，最多丢这一段时间的改动（本地 `dirty` 标记会在下次启动时先把自己推上去）。
- **多设备写必须开 `TGND_LOCK=tg`**：默认的 `file` 只协调同一台机器上的 server 与 cli。两台设备都用 `file` 各写各的，后推的快照会整库覆盖先推的，对方那份改动就没了。
- **推快照是要压缩整库的**：库大到几百 MB 时，`xz` 压缩本身要几秒，这段时间占 CPU 但不挡任何请求（后台协程做）。
- **上传中但本地缓冲丢了**：索引里会留一条 `status=1` 的幽灵记录，本机看不到、别的机器也看不到，直到超过 `GHOST_TTL` 被接管或被删除。（已知，后续处理）
- **`/lost+found` 残片**：`sync` 收编的无主分片按 hash 命名；序号不连续（中间缺片）的残片 `size` 记 0，下载时会被大小校验挡下来，不交付错位字节。
