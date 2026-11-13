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

**索引**：本地 SQLite（`index.db`），存 `路径 ↔ md5 ↔ 分片消息 id`。定期 `lzma` 压成 `tgnd.db.xz` 传到 TG **置顶**，别的节点启动时拉下来覆盖本地，这样多台机器看到的是同一份目录树。

**分布式锁**：聊天里有一条 `tgnd.lock`，**内容是持有者的 token**（`sha256(sha256(TG_SESSION)[:16] + TG_SESSION)` 取前 32 字节），空闲时内容只写 1 个字节 `0x00`。文件大小（1 / 32）只是拿来先做个快速筛选，真正判断归属靠比对内容：内容是自己的就复用，是别人的就等。写索引前抢锁，写完释放；抢不到每 1.5 秒重试，60 秒超时让进程退出。

**两阶段上传**：先持锁把待传文件全部登记成「上传中」（很快，只写本地索引），**释放锁**，然后不持锁地慢慢传字节，每传完一批再短暂拿一次锁把结果落库。所以传输期间 WebDAV 照样能读写。

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

索引被别的节点锁着时，写操作直接返回 `503` + `Retry-After: 3`，不会卡住客户端。

## 测试

```bash
python tests.py            # 内存版 Telegram，0.4 秒跑完 14 项
python tests.py --real     # 连真 TG（慢，每个写操作约 6 个往返）
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

**读缓存**（`CACHE_MAX_GB`，默认 `0` 关闭）：读过的文件按内容身份留一份在 `cache/blobs/`（`TGND_CACHE` 改目录）。缓存键 = `sha256(索引 hash | 大小 | 分片消息 id 集合)`——任何一次重新上传都会换掉分片消息 id，键随之改变，所以缓存不可能比索引旧，追加写/原地覆盖/删了重传都不需要额外失效；同内容秒传复用同一批分片，键相同照样命中。淘汰按 mtime 升序（命中即刷新）= LRU，写入前先削出空间，另有 60 秒巡检兜底。命中的条目仍校验长度，不符就丢掉重下。缓存是纯目录、零内存状态、无锁，CLI 和 server 共用同一个目录，谁都不依赖谁活着。

**Range 边看边缓存**：播放器拖进度条时，Range 的字节一边发给客户端一边按块对齐写进 `cache/blobs/<hash>-<size>.part`。这是个稀疏文件——写过的区间就是已缓存，「填到哪了」直接问文件系统（`lseek(SEEK_DATA/SEEK_HOLE)`），不另外存进度。请求结束后后台按 `FILL_RATE_KB` 把剩下的洞补齐（有客户端正在读的时候自动降到 1/8，吃空闲带宽），补完校验 hash，通过了再打洞（整块全 0 的区间用 `FALLOC_FL_PUNCH_HOLE` 挖掉，逻辑大小不变、物理占用变小），最后去掉 `.part` 后缀变成正式条目。
块对齐不是多余的：`SEEK_HOLE` 的粒度是文件系统块（`statvfs().f_frsize`，ext4 上 4KB），Range 边界不对齐时只写半个块，整块都会被当成已缓存、块内没写的部分是 0，补洞跳过它，hash 必然对不上。所以只把**完全落在请求范围内的完整块**写进缓存，不足一块的边缘直接丢掉——客户端已经收到了，不为此多下载任何字节，下次补齐时连着那个块一起拉。
正在填的 `.part` 照算进容量预算，但不参与 LRU 淘汰（删了等于扔进度），超过 `PART_TTL`（6 小时）没动过才清掉。

**后台上传**：WebDAV 的 `PUT` 不再等 TG。请求体落 `cache/staging/<hash>` 后立刻返回 `201`/`204`，后台按 `UPLOAD_QUEUE_WORKERS` 路把字节送上去。上传中的文件 `GET` 直接从 staging 读回来，不用等。队列不存路径，每次都从索引里现扫 `status=1`——`MOVE` 改了路径、`DELETE` 删了条目、同名覆盖顶掉旧内容都自动跟上；重启时用 `pending()` + staging 重建队列，没人认领的 staging 会被清掉。失败按指数退避重试 6 次，之后放弃但保留本地副本，索引里仍是「上传中」。

**`tgnd get` 命中缓存**：大于 `GET_LINK_MIN_MB`（默认 16MB）的用硬链接 + `chmod 444`，零拷贝且缓存条目（共 inode）变只读，改不动就不会污染；小文件直接复制。

## 已知限制

- **单个分片 2000MB**：TG 对「一个文件」的限制其实是 4000 个 part × 512KB，2000MB 正好卡线；`SLICE_MB` 超过 2047 会在 `SaveBigFilePartRequest` 上报 `FilePartsInvalidError`（2047MB = 4094 个 part）。整体文件大小不受影响，靠切分片绕开。
- **0 字节文件**：TG 不收空文件，只登记索引，不产生消息。
- **速度**：`send_file` 实测 380–690 KB/s（香港 → DC4），瓶颈在 TG，不在本地。
- **共享账号别乱开**：同一个 session 在两个 IP 上同时用会被吊销 auth key。
- **索引靠快照同步**：两个节点同时写会有极短窗口，靠锁 + 写后立刻推快照收敛；真并发写极端情况下仍需要 `reindex` 兜底。
