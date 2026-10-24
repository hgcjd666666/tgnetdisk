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
SLICE_MB=2047          # 分片大小，2047 是 2GB 内的最大值
TG_PROXY=              # 可选，http://127.0.0.1:7890
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
python cli.py put --slice 5 大文件 /backup/big.bin            # 指定分片大小(MB)
python cli.py put --slice 5 --offset 1024 a.bin /backup/big.bin  # 续传：从第 1GB 起，按 5MB 切片
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
python cli.py reindex        # 重新扫 TG 重建索引（索引丢了/换机器时用）
python cli.py gc --dry-run   # 看有哪些文件消息没人引用
python cli.py gc             # 真删掉
```

## WebDAV

用支持 WebDAV 的客户端挂 `http://IP:8090/`，Basic 认证走 `DAV_USER` / `DAV_PASS`。

```bash
# 命令行验证
curl -u admin:密码 -X PROPFIND -H 'Depth: 1' http://IP:8090/
curl -u admin:密码 -T 本地文件 http://IP:8090/远端路径
```

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

## 已知限制

- **单个分片 2GB**：TG 普通账号对「一个文件」的限额，靠切分片绕开，整体大小不受影响；`SLICE_MB` 别超过 2047。
- **0 字节文件**：TG 不收空文件，只登记索引，不产生消息。
- **速度**：`send_file` 实测 380–690 KB/s（香港 → DC4），瓶颈在 TG，不在本地。
- **共享账号别乱开**：同一个 session 在两个 IP 上同时用会被吊销 auth key。
- **索引靠快照同步**：两个节点同时写会有极短窗口，靠锁 + 写后立刻推快照收敛；真并发写极端情况下仍需要 `reindex` 兜底。
