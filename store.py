"""Telegram 存储后端：TG 消息按 hash 命名，路径索引走可替换的 Backend，快照压缩后传回 TG 置顶。

约定
  TG 文件消息文件名 = "<hash>.<分片序号>"    例  d41d8c...427e.0 / .1
  TG 文件消息 caption = {"i": 序号}
  TG 快照消息      文件名 = "tgnd.db.xz"，caption = {"db":1}，置顶
  Backend 负责 path ↔ hash 与分片 msg_id；默认 SQLite，换成 PostgreSQL 只需再实现一个 Backend
"""
import abc
import asyncio
from contextlib import asynccontextmanager, contextmanager
import functools
import hashlib
import io
import json
import logging

from logger import progress
import lzma
import os
import random
import shutil
import sqlite3
import tempfile
import traceback
from urllib.parse import quote
import time

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.upload import SaveBigFilePartRequest
from telethon.tl.types import DocumentAttributeFilename, InputFileBig

log = logging.getLogger("store")

# 单个分片上限由 TG 的「4000 个 part、每 part 512KB」决定：4000*512KB = 2000MB 整。
# 2047MB 会切成 4094 个 part，直接 FilePartsInvalidError，所以卡着线设 2000。
SLICE = int(os.environ.get("SLICE_MB", "2000")) * 1024 * 1024
PART = int(os.environ.get("UPLOAD_PART_KB", "512")) * 1024   # 单个分块的字节数
WORKERS = int(os.environ.get("UPLOAD_WORKERS", "2"))       # 同时在传的分块数
# 上传缓存目录：默认放脚本旁边的 cache/，不放 /tmp（服务器 /tmp 是 tmpfs，大文件会把内存吃穿）
CACHE_DIR = os.environ.get("TGND_CACHE") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "cache")


def cache_file(name: str) -> str:
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, name)


# ---------- 本地读缓存 ----------
# CACHE_MAX_GB=0（默认）关闭。>0 时把「分片齐、大小对得上」的文件留一份在
# cache/blobs/ 下，下次读同一份内容直接走本地盘，不再碰 TG。
#
# 缓存键 = sha256(索引 hash | 大小 | 分片消息 id 集合)。任何一次重新上传都会换掉
# 分片消息 id，键随之改变，所以缓存不可能比索引旧——追加写、原地覆盖、删了重传
# 都自动失效，不需要额外的失效逻辑；同内容秒传复用同一批分片，键相同，照样命中。
#
# 淘汰按 mtime 升序（命中时 os.utime 刷新）= LRU；写之前先削出空间，另加低频巡检兜底。
# 条目单独放 blobs/ 一层，跟 cache_file() 的临时文件（上传 part、下载暂存）隔开，
# 淘汰逻辑永远不会误删正在用的临时文件。
CACHE_CHUNK = 1 << 20          # 读缓存时的块大小
CACHE_SWEEP_GAP = 60           # 巡检最短间隔（秒）
PART_TTL = 6 * 3600            # 部分缓存多久没动就扔掉（秒）
_cache_swept_at = 0.0


def cache_limit() -> int:
    """上限字节数，<=0 表示关闭。每次读环境变量——env 是运行期才加载的。"""
    try:
        gb = float(os.environ.get("CACHE_MAX_GB", "0").strip() or "0")
    except ValueError:
        return 0
    return int(gb * (1024 ** 3)) if gb > 0 else 0


def blob_dir() -> str:
    d = os.path.join(CACHE_DIR, "blobs")
    os.makedirs(d, exist_ok=True)
    return d


def cache_key(e) -> str:
    """这条记录的内容身份：<hash>-<大小>；拿不到（缓存关闭 / 上传中 / 空文件）返回空串。

    直接用 hash 当名字，肉眼能看出是哪份内容。大小必须带上：`put --offset` 追加
    重传时 hash 会保留首块不变、大小变了，只靠 hash 会把两份内容混成一个缓存。
    """
    if cache_limit() <= 0 or not e or e.is_dir or e.status == 1:
        return ""
    if not part_ids(e.parts) or not e.hash:
        return ""
    return "%s-%d" % (e.hash, e.size or 0)


# 部分缓存：Range 请求边发边填的稀疏文件，收齐校验通过才转正去掉后缀
PART_SUFFIX = ".part"


def partial_path(key: str) -> str:
    return os.path.join(blob_dir(), key + PART_SUFFIX) if key else ""


def partial_open(key: str, size: int):
    """打开（必要时创建）部分缓存文件并保证长度是 size，返回 fd。

    建出来的是稀疏文件：ftruncate 到 size，没写过的区间是洞，
    所以「填到哪了」这件事根本不用另外存，SEEK_HOLE 问文件系统就行。
    """
    p = partial_path(key)
    if not p or size <= 0:
        return None
    try:
        fd = os.open(p, os.O_RDWR | os.O_CREAT, 0o644)
        cur = os.fstat(fd).st_size
        if cur == size:
            return fd
        # 长度对不上说明这条记录的内容换过了，旧的部分文件整个作废
        os.close(fd)
        fd = os.open(p, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
        os.ftruncate(fd, size)
        return fd
    except OSError as ex:
        log.warning("部分缓存打不开 %s: %s", key[:12], ex)
        return None


def first_hole(fd: int, size: int):
    """第一个还没写过的偏移；整份没有洞返回 None。

    走 SEEK_DATA/SEEK_HOLE，开销只跟洞的个数有关，不跟文件大小有关。
    """
    off = 0
    while off < size:
        try:
            d = os.lseek(fd, off, os.SEEK_DATA)
        except OSError:
            return off                  # 从 off 到尾巴全是洞
        if d > off:
            return off                  # [off, d) 是洞
        h = os.lseek(fd, d, os.SEEK_HOLE)
        if h >= size:
            return None
        off = h
    return None


def hash_fd(fd: int, size: int, chunk: int = CACHE_CHUNK) -> str:
    h = hashlib.shake_128()
    off = 0
    while off < size:
        b = os.pread(fd, min(chunk, size - off), off)
        if not b:
            break
        h.update(b)
        off += len(b)
    return h.hexdigest(16)


_LIBC = None


def punch_zeros(fd: int, size: int, chunk: int = CACHE_CHUNK) -> int:
    """把整块全 0 的区间挖成洞，逻辑大小不变、物理占用变小。返回挖掉的字节数。

    只能在「无洞 + hash 校验通过」之后做：挖完以后 SEEK_HOLE 就分不清
    「从没写过」和「写过全 0」了。
    """
    global _LIBC
    if _LIBC is None:
        import ctypes
        _LIBC = ctypes.CDLL("libc.so.6", use_errno=True)
    import ctypes
    mode = 0x01 | 0x02                  # FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE
    freed = 0
    off = 0
    while off < size:
        b = os.pread(fd, min(chunk, size - off), off)
        if not b:
            break
        if not b.strip(b"\0"):
            r = _LIBC.fallocate(fd, mode, ctypes.c_longlong(off),
                                ctypes.c_longlong(len(b)))
            if r == 0:
                freed += len(b)
        off += len(b)
    return freed


def partial_commit(key: str, want_hash: str, size: int) -> bool:
    """把收齐的部分文件转正：无洞 → 校验 hash → 打洞 → rename 去掉后缀。"""
    p = partial_path(key)
    if not p:
        return False
    try:
        fd = os.open(p, os.O_RDWR, 0o644)
    except OSError:
        return False
    try:
        if first_hole(fd, size) is not None:
            return False
        if want_hash:
            got = hash_fd(fd, size)
            if got != want_hash:
                # 分块追加写入的记录只有首块 hash，这种对不上就不要了
                log.warning("部分缓存校验不过，丢弃 %s：期望 %s 实际 %s",
                            key[:16], want_hash[:16], got[:16])
                os.close(fd)
                fd = None
                try:
                    os.unlink(p)
                except OSError:
                    pass
                return False
        freed = punch_zeros(fd, size)
        if freed:
            log.info("缓存打洞 %s：%.1f MB 变成稀疏", key[:16], freed / 1048576)
    finally:
        if fd is not None:
            os.close(fd)
    try:
        os.replace(p, os.path.join(CACHE_DIR, "blobs", key))
    except OSError as ex:
        log.warning("部分缓存转正失败 %s: %s", key[:16], ex)
        return False
    return True


def staging_dir() -> str:
    """待上传文件落盘的地方。写成「落盘即返回」以后，它就是这个功能的磁盘底。"""
    d = os.path.join(CACHE_DIR, "staging")
    os.makedirs(d, exist_ok=True)
    return d


def staging_path(digest: str) -> str:
    return os.path.join(staging_dir(), digest) if digest else ""


def staging_drop(digest: str):
    """收掉一份 staging。传完、被顶替、被删的时候调。"""
    p = staging_path(digest)
    if not p:
        return
    try:
        os.unlink(p)
    except OSError:
        pass


def _scan_dir(d: str, collect: bool):
    items, total = [], 0
    try:
        it = os.scandir(d)
    except OSError:
        return 0, items
    with it:
        for de in it:
            if de.name.startswith("."):
                continue
            try:
                if not de.is_file(follow_symlinks=False):
                    continue
                st = de.stat(follow_symlinks=False)
            except OSError:
                continue
            total += st.st_size
            # 正在填的部分文件也算占用，但不能被淘汰——删了等于把进度扔了
            if collect and not de.name.endswith(PART_SUFFIX):
                items.append((st.st_mtime, st.st_size, de.path))
    return total, items


def _cache_usage():
    """本地占用 = 读缓存 + staging。返回 (总占用, 可淘汰条目)。

    staging 里的文件正在等着上传，只计入占用、不进淘汰列表——不然传一半的
    文件会被自己腾空间的逻辑删掉，那才是真丢数据。
    """
    live, items = _scan_dir(blob_dir(), True)
    pend, _ = _scan_dir(os.path.join(CACHE_DIR, "staging"), False)
    return live + pend, items


def cache_trim(need: int = 0, keep: str = "") -> int:
    """削到 limit 以内并预留 need 字节，由旧到新删。返回释放的字节数。"""
    limit = cache_limit()
    if limit <= 0:
        return 0
    total, items = _cache_usage()
    if total + need <= limit:
        return 0
    items.sort()
    freed = 0
    for _, size, p in items:
        if total + need <= limit:
            break
        if p == keep:
            continue
        try:
            os.unlink(p)
        except OSError:
            continue
        total -= size
        freed += size
    return freed


def staging_ok(size: int) -> bool:
    """这么大的文件还能不能落进 staging。

    有总预算（CACHE_MAX_GB>0）时，读缓存给 staging 让路：先淘汰，淘汰完还是装不下
    就拒绝这次上传；预算关掉时只盯磁盘，留 2G 别把机器写死。
    """
    limit = cache_limit()
    if limit > 0:
        total, _ = _cache_usage()
        if total + size > limit:
            cache_trim(need=size)
            total, _ = _cache_usage()
        return total + size <= limit
    try:
        st = os.statvfs(CACHE_DIR)
    except OSError:
        return True
    avail = st.f_bavail * st.f_frsize
    # 留一点余量别把盘写死；小盘（比如 tmpfs）上按 20% 算，别一律卡 2G
    return avail >= size + min(2 << 30, int(avail * 0.2))


def cache_sweep(gap: float = CACHE_SWEEP_GAP) -> int:
    """低频巡检：超限就按 LRU 删。gap 秒内只真扫一次。"""
    global _cache_swept_at
    if cache_limit() <= 0:
        return 0
    now = time.monotonic()
    if now - _cache_swept_at < gap:
        return 0
    _cache_swept_at = now
    freed = cache_trim(0)
    # 长期没人碰的部分缓存直接扔：洞还在说明补洞要么早失败了要么没人管，
    # 留着只占地方。真要用的时候重新 Range 一遍就能再填。
    stale = 0
    try:
        now_ts = time.time()
        for de in os.scandir(blob_dir()):
            if not de.name.endswith(PART_SUFFIX):
                continue
            try:
                st = de.stat(follow_symlinks=False)
            except OSError:
                continue
            if now_ts - st.st_mtime <= PART_TTL:
                continue
            try:
                os.unlink(de.path)
                stale += st.st_size
            except OSError:
                pass
    except OSError:
        pass
    if stale:
        log.info("清理过期的部分缓存 %.1f MB", stale / (1024.0 ** 2))
    if freed:
        log.info("缓存巡检释放 %.1f MB", freed / (1024.0 ** 2))
    return freed + stale


def cache_commit(tmp: str, key: str, size: int) -> str:
    """把下完整的临时文件原子落进缓存；装不下就丢掉。返回落地路径或空串。"""
    limit = cache_limit()
    if not key or size <= 0 or size > limit:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        return ""
    dst = os.path.join(blob_dir(), key)
    cache_trim(need=size, keep=dst)
    total, _ = _cache_usage()
    if total + size > limit:      # staging 把预算吃满了，读缓存先让路
        try:
            os.unlink(tmp)
        except OSError:
            pass
        return ""
    try:
        # 临时文件同目录 + rename：别的进程要么看到旧的、要么看到完整的，读不到半个文件；
        # 正在读的进程即使条目被淘汰，已打开的 fd 也照样能读完。
        os.replace(tmp, dst)
    except OSError as ex:
        log.warning("缓存落盘失败 %s: %s", key[:12], ex)
        try:
            os.unlink(tmp)
        except OSError:
            pass
        return ""
    log.info("缓存写入 %s (%.1f MB)", key[:12], size / (1024.0 ** 2))
    return dst


DB_PATH = os.environ.get("TGND_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), "tgnd.db"))
BACKEND = os.environ.get("TGND_BACKEND", "sqlite")
DB_NAME = "tgnd.db.xz"
LOCK_NAME = "tgnd.lock"
LOCK_IDLE = "lock:null"
LOCK_PREFIX = "lock:"
# 单机模式：ONLINE=0 时不抢锁、不写 caption，快照也尽量少发。
# 前提是同一时间只有一个节点在写（多节点必须保持 ONLINE=1）。
def is_online() -> bool:
    """每次调用都读环境变量——env 文件是运行期才加载的，不能做成模块常量。"""
    return os.environ.get("ONLINE", "1").strip().lower() not in ("0", "false", "no", "off")
LOCK_TTL = 120                     # 锁超过这么久没释放就视为残留


# ---------- 基础工具 ----------
def norm(p: str) -> str:
    """规范化索引路径：消掉空段与 `.`/`..`。

    不消解 `..` 会让 `PUT /a/../b` 在库里存成一个字面的 "/a/../b"：
    children() 永远列不到它、DELETE /a 也删不掉它，变成谁都看不见的影子数据。
    """
    out = []
    for x in (p or "").split("/"):
        if not x or x == ".":
            continue
        if x == "..":
            if out:
                out.pop()
            continue
        out.append(x)
    return "/" + "/".join(out)


def parent_of(p: str):
    p = norm(p)
    if p == "/":
        return None
    i = p.rfind("/")
    return "/" if i == 0 else p[:i]


def base_of(p: str) -> str:
    return norm(p).rsplit("/", 1)[-1]


def digest_of(path: str, bufsize: int = 4 << 20) -> str:
    """内容摘要：SHAKE-128 取 128 bit（32 hex）。用于识别与校验，也用于秒传。

    32 字符 + ".序号"，稳稳在 TG 的 64 字符文件名额度内，不用再截断。
    """
    h = hashlib.shake_128()
    with open(path, "rb") as f:
        while True:
            b = f.read(bufsize)
            if not b:
                break
            h.update(b)
    return h.hexdigest(16)


def part_id(v) -> int:
    """分片值可能是 msg_id(int)，也可能是历史遗留的 [msg_id, size]。

    JSON 存进 SQLite 后 tuple 会变成 list，所以两种都要认。
    """
    if isinstance(v, (tuple, list)):
        return int(v[0] or 0)
    return int(v or 0)


def part_ids(parts: dict) -> list:
    """按分片序号排好的消息 id 列表。"""
    return [part_id(v) for _, v in sorted((parts or {}).items())]


def load_parts(s: str) -> dict:
    """从 JSON 还原 parts。key 在 JSON 里只能是字符串，必须转回 int——
    否则 sorted(parts) 会按 "0","1","10","2" 排序，10 片以上的文件下载顺序直接错乱。"""
    try:
        d = json.loads(s or "{}")
    except Exception:
        return {}
    try:
        return {int(k): v for k, v in d.items()}
    except (TypeError, ValueError):
        return {}


def dump_parts(parts: dict) -> str:
    """序列化 parts（key 统一成 int 再交给 json）。"""
    if not parts:
        return "{}"
    return json.dumps({int(k): v for k, v in parts.items()})


def is_path_session(val: str) -> bool:
    return val.startswith("/") or val.startswith("./") or val.endswith(".session") or "/" in val


def make_session(val: str):
    return val if is_path_session(val) else StringSession(val)


def is_db_msg(msg) -> bool:
    """快照消息靠文件名认：tgnd.db.xz。"""
    doc = getattr(msg, "document", None)
    if doc is None:
        return False
    for a in doc.attributes:
        if getattr(a, "file_name", None) == DB_NAME:
            return True
    return False


class LockBusy(Exception):
    """索引写锁被别的节点占着，且调用方要求「不等待」时抛出。"""


def is_lock_msg(msg) -> bool:
    doc = getattr(msg, "document", None)
    if doc is None:
        return False
    for a in doc.attributes:
        if getattr(a, "file_name", None) == LOCK_NAME:
            return True
    return False


def parse_lock(msg):
    """返回 (token, ts)。"""
    try:
        d = json.loads((msg.message or "").strip() or "{}")
        return d.get("lock", ""), float(d.get("ts", 0))
    except Exception:
        return "", 0.0


def doc_file_name(msg):
    """取消息里文件的原始文件名（手动上传的那些没按 hash 命名）。"""
    doc = getattr(msg, "document", None)
    if doc is None:
        return ""
    for a in doc.attributes:
        fn = getattr(a, "file_name", None)
        if fn:
            return fn
    return ""


def parse_name(msg):
    """文件名 "<hash>.<序号>" → (hash, 序号)。"""
    doc = getattr(msg, "document", None)
    if doc is None:
        return None
    for a in doc.attributes:
        fn = getattr(a, "file_name", None)
        if not fn or "." not in fn:
            continue
        head, tail = fn.rsplit(".", 1)
        if tail.isdigit() and len(head) == 32:
            return head, int(tail)
    return None


class SliceReader(io.RawIOBase):
    """把文件的一段当成独立文件交给 telethon，流式读，不落盘不进内存。"""

    def __init__(self, f, start: int, length: int):
        self._f = f
        self._start = start
        self._len = length
        f.seek(start)

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self._f.tell() - self._start

    def seek(self, off, whence=0):
        base = {0: 0, 1: self.tell(), 2: self._len}[whence]
        pos = max(0, min(base + off, self._len))
        self._f.seek(self._start + pos)
        return pos

    def read(self, n=-1):
        left = self._len - self.tell()
        if left <= 0:
            return b""
        if n is None or n < 0 or n > left:
            n = left
        return self._f.read(n)

    def readinto(self, b):
        data = self.read(len(b))
        b[:len(data)] = data
        return len(data)

    def fileno(self):
        raise OSError("no fileno")

    def close(self):
        pass


async def upload_parallel(client, src, workers=None, part=PART):
    """并发上传各 part，返回 InputFileBig。"""
    if isinstance(src, str):
        f, own = open(src, "rb"), True
        try:
            size = os.fstat(f.fileno()).st_size
        except OSError:
            f.seek(0, 2)
            size = f.tell()
    else:
        f, own = src, False
        size = f.seek(0, 2)
        f.seek(0)
    n = max(1, (size + part - 1) // part)
    fid = random.randrange(1 << 62)
    sem = asyncio.Semaphore(workers or WORKERS)
    done = 0
    step = max(1, n // 200)

    async def one(i):
        nonlocal done
        async with sem:
            f.seek(i * part)
            data = f.read(part)
            await client(SaveBigFilePartRequest(fid, i, n, data))
        done += 1
        if done % step == 0 or done == n:
            progress(f"{done}/{n} part ({100.0 * done / n:.1f}%, {size / 1048576:.0f} MB)")

    try:
        await asyncio.gather(*[one(i) for i in range(n)])
    finally:
        if own:
            f.close()
    return InputFileBig(fid, n, "blob")


class Entry:
    __slots__ = ("path", "is_dir", "mtime", "hash", "size", "parts", "status",
                 "expect", "skipped")

    def __init__(self, path, is_dir, mtime=0, hash="", size=0, parts=None,
                 status=0, expect=0):
        self.path = path
        self.is_dir = is_dir
        self.mtime = mtime
        self.hash = hash
        self.size = size
        self.parts = parts or {}
        self.status = status          # 0=完成 1=上传中
        self.expect = expect          # 预期分片数
        self.skipped = False          # 上传时命中「已存在且内容相同」

    @property
    def slices(self):
        return [self.parts[k] for k in sorted(self.parts)]

    @property
    def msg_id(self):
        return self.slices[0] if self.parts else 0

    @property
    def name(self):
        return "/" if self.path == "/" else base_of(self.path)

    def __repr__(self):
        return f"<{'D' if self.is_dir else 'F'} {self.path} {self.size} hash={self.hash[:8]} x{len(self.parts)}>"


# ---------- 索引后端接口 ----------
class Backend(abc.ABC):
    """路径索引的抽象。换实现只要实现下面这些方法，业务层不用动。"""

    @abc.abstractmethod
    def get(self, path: str) -> Entry | None: ...

    @abc.abstractmethod
    def children(self, path: str) -> list: ...

    @abc.abstractmethod
    def all_entries(self) -> dict: ...

    @abc.abstractmethod
    def set_file(self, path: str, hash: str, size: int, mtime: int, parts: dict,
                 status: int = 0, expect: int = 0): ...

    @abc.abstractmethod
    def pending(self) -> list: ...

    @abc.abstractmethod
    def set_dir(self, path: str, mtime: int): ...

    @abc.abstractmethod
    def existing_dirs(self, paths: list) -> set:
        """给一批目录路径，返回其中已存在的那些。"""
        ...

    @abc.abstractmethod
    def add_dirs(self, paths: list, mtime: int):
        """批量创建目录（幂等）。"""
        ...

    @abc.abstractmethod
    def remove(self, path: str): ...

    @abc.abstractmethod
    def find_by_hash(self, hash: str) -> list: ...

    @abc.abstractmethod
    def paths_under(self, prefix: str) -> list: ...

    @abc.abstractmethod
    def get_meta(self, k: str, default: str = "") -> str: ...

    @abc.abstractmethod
    def set_meta(self, k: str, v: str): ...

    @abc.abstractmethod
    def dump(self, dst: str): ...

    @abc.abstractmethod
    def restore(self, src: str): ...

    def close(self):
        pass


def _resilient(fn):
    """SQLite 操作遇到别的进程 restore 换文件时的瞬时错误，重试几次。

    现象是 OperationalError("disk I/O error") 或 "database is locked"：
    restore() 会 unlink -wal/-shm 再原子替换主库，正开着的连接下一步
    读写就会撞上。所有方法都幂等（insert or replace / delete / select），
    整体重试一遍是安全的。
    """
    @functools.wraps(fn)
    def wrap(self, *a, **kw):
        last = None
        for _ in range(6):
            try:
                return fn(self, *a, **kw)
            except sqlite3.OperationalError as e:
                msg = str(e).lower()
                if "disk i/o" not in msg and "locked" not in msg:
                    raise
                last = e
                time.sleep(0.02)
        raise last
    return wrap


class SqliteBackend(Backend):
    """SQLite 索引库。每个方法自己开连接、用完就关。

    db 是单个文件，CLI 和服务可能同时活着。常驻一条连接的话，另一边
    restore() 用 os.replace 换掉文件后，这边还攥着旧 inode，往里写的数据
    就成了孤儿。按操作开关连接，每次打开拿到的都是当前那个文件。
    """

    def __init__(self, path: str = DB_PATH):
        self.path = path
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        with self._conn(create=True) as db:
            self._init_db(db)

    @contextmanager
    def _conn(self, create=False):
        """开一条连接，用完就关。

        create=False 时用 mode=rw 打开：文件不存在直接报错，不偷偷建空库。
        restore() 是 unlink + os.replace，中间有个文件不存在的窗口，正撞上时
        重试等它替换完；不重试的话 connect 会建一个空库，接着查询就报
        "no such table"。
        """
        mode = "rwc" if create else "rw"
        uri = "file:" + quote(self.path) + "?mode=" + mode
        db = None
        for _ in range(20):
            try:
                db = sqlite3.connect(uri, uri=True, check_same_thread=False, timeout=10)
                db.execute("PRAGMA journal_mode=WAL")
                if not create and not db.execute(
                        "select 1 from sqlite_master where type='table' and name='files'").fetchone():
                    db.close()
                    db = None
                    time.sleep(0.02)      # 快照替换的空档，等它一下
                    continue
                break
            except sqlite3.OperationalError:
                if db is not None:
                    db.close()
                    db = None
                time.sleep(0.02)
        if db is None:
            raise RuntimeError(f"打不开索引库: {self.path}")
        try:
            yield db
            db.commit()
        finally:
            db.close()

    @staticmethod
    def _init_db(db):
        """建表 + 老库迁移。幂等，初始化后和 restore 之后各调一次。"""
        db.executescript("""
            CREATE TABLE IF NOT EXISTS files(
                path TEXT PRIMARY KEY, hash TEXT NOT NULL, size INTEGER DEFAULT 0,
                mtime INTEGER DEFAULT 0, parts TEXT DEFAULT '{}');
            CREATE TABLE IF NOT EXISTS dirs(path TEXT PRIMARY KEY, mtime INTEGER DEFAULT 0);
            CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
        """)
        cols = {r[1] for r in db.execute("PRAGMA table_info(files)")}
        if "status" not in cols:
            db.execute("ALTER TABLE files ADD COLUMN status INTEGER DEFAULT 0")
        if "expect" not in cols:
            db.execute("ALTER TABLE files ADD COLUMN expect INTEGER DEFAULT 0")
        if "name" in cols and "hash" in cols:
            # 老库还留着已废弃的 name 列，启动时顺手扔掉（SQLite 3.35+）
            try:
                db.execute("ALTER TABLE files DROP COLUMN name")
            except Exception:
                pass
        if "md5" in cols and "hash" not in cols:
            db.execute("ALTER TABLE files RENAME COLUMN md5 TO hash")
        # 索引放在迁移之后建：旧库先改列名，否则 CREATE INDEX 会报 no such column
        db.execute("CREATE INDEX IF NOT EXISTS idx_files_hash ON files(hash)")
        db.commit()

    @_resilient
    def get(self, path):
        p = norm(path)
        if p == "/":
            return Entry("/", True, int(time.time()))
        with self._conn() as db:
            r = db.execute(
                "select path,hash,size,mtime,parts,status,expect from files where path=?", (p,)).fetchone()
            if r:
                return Entry(r[0], False, r[3], r[1], r[2], load_parts(r[4]), r[5], r[6])
            r = db.execute("select path,mtime from dirs where path=?", (p,)).fetchone()
        if r:
            return Entry(r[0], True, r[1])
        return None

    @_resilient
    def children(self, path):
        """列直接子项。用 path 上的范围扫描而不是全表 LIKE，列表页不再随库增长变慢。"""
        p = norm(path)
        pre = (p.rstrip("/") or "") + "/"
        # 上界用「末位 '/' 换成 '0'」而不是 pre+"\uffff"：SQLite 按 UTF-8 字节比较，
        # "\uffff"(EF BF BF) 小于 emoji 等 4 字节字符(F0..)，会把 /a/😀 漏在范围外。
        hi = pre[:-1] + "0"
        out = []
        with self._conn() as db:
            for k, m in db.execute(
                    "select path,mtime from dirs where path>=? and path<?", (pre, hi)):
                rest = k[len(pre):]
                if rest and "/" not in rest:
                    out.append(Entry(k, True, m))
            for row in db.execute(
                    "select path,hash,size,mtime,parts,status,expect from files"
                    " where path>=? and path<?", (pre, hi)):
                rest = row[0][len(pre):]
                if rest and "/" not in rest:
                    out.append(Entry(row[0], False, row[3], row[1], row[2],
                                     load_parts(row[4]), row[5], row[6]))
        out.sort(key=lambda x: (not x.is_dir, x.name.lower()))
        return out

    @_resilient
    def all_entries(self):
        out = {"/": Entry("/", True, int(time.time()))}
        with self._conn() as db:
            for p, m in db.execute("select path,mtime from dirs"):
                out[p] = Entry(p, True, m)
            for p, hash, size, mtime, parts, st, ex in db.execute(
                    "select path,hash,size,mtime,parts,status,expect from files"):
                out[p] = Entry(p, False, mtime, hash, size, load_parts(parts), st, ex)
        return out

    @_resilient
    def set_file(self, path, hash, size, mtime, parts, status=0, expect=0):
        with self._conn() as db:
            db.execute(
                "insert or replace into files(path,hash,size,mtime,parts,status,expect)"
                " values(?,?,?,?,?,?,?)",
                (norm(path), hash, size, mtime, dump_parts(parts), status, expect))

    @_resilient
    def pending(self):
        with self._conn() as db:
            return [tuple(r) for r in db.execute(
                "select path,hash,expect,size,mtime from files where status=1")]

    @_resilient
    def get_meta(self, k, default=""):
        with self._conn() as db:
            r = db.execute("select v from meta where k=?", (k,)).fetchone()
        return r[0] if r else default

    @_resilient
    def set_meta(self, k, v):
        with self._conn() as db:
            db.execute("insert into meta(k,v) values(?,?) "
                       "on conflict(k) do update set v=?", (k, str(v), str(v)))

    @_resilient
    def existing_dirs(self, paths):
        if not paths:
            return set()
        q = ",".join("?" * len(paths))
        with self._conn() as db:
            return {r[0] for r in db.execute(
                f"select path from dirs where path in ({q})", list(paths))}

    @_resilient
    def add_dirs(self, paths, mtime):
        if not paths:
            return
        with self._conn() as db:
            db.executemany(
                "insert or replace into dirs(path,mtime) values(?,?)",
                [(norm(p), mtime) for p in paths])

    @_resilient
    def set_dir(self, path, mtime):
        with self._conn() as db:
            db.execute("insert or replace into dirs(path,mtime) values(?,?)", (norm(path), mtime))

    @_resilient
    def remove(self, path):
        p = norm(path)
        with self._conn() as db:
            db.execute("delete from files where path=?", (p,))
            db.execute("delete from dirs where path=?", (p,))

    @_resilient
    def find_by_hash(self, hash):
        with self._conn() as db:
            return [(r[0], r[1], load_parts(r[2])) for r in db.execute(
                "select path,size,parts from files where hash=?", (hash,))]

    @_resilient
    def paths_under(self, prefix):
        prefix = norm(prefix)
        pre = prefix.rstrip("/") + "/"
        hi = pre[:-1] + "0"        # 同 children()：不能用 "\uffff"，会漏掉 emoji 路径
        with self._conn() as db:
            out = [x[0] for x in db.execute(
                "select path from dirs where path=? or (path>=? and path<?)", (prefix, pre, hi))]
            out += [x[0] for x in db.execute(
                "select path from files where path=? or (path>=? and path<?)", (prefix, pre, hi))]
        return out

    def dump(self, dst):
        """导出快照。WAL 模式下主库文件可能不含最新数据，必须用 backup API 生成一致性副本。

        副本里的 meta.dirty 一律写成 0：快照代表「这份索引已经落库」的稳态。推送方
        dump 时本地的 dirty 还没清，不抹掉的话快照里会带着 dirty=1，拉取方 load()
        看到它会以为本地还有没推上去的改动，白白先推一次快照。
        """
        tmpdb = dst + ".db"
        try:
            with self._conn() as db:
                out = sqlite3.connect(tmpdb)
                db.backup(out)
                out.execute("update meta set v='0' where k='dirty'")
                out.commit()
                out.close()
            with open(tmpdb, "rb") as src, lzma.open(dst, "wb", preset=9) as fp:
                shutil.copyfileobj(src, fp)
        finally:
            try:
                os.unlink(tmpdb)
            except OSError:
                pass

    def restore(self, src):
        """用快照替换本地库。

        先把库解到临时文件并验证（quick_check + 读一次表），确认是有效索引库才替换；
        否则一个坏快照会把本地索引直接清空，连回滚的余地都没有。

        替换后不必管别的进程：它们每个方法自己开连接，下次操作就会打开新文件。
        """
        tmp = self.path + ".restore"
        try:
            with lzma.open(src, "rb") as f, open(tmp, "wb") as out:
                shutil.copyfileobj(f, out)
            chk = sqlite3.connect(tmp)
            try:
                row = chk.execute("PRAGMA quick_check").fetchone()
                if not row or row[0] != "ok":
                    raise RuntimeError(f"快照不是有效的索引库: {row}")
                chk.execute("select count(*) from files").fetchone()
            finally:
                chk.close()
            for suf in ("", "-wal", "-shm"):
                try:
                    os.unlink(self.path + suf)
                except OSError:
                    pass
            os.replace(tmp, self.path)
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass
        with self._conn() as db:
            self._init_db(db)      # 旧快照可能缺列，补一次

    def close(self):
        """没有常驻连接，留个空实现给 Store.close() 调用。"""
        pass


BACKENDS = {"sqlite": SqliteBackend}


def make_backend(name: str | None = None, **kw) -> Backend:
    name = (name or BACKEND).lower()
    if name not in BACKENDS:
        raise SystemExit(f"未知索引后端: {name}（可选 {list(BACKENDS)}）")
    return BACKENDS[name](**kw)


# ---------- 业务层 ----------
class _WriteGuard:
    """`async with self.lock:` 的入口：抢分布式锁 + 本地互斥 + 租约续期。

    WebDAV 连着写几个文件时，只抢一次锁；空闲满 lease 秒后由 _lease_watch
    统一落库并放锁。这样不会每写一次就 edit 两次 TG。
    """

    def __init__(self, store, lease: float = 3.0):
        self.s = store
        self.lease = lease

    async def __aenter__(self):
        s = self.s
        await s.mutex.acquire()
        try:
            if not s.fast and is_online():
                await s.ensure_fresh()
                if not s._holding:
                    await s.acquire()
                if self.lease > 0:
                    s._lease = self.lease
                    s._touch()
                    if s._lease_task is None or s._lease_task.done():
                        s._lease_task = asyncio.create_task(s._lease_watch())
            s._in_guard = True
            return self
        except Exception:
            s.mutex.release()
            raise

    async def __aexit__(self, *exc):
        s = self.s
        s._in_guard = False
        s._touch()                    # 续租约：真正落库留给 _lease_watch
        s.mutex.release()


class Store:
    def __init__(self, client: TelegramClient, chat, backend: Backend | None = None):
        self.client = client
        self.chat = chat
        self.backend = backend or make_backend()
        self.mutex = asyncio.Lock()
        self.lock = _WriteGuard(self)
        self._dirty = False                # 用 setter 会写 meta，启动时别覆盖上次的标记
        self._snap_digest = ""             # 上次推上去的快照内容摘要，相同就跳过
        self.last_id = 0
        self.snap_msg_id = 0
        self.snap_doc_id = 0
        self.lock_msg_id = 0
        self.fast = False                  # True 时跳过分布式锁与快照（只测数据路径）
        self._in_guard = False
        self.token = self._make_token()
        self._holding = False        # 当前是否持有 TG 锁
        self._lease = 0.0            # 租约秒数（>0 表示批量模式）
        self._lease_task = None
        self._last_op = 0.0
        self._gen = 0                 # dirty 代数：快照推送期间又写了东西就不能把 dirty 清掉
        self._replaced = {}           # register 覆盖掉的旧条目，等 upload_pending 成功后清理

    @property
    def entries(self):
        return self.backend.all_entries()

    def get(self, path):
        return self.backend.get(path)

    def children(self, path):
        return self.backend.children(path)

    # ---------- 写入 ----------
    async def _ensure_parents(self, p: str):
        """把 p 的各级父目录补进索引。一次查全链，缺的一次插完。

        父路径里任何一个已经是「文件」就不能再当目录用：否则同一路径同时出现在
        files 和 dirs 两张表里，get() 优先返回文件，子树被永久遮蔽。
        """
        par = parent_of(p)
        chain = []
        while par and par != "/":
            chain.append(par)
            par = parent_of(par)
        if not chain:
            return
        have = self.backend.existing_dirs(chain)
        for d in chain:
            if d in have:
                continue
            e = self.backend.get(d)
            if e is not None and not e.is_dir:
                raise FileExistsError(f"父路径已经是文件: {d}")
        missing = [d for d in reversed(chain) if d not in have]
        if missing:
            self.backend.add_dirs(missing, int(time.time()))

    async def mkdir(self, path: str, _force=False):
        p = norm(path)
        await self.ensure_fresh()
        async with self.lock:
            cur = self.backend.get(p)
            if cur and not _force:
                if not cur.is_dir:
                    raise FileExistsError(f"同名文件已存在: {p}")
                return cur
            await self._ensure_parents(p)
            self.backend.set_dir(p, int(time.time()))
            self.dirty = True
            return self.backend.get(p)

    async def _send_slices(self, local_path: str, tag: str, base_idx: int,
                           size: int, parts: dict, S: int) -> int:
        """把本地文件按 S 切片发上 TG，边发边填 parts，返回发出的片数。

        put_file 和 upload_pending 的字节传输完全一样，抽出来免得两处各改一遍。
        """
        async def send(src, idx_no):
            attrs = [DocumentAttributeFilename(f"{tag}.{idx_no}")]
            try:
                ref = await upload_parallel(self.client, src)
            except Exception as ex:
                log.warning("并行上传失败，退回顺序: %s", ex)
                msg = await self.client.send_file(self.chat, src, force_document=True,
                                                  attributes=attrs)
            else:
                msg = await self.client.send_file(self.chat, ref, force_document=True,
                                                  attributes=attrs)
            parts[idx_no] = msg.id
            self.last_id = max(self.last_id, msg.id)
            return msg

        if size <= S:
            await send(local_path, base_idx)
            return 1
        n = 0
        pos = 0
        with open(local_path, "rb") as f:
            while pos < size:
                length = min(S, size - pos)
                await send(SliceReader(f, pos, length), base_idx + n)
                pos += length
                n += 1
        return n

    async def put_file(self, path: str, local_path: str, mtime: int | None = None,
                       slice_size: int | None = None, append: bool = False,
                       start_index: int | None = None, skip_same: bool = False,
                       total_size: int | None = None):
        p = norm(path)
        blob = os.path.getsize(local_path)      # 本次要传的字节（append 时只是一块）
        size = total_size if (append and total_size is not None) else blob
        S = slice_size or SLICE
        digest = None if append else digest_of(local_path)
        mt = int(mtime) if mtime else int(os.path.getmtime(local_path))
        await self.ensure_fresh()

        # 阶段一：只在索引里登记「上传中」，锁在这几百毫秒内就释放。
        # 注意：这里不删旧内容。新字节还没传上去，先删旧记录/旧消息的话，
        # 阶段二一断网旧文件就永久没了（索引只剩 status=1 的幽灵记录）。
        async with self.lock:
            old = self.backend.get(p)
            if old and not old.is_dir and not append:
                if skip_same and old.status == 0 and old.hash == digest and old.size == size:
                    old.skipped = True
                    log.info("跳过（已存在且内容相同）: %s", p)
                    return old
            await self._ensure_parents(p)

            if blob == 0 and not append:
                self.backend.set_file(p, digest or "", 0, mt, {}, 0, 0)
                self.dirty = True
                log.info("空文件只登记索引: %s", p)
                await self._release_hash(old)
                return self.backend.get(p)

            if digest and not append:
                for opath, osize, oparts in self.backend.find_by_hash(digest):
                    if not self._dedup_source_ok(opath, osize, oparts):
                        continue
                    self.backend.set_file(p, digest, osize, mt, oparts, 0, 0)
                    self.dirty = True
                    log.info("秒传命中 %s (hash %s) ← %s", p, digest[:8], opath)
                    await self._release_hash(old)
                    return self.backend.get(p)

            parts = {}
            if append and old and not old.is_dir:
                parts = dict(old.parts)
                if not digest:
                    digest = old.hash
            base_idx = start_index if start_index is not None else (
                (max(parts) + 1) if parts else 0)
            tag = digest or hashlib.md5(f"{p}{time.time()}".encode()).hexdigest()[:32]
            nparts = max(1, (blob + S - 1) // S) if blob else 0
            self.backend.set_file(p, digest or tag, size, mt, parts, 1, nparts)
            self.dirty = True

        # 阶段二：真正把字节传上 TG，这段时间不持锁
        try:
            await self._send_slices(local_path, tag, base_idx, blob, parts, S)
        except BaseException:
            # 传一半失败：索引恢复成旧内容，旧文件仍可用（新发出去的分片消息成孤儿，下次 reindex 收进 /lost+found）
            async with self.lock:
                if old is not None and not old.is_dir:
                    self.backend.set_file(p, old.hash, old.size, old.mtime,
                                          old.parts, old.status, old.expect)
                elif old is None:
                    self.backend.remove(p)
                self.dirty = True
            raise

        # 阶段三：提交结果，锁只在这几百毫秒内再持一次；旧内容这时才真正删掉
        async with self.lock:
            if digest and not append:
                # 并发上传同一内容：字节都传完了才进到这里，可能别的协程/进程
                # 已经先落地。这时复用它那组分片，把自己刚传的这组消息删掉，
                # 否则同一份内容在 TG 上会存两份。
                for opath, osize, oparts in self.backend.find_by_hash(digest):
                    if opath != p and self._dedup_source_ok(opath, osize, oparts):
                        self.backend.set_file(p, digest, osize, mt, dict(oparts), 0, 0)
                        self.dirty = True
                        await self._release_hash(
                            Entry(p, False, mt, digest, size, dict(parts), 0, nparts))
                        return self.backend.get(p)
            self.backend.set_file(p, digest or tag, size, mt, parts, 0, nparts)
            self.dirty = True
            await self._release_hash(old)
        return self.backend.get(p)

    async def register(self, path: str, local_path: str, digest: str | None = None,
                       size: int | None = None, mtime: int | None = None):
        """阶段一：只登记「待上传」，不传字节。哈希可以由调用方预先算好传进来。"""
        p = norm(path)
        if size is None:
            size = os.path.getsize(local_path)
        if digest is None:
            digest = digest_of(local_path)
        mt = int(mtime) if mtime else int(os.path.getmtime(local_path))
        async with self.mutex:
            old = self.backend.get(p)
            if old and not old.is_dir and old.status == 0 and old.hash == digest and old.size == size:
                old.skipped = True
                return old
            await self._ensure_parents(p)
            if old and not old.is_dir:
                # 字节还没传，先把旧条目留着；upload_pending 成功后再清理旧消息
                self._replaced[p] = old
            if digest:
                for opath, osize, oparts in self.backend.find_by_hash(digest):
                    if not self._dedup_source_ok(opath, osize, oparts):
                        continue
                    self.backend.set_file(p, digest, osize, mt, dict(oparts), 0, 0)
                    self.dirty = True
                    return self.backend.get(p)
            nparts = max(1, (size + SLICE - 1) // SLICE) if size else 0
            self.backend.set_file(p, digest, size, mt, {}, 1, nparts)
            self.dirty = True
            return self.backend.get(p)

    async def upload_pending(self, path: str, local_path: str):
        """阶段二+三：把登记过的文件真正传上 TG，然后提交（锁只在提交那一瞬）。"""
        p = norm(path)
        e = self.backend.get(p)
        if e is None or e.is_dir or e.status == 0:
            return e
        size = e.size
        digest = e.hash
        tag = digest or hashlib.md5(f"{p}{time.time()}".encode()).hexdigest()[:32]
        S = SLICE
        parts: dict[int, int] = {}

        if size:
            await self._send_slices(local_path, tag, 0, size, parts, S)
        orphan = None
        async with self.mutex:
            # 字节传完可能已经过了几分钟，这中间这条记录完全可能被删掉、被同名覆盖。
            # 不复核就提交，会把删掉的路径重新挂回索引，或者拿旧字节顶掉新内容。
            cur = self.backend.get(p)
            if cur is None or cur.is_dir or cur.status != 1:
                orphan = list(parts.values())
            elif digest and cur.hash and cur.hash != digest:
                orphan = list(parts.values())
            else:
                self.backend.set_file(p, digest or tag, size, e.mtime, parts, 0, len(parts))
                self.dirty = True
        if orphan:
            log.info("上传期间记录已失效，丢弃刚传的 %d 个分片: %s", len(orphan), p)
            await self._del_ids(orphan)
            return self.backend.get(p)
        old = self._replaced.pop(p, None)
        if old is not None:
            await self._release_hash(old)
        return self.backend.get(p)

    @staticmethod
    def _dedup_source_ok(opath: str, osize: int, parts: dict) -> bool:
        """能不能拿这条记录当秒传源。

        /lost+found 里的是 reindex 捡回来的残片（size 记 0、分片可能不全），
        拿它秒传会造出缺片的记录；上传中的记录（status=1）parts 也还没提交。
        """
        if not parts or not osize or osize <= 0:
            return False
        if norm(opath).startswith("/lost+found/"):
            return False
        return True

    async def _release_hash(self, e):
        """删掉条目 e 的分片消息——只删「没有任何其他条目引用」的那些 id。

        引用计数按消息 id 而不是按 hash：同一 hash 可能有重传的多个批次，
        粗粒度按 hash 会让旧批次残片永远删不掉（重传用的已经是新消息）。
        """
        if not e or e.is_dir or not e.parts:
            return
        mine = part_ids(e.parts)
        if not mine:
            return
        referenced = set()
        for _, _, oparts in self.backend.find_by_hash(e.hash):
            referenced.update(part_ids(oparts))
        doomed = [mid for mid in mine if mid not in referenced]
        if not doomed:
            return
        await self._del_ids(doomed)
        log.info("删除 %s 的 %d 个分片消息", e.path, len(doomed))

    async def _drop(self, path: str, hard=True):
        """删记录；hard 时若该 hash 没有别的路径引用，连 TG 消息一起删。"""
        p = norm(path)
        e = self.backend.get(p)
        self.backend.remove(p)
        self.dirty = True
        if hard:
            await self._release_hash(e)

    async def delete(self, path: str, recursive=True):
        p = norm(path)
        await self.ensure_fresh()
        async with self.lock:
            e = self.backend.get(p)
            if not e:
                raise FileNotFoundError(p)
            if e.is_dir:
                if self.children(p) and not recursive:
                    raise OSError("目录非空")
                kids = [x for x in self.backend.paths_under(p) if x != p]
                for k in sorted(kids, key=len, reverse=True):
                    await self._drop(k)
                await self._drop(p)
            else:
                await self._drop(p)

    async def move(self, src: str, dst: str):
        s, d = norm(src), norm(dst)
        if s == d:
            return
        if d.startswith(s.rstrip("/") + "/"):
            # 移到自己的子树里：先写新路径再删旧路径，会把刚写的全删光
            raise OSError(f"不能把 {s} 移动到它自己的子目录 {d} 下")
        if d == "/" or s.startswith(d.rstrip("/") + "/"):
            # 目标是自己的祖先（含根）：WebDAV 层会先删目标再移动，整棵源子树跟着没了
            raise OSError(f"不能把 {s} 移动到它的祖先目录 {d}")
        await self.ensure_fresh()
        if not self.backend.get(s):
            raise FileNotFoundError(s)
        async with self.lock:
            await self._ensure_parents(d)          # 目标父目录不存在就补上，否则子树挂不上
            rows = [self.backend.get(x) for x in self.backend.paths_under(s)]
            rows = [x for x in rows if x]
            for x in rows:
                newp = d + x.path[len(s):]
                if x.is_dir:
                    self.backend.set_dir(newp, x.mtime)
                else:
                    self.backend.set_file(newp, x.hash, x.size, x.mtime, x.parts,
                                        x.status, x.expect)
            for x in rows:
                self.backend.remove(x.path)
            self.dirty = True

    async def copy(self, src: str, dst: str):
        s, d = norm(src), norm(dst)
        if s == d:
            return
        if d.startswith(s.rstrip("/") + "/"):
            raise OSError(f"不能把 {s} 复制到它自己的子目录 {d} 下")
        if d == "/" or s.startswith(d.rstrip("/") + "/"):
            raise OSError(f"不能把 {s} 复制到它的祖先目录 {d}")
        await self.ensure_fresh()
        if not self.backend.get(s):
            raise FileNotFoundError(s)
        async with self.lock:
            await self._ensure_parents(d)
            rows = [self.backend.get(x) for x in self.backend.paths_under(s)]
            for x in rows:
                if not x:
                    continue
                newp = d + x.path[len(s):]
                if x.is_dir:
                    self.backend.set_dir(newp, x.mtime)
                else:
                    self.backend.set_file(newp, x.hash, x.size, x.mtime, x.parts,
                                        x.status, x.expect)
            self.dirty = True

    # ---------- 读取内容 ----------
    def cached_path(self, path: str) -> str:
        """命中本地读缓存就返回它，否则空串（命中会顺手刷新 LRU）。

        返回的是共享缓存池里的文件：随时可能被淘汰，别拿它当最终结果交出去，
        只适合本进程立刻 copyfile / 打开读。
        """
        e = self.backend.get(path)
        if not e or e.is_dir or e.status == 1:
            return ""
        key = cache_key(e)
        if not key:
            return ""
        p = os.path.join(CACHE_DIR, "blobs", key)
        try:
            if e.size and os.path.getsize(p) != e.size:
                return ""
            os.utime(p, None)
        except OSError:
            return ""
        return p

    async def iter_content(self, path: str, verify: bool = True,
                           offset: int = 0, length: int | None = None):
        """按分片顺序流式吐出内容；verify=True 时边下边算 hash，结束时跟索引比对。

        offset/length 是给 HTTP Range 用的：只吐这一段，并且不写缓存、不校验全文件
        摘要——手上只有一段字节，算不出整体 hash。分片缺失照样报错。

        开了本地缓存（CACHE_MAX_GB>0）多走一步：命中直接读本地盘，未命中边下边落盘。
        """
        e = self.backend.get(path)
        if not e or e.is_dir or e.status == 1:
            raise FileNotFoundError(path)
        offset = max(0, int(offset or 0))
        ranged = offset > 0 or length is not None
        if e.size:
            offset = min(offset, e.size)
        if length is None:
            want = (e.size - offset) if (ranged and e.size) else None
        else:
            want = max(0, int(length))
            if e.size:
                want = min(want, e.size - offset)
        if ranged and want == 0:
            return
        key = "" if ranged else cache_key(e)
        cache_sweep()
        if key:
            cp = os.path.join(CACHE_DIR, "blobs", key)
            try:
                f = await asyncio.to_thread(open, cp, "rb")
            except OSError:
                f = None
            if f is not None:
                # 先比长度再吐字节：残片一旦 yield 出去就收不回来了，
                # 客户端会拿到「坏前缀 + 重下内容」拼起来的怪东西。
                try:
                    fsize = await asyncio.to_thread(os.path.getsize, cp)
                except OSError:
                    fsize = -1
                if e.size and fsize != e.size:
                    log.warning("缓存条目长度不符，弃用重下: %s 本地 %d 字节 / 记录 %s",
                                path, fsize, e.size)
                    f.close()
                    try:
                        os.unlink(cp)
                    except OSError:
                        pass
                else:
                    await asyncio.to_thread(os.utime, cp, None)     # 刷新 LRU 位置
                    try:
                        while True:
                            b = await asyncio.to_thread(f.read, CACHE_CHUNK)
                            if not b:
                                break
                            yield b
                    finally:
                        f.close()
                    return
        h = hashlib.shake_128()
        got = 0
        skip = offset
        left = want
        tmp = None
        fh = None
        if key:
            try:
                fd, tmp = tempfile.mkstemp(prefix=".dl-", dir=blob_dir())
                fh = os.fdopen(fd, "wb")
            except OSError as ex:
                log.warning("缓存临时文件创建失败: %s", ex)
                tmp = fh = None
        # 分片按序号排好，再按批预取消息对象：一片一次 get_messages 的话，
        # 一个 20 片的大文件光取消息就要 20 个 RTT（到 DC4 单程 250ms）。
        order = []
        for k in sorted(e.parts):
            order.append((k, part_id(e.parts[k])))
        batch = 50
        missing = []
        try:
            for i in range(0, len(order), batch):
                if left is not None and left <= 0:
                    break
                chunk = order[i:i + batch]
                try:
                    msgs = await self.client.get_messages(self.chat, ids=[mid for _, mid in chunk])
                except Exception as ex:
                    log.warning("取分片消息失败 %s: %s", path, ex)
                    msgs = []
                by_id = {m.id: m for m in (msgs or []) if m}
                for k, mid in chunk:
                    if left is not None and left <= 0:
                        break
                    m = by_id.get(mid)
                    if not m or not getattr(m, "document", None):
                        log.warning("分片缺失: %s idx=%s msg=%s", path, k, mid)
                        missing.append(k)
                        continue
                    # 分片大小不固定（上传时可能用过别的 slice_size），所以定位起点
                    # 只能按消息上记的 document.size 一片片累加，整片跳过时不下字节。
                    psize = getattr(m.document, "size", 0) or 0
                    if skip:
                        if skip >= psize:
                            skip -= psize
                            continue
                        inner, skip = skip, 0
                    else:
                        inner = 0
                    async for piece in self.client.iter_download(m, offset=inner, limit=left):
                        if not ranged:
                            h.update(piece)
                        got += len(piece)
                        if left is not None:
                            left -= len(piece)
                        if fh is not None:
                            fh.write(piece)
                        yield piece
                        if left is not None and left <= 0:
                            break
            if missing:
                # 静默跳过会交付一个截断但「看起来成功」的文件，下载端必须知道失败。
                raise IOError(f"{path} 缺 {len(missing)} 个分片 (idx {sorted(missing)[:10]}, "
                              f"共 {len(order)} 片)，只读到 {got} 字节")
            if not ranged:
                if e.size and got != e.size:
                    # 大小不符是硬错误：交付出去的就是残缺文件
                    raise IOError(f"{path} 下载大小不符: 实际 {got} / 记录 {e.size}")
                if verify and e.hash:
                    d = h.hexdigest(16)
                    if d != e.hash:
                        # 分块追加写入（put --offset/--length）的记录只有首块的 hash，
                        # 内容合法但对不上，这里只告警；真正的数据缺失由上面的分片/大小检查兜住。
                        log.warning("下载校验不一致: %s 期望 %s 实际 %s（%d 字节 / 记录 %s 字节）",
                                    path, e.hash, d, got, e.size)
                # 分片齐、大小对，才把这份内容留在本地
                if fh is not None:
                    try:
                        fh.close()
                    except OSError:
                        pass
                    fh = None
                    cache_commit(tmp, key, got)
                    tmp = None
        finally:
            # 客户端中途断开（GeneratorExit）或出错：临时文件必须清掉
            if fh is not None:
                try:
                    fh.close()
                except OSError:
                    pass
            if tmp:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass

    async def _del_ids(self, ids):
        ids = [i for i in ids if i]
        for k in range(0, len(ids), 100):
            try:
                await self.client.delete_messages(self.chat, ids[k:k + 100])
            except Exception as e:
                log.warning("删除消息失败: %s", e)

    # ---------- 同步 / 快照 ----------
    async def sync(self, full: bool = True):
        """扫描 TG 消息，补全 hash → 分片映射，并记录最新快照消息。"""
        parts_by_hash = {}
        size_by_hash = {}                 # 每个 hash 扫到的分片总字节数，收编 lost+found 时用
        manual = {}
        snap_max, snap_cnt = 0, 0
        n = 0
        last = 0 if full else int(self.backend.get_meta("last_id", "0") or 0)
        try:
            pinned_id = await self._pinned_id()
        except Exception:
            pinned_id = None
        kw = {"reverse": True}
        if last:
            kw["min_id"] = last
        async for m in self.client.iter_messages(self.chat, limit=None, **kw):
            n += 1
            if is_db_msg(m):
                # 只有置顶的那条（或我们记下的 snap_msg_id）才是真快照。
                # 手动上传一个碰巧叫 tgnd.db.xz 的文件不能被当成快照丢掉。
                if (pinned_id is not None and m.id == pinned_id) or m.id == self.snap_msg_id:
                    snap_cnt += 1
                    snap_max = max(snap_max, m.id)
                else:
                    fname = doc_file_name(m)
                    manual[m.id] = (fname, getattr(m.document, "size", 0))
                continue
            nm = parse_name(m)
            if not nm:
                fname = doc_file_name(m)
                # 索引快照、锁文件是我们自己的，不算手动上传
                if fname and not fname.startswith("tgnd.") and not is_db_msg(m):
                    manual[m.id] = (fname, getattr(m.document, "size", 0))
                continue
            hash, idx = nm
            parts_by_hash.setdefault(hash, {})[idx] = m.id
            size_by_hash[hash] = size_by_hash.get(hash, 0) + int(
                getattr(getattr(m, "document", None), "size", 0) or 0)
            if m.id > self.last_id:
                self.last_id = m.id
            if m.id > last:
                last = m.id
        for hash, parts in parts_by_hash.items():
            for opath, osize, oparts in self.backend.find_by_hash(hash):
                if oparts == parts:
                    continue
                # 正常 sync（消息被补或被删）必然与已有 parts 有交集；完全不重合
                # 说明频道里混进了同名的外来消息（比如手动上传的文件恰好叫 hash.0），
                # 拿它覆盖记录会把真实文件的内容换掉。
                if oparts and not (set(part_ids(oparts)) & set(parts.values())):
                    log.warning("hash %s 扫到的分片与索引记录完全不重合，跳过覆盖（疑似同名外来消息）",
                                hash[:8])
                    continue
                e = self.backend.get(opath)
                # mtime 用原记录的：改成 now 会让 WebDAV 客户端以为文件被修改过而重传
                self.backend.set_file(opath, hash, osize,
                                      e.mtime if e else int(time.time()), parts, 0, 0)
        self.backend.set_meta("last_id", last)
        for path, hash, expect, size, mtime in (self.backend.pending() if full else []):
            got = parts_by_hash.get(hash, {})
            if expect and len(got) >= expect:
                self.backend.set_file(path, hash, size, mtime, got, 0, expect)
                log.info("补记完成的上传: %s (%d 片)", path, len(got))
        if full:
            # 扫到分片、但索引里没有任何条目认领的 hash → 收进 /lost+found（仿 ext4），别让数据凭空消失。
            # 注意：必须在 pending 补记之后做，否则上传中的记录会被误判成无主。
            # 认领关系按 (hash, 消息 id) 判定：同一 hash 可能有多批消息（重传过），
            # 只看 hash 会把「hash 已认领、但这批消息 id 没被引用」的漏掉。
            claimed = set()
            for e in self.backend.all_entries().values():
                if e.is_dir or not e.hash:
                    continue
                for v in e.parts.values():
                    claimed.add((e.hash, part_id(v)))
            orphan = {}
            for h, parts in parts_by_hash.items():
                if not h:
                    continue
                left = {idx: mid for idx, mid in parts.items() if (h, mid) not in claimed}
                if left:
                    orphan[h] = left
            if manual:
                now = int(time.time())
                self.backend.set_dir("/lost+found", now)
                for mid, (fname, msize) in manual.items():
                    p = f"/lost+found/{fname}"
                    k = 1
                    while self.backend.get(p):
                        p = f"/lost+found/{fname}.{k}"
                        k += 1
                    self.backend.set_file(p, "", msize, now, {0: mid}, 0, 1)
                log.info("手动上传的消息归入 /lost+found：%d 个", len(manual))
            if orphan:
                now = int(time.time())
                self.backend.set_dir("/lost+found", now)
                for h, parts in orphan.items():
                    # size 用扫到的分片总字节：记 0 的话下载时没法校验完整性
                    self.backend.set_file(f"/lost+found/{h}", h, size_by_hash.get(h, 0),
                                          now, parts, 0, len(parts))
                log.info("无主消息归入 /lost+found：%d 个 hash，%d 片",
                         len(orphan), sum(len(v) for v in orphan.values()))
        if snap_max:
            self.snap_msg_id = snap_max
        log.info("扫描 %d 条消息，%d 个 hash，快照 %d 条", n, len(parts_by_hash), snap_cnt)
        return n

    def _remember_snap_doc(self, m):
        """记下快照消息的 document.id：ensure_fresh 靠它判断远端快照有没有换过。

        推完快照不更新它，下次写操作就会把自己的快照当成「别人更新的」再下载一遍。
        """
        doc_id = getattr(getattr(m, "document", None), "id", 0)
        if doc_id:
            self.snap_doc_id = doc_id

    async def _push_snapshot(self, tmp: str, caption: str):
        """把压缩好的快照推上去：能就地替换置顶那条就替换，否则新发一条并置顶。

        返回 (msg, replaced)：msg 是消息对象或已有消息 id，replaced 表示是否原地替换。
        """
        if self.snap_msg_id:
            try:
                m = await self.client.edit_message(
                    self.chat, self.snap_msg_id, file=tmp, force_document=True,
                    attributes=[DocumentAttributeFilename(DB_NAME)], text=caption)
                self._remember_snap_doc(m)
                return self.snap_msg_id, True
            except Exception as e:
                log.warning("就地替换失败，改为新发一条: %s", e)
        msg = await self.client.send_file(
            self.chat, tmp, force_document=True, caption=caption,
            attributes=[DocumentAttributeFilename(DB_NAME)])
        old = self.snap_msg_id
        self.snap_msg_id = msg.id
        self._remember_snap_doc(msg)
        if old and old != msg.id:
            await self._del_ids([old])
        try:
            await self.client.pin_message(self.chat, msg, notify=False)
        except Exception as e:
            log.warning("置顶失败: %s", e)
        return msg, False

    async def snapshot(self, final: bool = False, grab_lock: bool = False):
        """把索引压成快照传到 TG，置顶并删掉旧的。

        final=True 时顺便把 caption 改回 lock:null 并清持有标记——
        一次 edit 同时完成「落库 + 释放锁」，少一次 TG 写。

        grab_lock=True 才先抢锁。acquire() 内部建快照时必须传 False，
        否则 acquire → snapshot → acquire 互相递归，空频道上直接卡死。
        """
        if grab_lock and is_online() and not self._holding:
            await self.acquire()
        cap = (LOCK_IDLE if is_online() else "") if final else self._lock_text()
        fd, tmp = tempfile.mkstemp(prefix="tgnd_db_", suffix=".xz")
        os.close(fd)
        try:
            gen = self._gen
            self.backend.dump(tmp)
            size = os.path.getsize(tmp)
            msg, replaced = await self._push_snapshot(tmp, cap)
            if final:
                self._holding = False
            if self._gen == gen:          # 推送期间又写了东西就不能把 dirty 清掉
                self.dirty = False
            if replaced:
                log.info("索引快照已就地替换 (msg %s, %d 字节)", msg, size)
                return None
            log.info("索引快照已上传 (msg %d, %d 字节)", msg.id, size)
            return msg
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass

    async def snapshot_locked(self, nonblock: bool = False):
        """压缩快照 → 抢锁 → 一次 edit 完成「上传 + 放锁」。锁内只有一次网络写。

        nonblock=True 时锁被别的节点占着就立刻抛 LockBusy（给 WebDAV 的收尾用，
        否则每个写请求都要在锁上等满 acquire 的 timeout）。
        """
        fd, tmp = tempfile.mkstemp(prefix="tgnd_db_", suffix=".xz")
        os.close(fd)
        try:
            gen = self._gen
            self.backend.dump(tmp)                 # 本地压缩，锁外做
            size = os.path.getsize(tmp)
            with open(tmp, "rb") as fp:
                digest = hashlib.md5(fp.read()).hexdigest()
            if digest == self._snap_digest:
                if self._gen == gen:
                    self.dirty = False              # 内容没变，一次网络都不发
                return self.snap_msg_id
            if is_online() and not self._holding:
                await self.acquire(nonblock=nonblock)
            try:
                msg, replaced = await self._push_snapshot(tmp, LOCK_IDLE)
                self._holding = False
                self._snap_digest = digest          # 推成功才记：失败后同样的内容还要能重试
                if self._gen == gen:
                    self.dirty = False
                mid = msg if replaced else msg.id
                log.info("快照已%s并放锁 (msg %s, %d 字节)", "就地替换" if replaced else "上传", mid, size)
                return mid
            finally:
                if is_online() and self._holding:
                    await self.release()
                else:
                    self._holding = False
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass

    async def _find_snapshot(self):
        """优先从置顶消息里拿快照（锁也置顶，所以这里要按文件名挑）。"""
        for m in await self._pinned():
            if is_db_msg(m):
                return m
        try:
            from telethon.tl.functions.channels import GetFullChannelRequest
            ent = await self.client.get_entity(self.chat)
            full = await self.client(GetFullChannelRequest(ent))
            pid = getattr(full.full_chat, "pinned_msg_id", None)
            if pid:
                m = await self.client.get_messages(self.chat, ids=pid)
                if m and is_db_msg(m):
                    return m
        except Exception as e:
            log.warning("读取置顶失败: %s", e)
        async for m in self.client.iter_messages(self.chat):
            if is_db_msg(m):
                return m
        return None

    async def _pinned_id(self):
        from telethon.tl.functions.channels import GetFullChannelRequest
        ent = await self.client.get_entity(self.chat)
        full = await self.client(GetFullChannelRequest(ent))
        return getattr(full.full_chat, "pinned_msg_id", None)

    @staticmethod
    def _make_token() -> str:
        """锁标识：盐由 TG_SESSION 派生，再和节点名一起哈希。

        TGND_NODE 用来区分同一台机器上的入口（server / cli）：同一个 session 下
        两者 token 必须不同，否则 acquire 会把对方持有的锁当成自己的（cur == mine
        直接 return），两个进程同时写索引。
        """
        sess = os.environ.get("TG_SESSION", "")
        node = os.environ.get("TGND_NODE", "")
        salt = hashlib.sha256((sess + "|" + node).encode()).hexdigest()[:16]
        return hashlib.sha256((salt + sess + node).encode()).hexdigest()[:32]

    async def _pinned(self, limit: int = 50):
        """只看置顶消息。全量扫聊天记录只应出现在显式 reindex 里。"""
        try:
            from telethon.tl.types import InputMessagesFilterPinned
            msgs = await self.client.get_messages(
                self.chat, filter=InputMessagesFilterPinned(), limit=limit)
            return [m for m in msgs if m]
        except Exception as e:
            log.warning("读取置顶消息失败: %s", e)
            return []

    @property
    def dirty(self) -> bool:
        return self._dirty

    @dirty.setter
    def dirty(self, v: bool):
        """dirty 落进 meta：进程重启后还能知道「上次有没有没推上去的改动」。"""
        self._dirty = bool(v)
        if v:
            self._gen += 1
        try:
            self.backend.set_meta("dirty", "1" if v else "0")
        except Exception:
            pass

    @staticmethod
    def _who() -> str:
        """抢锁/放锁的调用来源，方便定位到底是谁在频繁动锁。"""
        frames = traceback.extract_stack()[:-2]
        for fr in reversed(frames):
            if not fr.filename.endswith("store.py"):
                return f"{os.path.basename(fr.filename)}:{fr.lineno} {fr.name}"
        return "store"

    def _lock_text(self) -> str:
        """锁状态写在快照消息的 caption 里：空闲 lock:null，锁定 lock:<hash>。单机模式留空。"""
        if not is_online():
            return ""
        return (LOCK_PREFIX + self.token) if self._holding else LOCK_IDLE

    async def _snap_msg(self):
        """置顶的那条快照消息（锁状态就在它的 caption 上）。

        先按自己记下的 id 直读：刚推完快照时它一定可读，不必等置顶过滤器刷新。
        """
        if self.snap_msg_id:
            try:
                m = await self.client.get_messages(self.chat, ids=self.snap_msg_id)
                if m and is_db_msg(m):
                    return m
            except Exception as e:
                log.warning("按 id 读取快照消息失败: %s", e)
        for m in await self._pinned():
            if is_db_msg(m):
                return m
        return None

    @staticmethod
    def _cap(msg) -> str:
        return (getattr(msg, "message", "") or "").strip() if msg else ""

    async def acquire(self, timeout: int = 60, nonblock: bool = False):
        """锁 = 置顶快照消息的 caption。

        空闲时是 `lock:null`，被占用时是 `lock:<sha256(盐+token)>`。
        token 本身是 TG_SESSION 的 sha256 派生值，不暴露 session。
        """
        if not is_online():                     # 单机模式：不碰 TG，直接认为持有
            self._holding = True
            return
        mine = LOCK_PREFIX + self.token
        end = time.time() + timeout
        bootstrapped = False
        while True:
            m = await self._snap_msg()
            if m is None:
                # 锁就挂在快照消息上：没有快照 = 没人持锁，先发一条空快照。
                # 这里只能调 grab_lock=False（acquire 里再抢锁就是递归死锁）。
                if not bootstrapped:
                    bootstrapped = True
                    log.info("频道里没有快照消息，先建一条再抢锁")
                    await self.snapshot(grab_lock=False)
                    await asyncio.sleep(0.5)   # 给 TG 一点时间让新消息可读
                    continue
                m = await self._find_snapshot()    # 置顶读不到时全量扫一次兜底
                if m is None:
                    raise RuntimeError("频道里没有可用的索引快照，无法加锁（先 sync 一次）")
            cur = self._cap(m)
            if cur == mine:                     # 已经是自己持有的，别再 edit（会报未修改）
                self.snap_msg_id = m.id
                self._holding = True
                return
            if cur in ("", LOCK_IDLE):
                await self.client.edit_message(self.chat, m.id, text=mine)
                # 回读确认，防止两个节点同时写
                m2 = await self.client.get_messages(self.chat, ids=m.id)
                if self._cap(m2) == mine:
                    self.snap_msg_id = m.id
                    self._holding = True
                    log.info("锁 msg %d 空闲 → 占用", m.id)
                    return
            if nonblock:
                raise LockBusy("索引写锁被另一个节点持有")
            if time.time() > end:
                log.error("等待索引写锁超过 %d 秒，判定异常退出", timeout)
                raise TimeoutError("索引正被另一个节点写入，等待超时")
            await asyncio.sleep(1.5)

    async def release(self):
        """把 caption 改回 lock:null。"""
        if not is_online():
            self._holding = False
            return
        if not self._holding or not self.snap_msg_id:
            self._holding = False
            return
        try:
            await self.client.edit_message(self.chat, self.snap_msg_id, text=LOCK_IDLE)
        except Exception as e:
            log.warning("释放锁失败: %s", e)
        self._holding = False

    async def _clear_own_stale_lock(self, timeout: int = 120):
        """启动时：锁是自己的残留就置空，是别人的就轮询等它释放。单机模式跳过。"""
        if not is_online():
            return
        end = time.time() + timeout
        mine = LOCK_PREFIX + self.token
        waited = False
        while True:
            try:
                m = await self._snap_msg()
                if not m:
                    return
                cur = self._cap(m)
                if cur in ("", LOCK_IDLE):
                    return
                if cur == mine:
                    log.warning("启动时清理自己残留的锁 msg %d", m.id)
                    await self.client.edit_message(self.chat, m.id, text=LOCK_IDLE)
                    return
            except Exception as e:
                log.warning("启动检查锁失败: %s", e)
                return
            if time.time() > end:
                log.warning("启动时锁仍被其他节点持有，先继续（写操作时会再等）")
                return
            if not waited:
                log.info("启动时锁被其他节点持有，轮询等待…")
                waited = True
            await asyncio.sleep(2)

    def _touch(self):
        """记录一次写操作时间，租约靠它判断「空闲多久了」。"""
        self._last_op = time.time()

    async def _lease_watch(self):
        """租约守护：空闲满 lease 秒才落库 + 放锁。"""
        while True:
            await asyncio.sleep(0.3)
            if self._in_guard:                 # 正在写，别动
                continue
            if time.time() - self._last_op < self._lease:
                continue
            log.info("租约空闲满 %.1fs，落库并释放锁", self._lease)
            try:
                if self.dirty:
                    await self.snapshot()
            except Exception as e:
                log.warning("租约到期写快照失败: %s", e)
            finally:
                await self.release()
            return

    async def wait_released(self, timeout: float = 300):
        """等租约真正释放（批量任务跑完、进程退出前调用）。"""
        t = self._lease_task
        if t and not t.done():
            try:
                await asyncio.wait_for(t, timeout)
            except asyncio.TimeoutError:
                log.warning("等待租约释放超时 %.0fs", timeout)

    @asynccontextmanager
    async def guard(self, lease: float = 0.0, timeout: int = 60):
        """写操作统一入口。

        lease=0（默认）：抢锁 → 干完 → 写快照 → 放锁。
        lease>0（批量模式）：抢到锁就一直拿着，期间不写快照也不放锁，
                            空闲满 lease 秒后由 _lease_watch 统一落库并释放。
        """
        if self._in_guard:                      # 重入
            self._touch()
            yield
            return
        if not is_online():                          # 单机：不碰锁，只在退出时落库
            self._in_guard = True
            try:
                yield
            finally:
                if self.dirty:
                    try:
                        await self.snapshot()
                    except Exception as e:
                        log.warning("快照上传失败: %s", e)
                self._in_guard = False
            return
        self._in_guard = True
        try:
            await self.ensure_fresh()
            if is_online() and not self._holding:
                await self.acquire(timeout=timeout, nonblock=(timeout <= 0))
            elif not is_online():
                self._holding = True
        except Exception:
            self._in_guard = False
            raise

        if lease > 0:
            self._lease = lease
            self._touch()
            if self._lease_task is None or self._lease_task.done():
                self._lease_task = asyncio.create_task(self._lease_watch())
            try:
                yield
            finally:
                self._touch()
                self._in_guard = False
            return

        try:
            yield
        finally:
            try:
                if self.dirty:
                    try:
                        await self.snapshot()
                    except Exception as e:
                        log.warning("快照上传失败: %s", e)
            finally:
                await self.release()
                self._in_guard = False

    async def ensure_fresh(self):
        """写操作前先看 TG 上的快照有没有被别人更新，有就先拉下来。单机模式直接跳过。"""
        if not is_online():
            return False
        # 快照用 edit_message 原地替换，消息 id 不变，靠 document.id 判断内容是否换过
        if self._holding and self._lease:
            return False                       # 批量模式下锁一直是自己的，不用反复查
        try:
            pid = await self._pinned_id()
            if not pid:
                return False
            m = await self.client.get_messages(self.chat, ids=pid)
        except Exception as e:
            log.warning("检查置顶失败: %s", e)
            return False
        if not m or not is_db_msg(m):
            return False
        doc_id = getattr(getattr(m, "document", None), "id", 0)
        if self.snap_doc_id and doc_id == self.snap_doc_id and pid == self.snap_msg_id:
            return False
        log.info("检测到索引快照更新 (doc %s → %s)，先同步", self.snap_doc_id, doc_id)
        await self.load(pid)
        return True

    async def load(self, pid: int | None = None):
        """从 TG 拉最新快照覆盖本地索引。

        但如果本地库里记着「上次有改动没推上去」，先把自己推上去，否则会被旧快照覆盖。
        """
        if self.backend.get_meta("dirty") == "1":
            log.info("本地有未落库的改动，先推快照")
            try:
                await self.snapshot_locked()
                return False        # 刚推上去的就是本地状态，不用再拿远端覆盖自己
            except Exception as e:
                log.warning("推本地快照失败，继续按远端加载: %s", e)
                if not pid:
                    return False
        if pid:
            best = await self.client.get_messages(self.chat, ids=pid)
            if not best or not is_db_msg(best):
                best = await self._find_snapshot()
        else:
            best = await self._find_snapshot()
        if not best:
            log.info("TG 上没有索引快照，使用本地库")
            await self._clear_own_stale_lock()
            return False
        fd, tmp = tempfile.mkstemp(prefix="tgnd_db_dl_", suffix=".xz")
        os.close(fd)
        try:
            try:
                await self.client.download_media(best, file=tmp)
                self.backend.restore(tmp)
            except Exception as e:
                # 可能是同名的「手动上传」文件，或者快照本身损坏；restore 里已校验过，
                # 失败时本地库保持不变，这里只告警不崩。
                log.warning("快照载入失败（跳过，继续用本地库）: %s", e)
                return False
            self.snap_msg_id = best.id
            self.snap_doc_id = getattr(getattr(best, "document", None), "id", 0)
            log.info("已从 TG 载入索引快照 (msg %d)", best.id)
            await self._clear_own_stale_lock()
            return True
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass
