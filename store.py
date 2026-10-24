"""Telegram 存储后端：TG 消息按 md5 命名，路径索引走可替换的 Backend，快照压缩后传回 TG 置顶。

约定
  TG 文件消息文件名 = "<md5>.<分片序号>"    例  d41d8c...427e.0 / .1
  TG 文件消息 caption = {"i": 序号}
  TG 快照消息      文件名 = "tgnd.db.xz"，caption = {"db":1}，置顶
  Backend 负责 path ↔ md5 与分片 msg_id；默认 SQLite，换成 PostgreSQL 只需再实现一个 Backend
"""
import abc
import asyncio
from contextlib import asynccontextmanager
import hashlib
import io
import json
import logging
import lzma
import os
import random
import shutil
import sqlite3
import tempfile
import time

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.upload import SaveBigFilePartRequest
from telethon.tl.types import DocumentAttributeFilename, InputFileBig

log = logging.getLogger("store")

SLICE = int(os.environ.get("SLICE_MB", "2047")) * 1024 * 1024
PART = 512 * 1024
WORKERS = int(os.environ.get("UPLOAD_WORKERS", "2"))
DB_PATH = os.environ.get("TGND_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), "tgnd.db"))
BACKEND = os.environ.get("TGND_BACKEND", "sqlite")
DB_NAME = "tgnd.db.xz"
LOCK_NAME = "tgnd.lock"
LOCK_TTL = 120                     # 锁超过这么久没释放就视为残留


# ---------- 基础工具 ----------
def norm(p: str) -> str:
    return "/" + "/".join(x for x in (p or "").split("/") if x)


def parent_of(p: str):
    p = norm(p)
    if p == "/":
        return None
    i = p.rfind("/")
    return "/" if i == 0 else p[:i]


def base_of(p: str) -> str:
    return norm(p).rsplit("/", 1)[-1]


def digest_of(path: str, bufsize: int = 4 << 20) -> str:
    """内容摘要：sha256（16 进制，64 hex）。用于识别与校验，也用于秒传。"""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(bufsize)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


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


def parse_name(msg):
    """文件名 "<md5>.<序号>" → (md5, 序号)。"""
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
            log.info("  %d/%d part (%.1f%%, %.0f MB)", done, n, 100.0 * done / n, size / 1048576)

    try:
        await asyncio.gather(*[one(i) for i in range(n)])
    finally:
        if own:
            f.close()
    return InputFileBig(fid, n, "blob")


class Entry:
    __slots__ = ("path", "is_dir", "mtime", "md5", "size", "parts", "status", "expect", "skipped")

    def __init__(self, path, is_dir, mtime=0, md5="", size=0, parts=None,
                 status=0, expect=0):
        self.path = path
        self.is_dir = is_dir
        self.mtime = mtime
        self.md5 = md5
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
        return f"<{'D' if self.is_dir else 'F'} {self.path} {self.size} md5={self.md5[:8]} x{len(self.parts)}>"


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
    def set_file(self, path: str, md5: str, size: int, mtime: int, parts: dict,
                 status: int = 0, expect: int = 0): ...

    @abc.abstractmethod
    def pending(self) -> list: ...

    @abc.abstractmethod
    def set_dir(self, path: str, mtime: int): ...

    @abc.abstractmethod
    def remove(self, path: str): ...

    @abc.abstractmethod
    def find_by_md5(self, md5: str) -> list: ...

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


class SqliteBackend(Backend):
    def __init__(self, path: str = DB_PATH):
        self.path = path
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        self._open()

    def _open(self):
        self.db = sqlite3.connect(self.path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS files(
                path TEXT PRIMARY KEY, md5 TEXT NOT NULL, size INTEGER DEFAULT 0,
                mtime INTEGER DEFAULT 0, parts TEXT DEFAULT '{}');
            CREATE TABLE IF NOT EXISTS dirs(path TEXT PRIMARY KEY, mtime INTEGER DEFAULT 0);
            CREATE INDEX IF NOT EXISTS idx_files_md5 ON files(md5);
            CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
        """)
        cols = {r[1] for r in self.db.execute("PRAGMA table_info(files)")}
        if "status" not in cols:
            self.db.execute("ALTER TABLE files ADD COLUMN status INTEGER DEFAULT 0")
        if "expect" not in cols:
            self.db.execute("ALTER TABLE files ADD COLUMN expect INTEGER DEFAULT 0")
        self.db.commit()

    def get(self, path):
        p = norm(path)
        if p == "/":
            return Entry("/", True, int(time.time()))
        r = self.db.execute(
            "select path,md5,size,mtime,parts,status,expect from files where path=?", (p,)).fetchone()
        if r:
            return Entry(r[0], False, r[3], r[1], r[2], json.loads(r[4] or "{}"), r[5], r[6])
        r = self.db.execute("select path,mtime from dirs where path=?", (p,)).fetchone()
        if r:
            return Entry(r[0], True, r[1])
        return None

    def children(self, path):
        p = norm(path)
        prefix = p.rstrip("/") + "/"
        names = [x[0] for x in self.db.execute("select path from dirs")] + \
                [x[0] for x in self.db.execute("select path from files")]
        out = []
        for k in names:
            if k == p or not k.startswith(prefix):
                continue
            if "/" not in k[len(prefix):]:
                e = self.get(k)
                if e:
                    out.append(e)
        out.sort(key=lambda x: (not x.is_dir, x.name.lower()))
        return out

    def all_entries(self):
        out = {"/": Entry("/", True, int(time.time()))}
        for p, m in self.db.execute("select path,mtime from dirs"):
            out[p] = Entry(p, True, m)
        for p, md5, size, mtime, parts, st, ex in self.db.execute(
                "select path,md5,size,mtime,parts,status,expect from files"):
            out[p] = Entry(p, False, mtime, md5, size, json.loads(parts or "{}"), st, ex)
        return out

    def set_file(self, path, md5, size, mtime, parts, status=0, expect=0):
        self.db.execute(
            "insert or replace into files(path,md5,size,mtime,parts,status,expect)"
            " values(?,?,?,?,?,?,?)",
            (norm(path), md5, size, mtime, json.dumps(parts), status, expect))
        self.db.commit()

    def pending(self):
        return [tuple(r) for r in self.db.execute(
            "select path,md5,expect,size,mtime from files where status=1")]

    def get_meta(self, k, default=""):
        r = self.db.execute("select v from meta where k=?", (k,)).fetchone()
        return r[0] if r else default

    def set_meta(self, k, v):
        self.db.execute("insert into meta(k,v) values(?,?) "
                        "on conflict(k) do update set v=?", (k, str(v), str(v)))
        self.db.commit()

    def set_dir(self, path, mtime):
        self.db.execute("insert or replace into dirs(path,mtime) values(?,?)", (norm(path), mtime))
        self.db.commit()

    def remove(self, path):
        p = norm(path)
        self.db.execute("delete from files where path=?", (p,))
        self.db.execute("delete from dirs where path=?", (p,))
        self.db.commit()

    def find_by_md5(self, md5):
        return [(r[0], r[1], json.loads(r[2] or "{}")) for r in self.db.execute(
            "select path,size,parts from files where md5=?", (md5,))]

    def paths_under(self, prefix):
        prefix = norm(prefix)
        pre = prefix.rstrip("/") + "/"
        out = [x[0] for x in self.db.execute("select path from dirs where path=? or path like ?", (prefix, pre + "%"))]
        out += [x[0] for x in self.db.execute("select path from files where path=? or path like ?", (prefix, pre + "%"))]
        return out

    def dump(self, dst):
        """导出快照。WAL 模式下主库文件可能不含最新数据，必须用 backup API 生成一致性副本。"""
        self.db.commit()
        tmpdb = dst + ".db"
        try:
            out = sqlite3.connect(tmpdb)
            self.db.backup(out)
            out.close()
            with open(tmpdb, "rb") as src, lzma.open(dst, "wb", preset=9) as fp:
                shutil.copyfileobj(src, fp)
        finally:
            try:
                os.unlink(tmpdb)
            except OSError:
                pass

    def restore(self, src):
        self.db.close()
        for suf in ("", "-wal", "-shm"):
            try:
                os.unlink(self.path + suf)
            except OSError:
                pass
        with lzma.open(src, "rb") as f, open(self.path, "wb") as out:
            shutil.copyfileobj(f, out)
        self._open()

    def close(self):
        try:
            self.db.close()
        except Exception:
            pass


BACKENDS = {"sqlite": SqliteBackend}


def make_backend(name: str | None = None, **kw) -> Backend:
    name = (name or BACKEND).lower()
    if name not in BACKENDS:
        raise SystemExit(f"未知索引后端: {name}（可选 {list(BACKENDS)}）")
    return BACKENDS[name](**kw)


# ---------- 业务层 ----------
class _WriteGuard:
    """把 `async with self.lock:` 升级成「抢分布式锁 + 互斥 + 收尾上传快照」。"""

    def __init__(self, store):
        self.s = store

    async def __aenter__(self):
        if not self.s.fast:
            await self.s.acquire()
        await self.s.mutex.acquire()
        return self

    async def __aexit__(self, *exc):
        try:
            if not self.s.fast and self.s.dirty:
                try:
                    await self.s.snapshot()
                except Exception as e:
                    log.warning("快照上传失败: %s", e)
        finally:
            self.s.mutex.release()
            if not self.s.fast:
                try:
                    await self.s.release()
                except Exception as e:
                    log.warning("释放锁失败: %s", e)


class Store:
    def __init__(self, client: TelegramClient, chat, backend: Backend | None = None):
        self.client = client
        self.chat = chat
        self.backend = backend or make_backend()
        self.mutex = asyncio.Lock()
        self.lock = _WriteGuard(self)
        self.dirty = False
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

    @property
    def entries(self):
        return self.backend.all_entries()

    def get(self, path):
        return self.backend.get(path)

    def children(self, path):
        return self.backend.children(path)

    # ---------- 写入 ----------
    async def _ensure_parents(self, p: str):
        par = parent_of(p)
        chain = []
        while par and par != "/":
            chain.append(par)
            par = parent_of(par)
        for d in reversed(chain):
            if not self.backend.get(d):
                self.backend.set_dir(d, int(time.time()))

    async def mkdir(self, path: str, _force=False):
        p = norm(path)
        await self.ensure_fresh()
        async with self.lock:
            cur = self.backend.get(p)
            if cur and not _force:
                return cur
            await self._ensure_parents(p)
            self.backend.set_dir(p, int(time.time()))
            self.dirty = True
            return self.backend.get(p)

    async def put_file(self, path: str, local_path: str, mtime: int | None = None,
                       slice_size: int | None = None, append: bool = False,
                       start_index: int | None = None, skip_same: bool = False):
        p = norm(path)
        size = os.path.getsize(local_path)
        S = slice_size or SLICE
        digest = None if append else digest_of(local_path)
        mt = int(mtime) if mtime else int(os.path.getmtime(local_path))
        await self.ensure_fresh()

        # 阶段一：只在索引里登记「上传中」，锁在这几百毫秒内就释放
        async with self.lock:
            old = self.backend.get(p)
            if old and not old.is_dir and not append:
                if skip_same and old.status == 0 and old.md5 == digest and old.size == size:
                    old.skipped = True
                    log.info("跳过（已存在且内容相同）: %s", p)
                    return old
                await self._drop(p)
                old = None
            await self._ensure_parents(p)

            if size == 0 and not append:
                self.backend.set_file(p, digest or "", 0, mt, {}, 0, 0)
                self.dirty = True
                log.info("空文件只登记索引: %s", p)
                return self.backend.get(p)

            if digest and not append:
                for opath, osize, oparts in self.backend.find_by_md5(digest):
                    if oparts:
                        self.backend.set_file(p, digest, osize, mt, oparts, 0, 0)
                        self.dirty = True
                        log.info("秒传命中 %s (md5 %s) ← %s", p, digest[:8], opath)
                        return self.backend.get(p)

            parts = {}
            if append and old and not old.is_dir:
                parts = dict(old.parts)
                if not digest:
                    digest = old.md5
            base_idx = start_index if start_index is not None else (
                (max(parts) + 1) if parts else 0)
            tag = digest or hashlib.md5(f"{p}{time.time()}".encode()).hexdigest()
            nparts = max(1, (size + S - 1) // S) if size else 0
            self.backend.set_file(p, digest or tag, size, mt, parts, 1, nparts)
            self.dirty = True

        # 阶段二：真正把字节传上 TG，这段时间不持锁
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

        if size <= S and not append:
            await send(local_path, base_idx)
        else:
            n = 0
            pos = 0
            with open(local_path, "rb") as f:
                while pos < size:
                    length = min(S, size - pos)
                    await send(SliceReader(f, pos, length), base_idx + n)
                    pos += length
                    n += 1
                    log.info("分片 %d 完成 (%d 字节)", base_idx + n - 1, length)

        # 阶段三：提交结果，锁只在这几百毫秒内再持一次
        async with self.lock:
            self.backend.set_file(p, digest or tag, size, mt, parts, 0, nparts)
            self.dirty = True
        return self.backend.get(p)

    async def register(self, path: str, local_path: str):
        """阶段一：只登记「待上传」，不传字节（持锁几百毫秒内完成）。"""
        p = norm(path)
        size = os.path.getsize(local_path)
        digest = digest_of(local_path)
        mt = int(os.path.getmtime(local_path))
        async with self.lock:
            old = self.backend.get(p)
            if old and not old.is_dir and old.status == 0 and old.md5 == digest and old.size == size:
                old.skipped = True
                return old
            await self._ensure_parents(p)
            if digest:
                for opath, osize, oparts in self.backend.find_by_md5(digest):
                    if oparts:
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
        digest = e.md5
        tag = digest or hashlib.md5(f"{p}{time.time()}".encode()).hexdigest()
        S = SLICE
        parts: dict[int, int] = {}

        async def send(src, idx_no):
            attrs = [DocumentAttributeFilename(f"{tag}.{idx_no}")]
            try:
                ref = await upload_parallel(self.client, src)
            except Exception as ex:
                log.warning("并行上传失败，退回顺序: %s", ex)
                msg = await self.client.send_file(self.chat, src, force_document=True, attributes=attrs)
            else:
                msg = await self.client.send_file(self.chat, ref, force_document=True, attributes=attrs)
            parts[idx_no] = msg.id
            self.last_id = max(self.last_id, msg.id)

        if size == 0:
            pass
        elif size <= S:
            await send(local_path, 0)
        else:
            n = 0
            pos = 0
            with open(local_path, "rb") as f:
                while pos < size:
                    length = min(S, size - pos)
                    await send(SliceReader(f, pos, length), n)
                    pos += length
                    n += 1
        async with self.lock:
            self.backend.set_file(p, digest or tag, size, e.mtime, parts, 0, len(parts))
            self.dirty = True
        return self.backend.get(p)

    async def _drop(self, path: str, hard=True):
        """删记录；hard 时若该 md5 没有别的路径引用，连 TG 消息一起删。"""
        p = norm(path)
        e = self.backend.get(p)
        self.backend.remove(p)
        self.dirty = True
        if not (hard and e and not e.is_dir and e.parts):
            return
        left = [x for x in self.backend.find_by_md5(e.md5) if x[2]]
        if left:
            return
        await self._del_ids(list(e.parts.values()))
        log.info("删除 %s 的 %d 个分片消息", p, len(e.parts))

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
        await self.ensure_fresh()
        if not self.backend.get(s):
            raise FileNotFoundError(s)
        async with self.lock:
            rows = [self.backend.get(x) for x in self.backend.paths_under(s)]
            rows = [x for x in rows if x]
            for x in rows:
                newp = d + x.path[len(s):]
                if x.is_dir:
                    self.backend.set_dir(newp, x.mtime)
                else:
                    self.backend.set_file(newp, x.md5, x.size, x.mtime, x.parts)
            for x in rows:
                self.backend.remove(x.path)
            self.dirty = True

    async def copy(self, src: str, dst: str):
        s, d = norm(src), norm(dst)
        await self.ensure_fresh()
        if not self.backend.get(s):
            raise FileNotFoundError(s)
        async with self.lock:
            rows = [self.backend.get(x) for x in self.backend.paths_under(s)]
            for x in rows:
                if not x:
                    continue
                newp = d + x.path[len(s):]
                if x.is_dir:
                    self.backend.set_dir(newp, x.mtime)
                else:
                    self.backend.set_file(newp, x.md5, x.size, x.mtime, x.parts)
            self.dirty = True

    # ---------- 读取内容 ----------
    async def iter_content(self, path: str):
        e = self.backend.get(path)
        if not e or e.is_dir or e.status == 1:
            raise FileNotFoundError(path)
        for k in sorted(e.parts):
            m = await self.client.get_messages(self.chat, ids=e.parts[k])
            if not m or not getattr(m, "document", None):
                continue
            async for piece in self.client.iter_download(m):
                yield piece

    async def download_to_temp(self, path: str) -> str:
        fd, tmp = tempfile.mkstemp(prefix="tgnd_dl_")
        with os.fdopen(fd, "wb") as f:
            async for piece in self.iter_content(path):
                f.write(piece)
        return tmp

    async def _del_ids(self, ids):
        ids = [i for i in ids if i]
        for k in range(0, len(ids), 100):
            try:
                await self.client.delete_messages(self.chat, ids[k:k + 100])
            except Exception as e:
                log.warning("删除消息失败: %s", e)

    # ---------- 清理 ----------
    async def gc(self, dry_run: bool = False):
        """清理：TG 上没人引用的文件消息（孤儿）+ 索引里指向不存在消息的记录。"""
        async with self.lock:
            live = {}
            async for m in self.client.iter_messages(self.chat):
                nm = parse_name(m)
                if nm:
                    live[m.id] = nm
            used, missing = set(), []
            for e in self.backend.all_entries().values():
                if e.is_dir or not e.parts:
                    continue
                for mid in e.parts.values():
                    used.add(mid)
                if any(mid not in live for mid in e.parts.values()):
                    missing.append(e.path)
            orphans = sorted(mid for mid in live if mid not in used)
            log.info("TG 文件消息 %d，被引用 %d，孤儿 %d，无效记录 %d",
                     len(live), len(used), len(orphans), len(missing))
            if dry_run:
                return orphans, missing
            if orphans:
                await self._del_ids(orphans)
            for p in missing:
                self.backend.remove(p)
            if missing:
                self.dirty = True
            return orphans, missing

    # ---------- 同步 / 快照 ----------
    async def sync(self, full: bool = True):
        """扫描 TG 消息，补全 md5 → 分片映射，并记录最新快照消息。"""
        parts_by_md5 = {}
        snaps = []
        n = 0
        last = 0 if full else int(self.backend.get_meta("last_id", "0") or 0)
        kw = {"reverse": True}
        if last:
            kw["min_id"] = last
        async for m in self.client.iter_messages(self.chat, limit=None, **kw):
            n += 1
            if is_db_msg(m):
                snaps.append(m)
                continue
            nm = parse_name(m)
            if not nm:
                continue
            md5, idx = nm
            parts_by_md5.setdefault(md5, {})[idx] = m.id
            if m.id > self.last_id:
                self.last_id = m.id
            if m.id > last:
                last = m.id
        for md5, parts in parts_by_md5.items():
            for opath, osize, oparts in self.backend.find_by_md5(md5):
                if oparts != parts:
                    self.backend.set_file(opath, md5, osize, int(time.time()), parts)
        self.backend.set_meta("last_id", last)
        for path, md5, expect, size, mtime in (self.backend.pending() if full else []):
            got = parts_by_md5.get(md5, {})
            if expect and len(got) >= expect:
                self.backend.set_file(path, md5, size, mtime, got, 0, expect)
                log.info("补记完成的上传: %s (%d 片)", path, len(got))
        if snaps:
            self.snap_msg_id = max(s.id for s in snaps)
        log.info("扫描 %d 条消息，%d 个 md5，快照 %d 条", n, len(parts_by_md5), len(snaps))
        return n

    async def snapshot(self):
        """把索引压成快照传到 TG，置顶并删掉旧的。"""
        fd, tmp = tempfile.mkstemp(prefix="tgnd_db_", suffix=".xz")
        os.close(fd)
        try:
            self.backend.dump(tmp)
            size = os.path.getsize(tmp)
            if self.snap_msg_id:
                try:
                    await self.client.edit_message(
                        self.chat, self.snap_msg_id, file=tmp, force_document=True,
                        attributes=[DocumentAttributeFilename(DB_NAME)])
                    self.dirty = False
                    log.info("索引快照已就地替换 (msg %d, %d 字节)", self.snap_msg_id, size)
                    return None
                except Exception as e:
                    log.warning("就地替换失败，改为新发一条: %s", e)
            msg = await self.client.send_file(
                self.chat, tmp, force_document=True,
                attributes=[DocumentAttributeFilename(DB_NAME)])
            old = self.snap_msg_id
            self.snap_msg_id = msg.id
            if old and old != msg.id:
                await self._del_ids([old])
            try:
                await self.client.pin_message(self.chat, msg, notify=False)
            except Exception as e:
                log.warning("置顶失败: %s", e)
            self.dirty = False
            log.info("索引快照已上传 (msg %d, %d 字节)", msg.id, os.path.getsize(tmp))
            return msg
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass

    async def _find_snapshot(self):
        """优先从置顶消息拿快照，省得扫聊天记录。"""
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
        """锁标识：盐由 TG_SESSION 派生，再和 session 一起哈希。"""
        sess = os.environ.get("TG_SESSION", "")
        salt = hashlib.sha256(sess.encode()).hexdigest()[:16]
        return hashlib.sha256((salt + sess).encode()).hexdigest()[:32]

    async def _lock_msg(self):
        async for m in self.client.iter_messages(self.chat, limit=10):
            if is_lock_msg(m):
                return m
        return None

    async def _lock_bytes(self, m) -> bytes:
        fd, tmp = tempfile.mkstemp(prefix="tgnd_lock_")
        os.close(fd)
        try:
            await self.client.download_media(m, file=tmp)
            with open(tmp, "rb") as f:
                return f.read()
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass

    async def _set_lock(self, msg_id: int, payload: bytes):
        await self.client.edit_message(
            self.chat, msg_id, file=io.BytesIO(payload), force_document=True,
            attributes=[DocumentAttributeFilename(LOCK_NAME)])

    async def acquire(self, timeout: int = 60, nonblock: bool = False):
        """锁就是一个 tgnd.lock 文件：大小 1 且内容 0x00 = 空闲，大小 32 = 锁定（内容为持有者 token）。"""
        mine = self.token.encode()
        end = time.time() + timeout
        while True:
            m = await self._lock_msg()
            if m is None:
                msg = await self.client.send_file(
                    self.chat, io.BytesIO(mine), caption="", force_document=True,
                    attributes=[DocumentAttributeFilename(LOCK_NAME)])
                self.lock_msg_id = msg.id
                self._holding = True
                log.info("创建锁 msg %d", msg.id)
                return
            size = m.document.size if m.document else 0
            if size == 1:                                  # 空闲
                await self._set_lock(m.id, mine)
                self.lock_msg_id = m.id
                self._holding = True
                log.info("锁 msg %d 空闲 → 占用", m.id)
                return
            data = await self._lock_bytes(m)
            if data == mine:                               # 已经是自己的锁
                self.lock_msg_id = m.id
                self._holding = True
                return
            if nonblock:
                raise LockBusy("索引写锁被另一个节点持有")
            if time.time() > end:
                log.error("等待索引写锁超过 %d 秒，判定异常退出", timeout)
                raise TimeoutError("索引正被另一个节点写入，等待超时")
            await asyncio.sleep(1.5)

    async def release(self):
        """把锁文件改成 1 字节的 0x00，表示空闲。"""
        if not self.lock_msg_id:
            return
        try:
            await self._set_lock(self.lock_msg_id, b"\x00")
        except Exception as e:
            log.warning("释放锁失败: %s", e)
        self.lock_msg_id = 0
        self._holding = False

    async def _clear_own_stale_lock(self, timeout: int = 120):
        """启动时：自己的残留锁直接置空，别人的锁就轮询等它释放。"""
        end = time.time() + timeout
        waited = False
        while True:
            try:
                m = await self._lock_msg()
                if not m:
                    return
                size = m.document.size if m.document else 0
                if size == 1:
                    return
                data = await self._lock_bytes(m)
                if data == self.token.encode():
                    log.warning("启动时清理自己残留的锁 msg %d", m.id)
                    await self._set_lock(m.id, b"\x00")
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
        self._in_guard = True
        try:
            await self.ensure_fresh()
            if not self._holding:
                await self.acquire(timeout=timeout, nonblock=(timeout <= 0))
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
        """写操作前先看 TG 上的快照有没有被别人更新，有就先拉下来。

        快照用 edit_message 原地替换，消息 id 不变，所以靠 document.id 判断内容是否换过。
        """
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
        """从 TG 拉最新快照覆盖本地索引。"""
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
            await self.client.download_media(best, file=tmp)
            self.backend.restore(tmp)
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
