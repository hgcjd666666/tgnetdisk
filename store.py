"""Telegram 存储后端：路径写进消息 caption，TG 就是唯一真相源。

约定：
  目录: caption = "/photos/"        （以 / 结尾）
  文件: caption = "/photos/a.txt"   （完整路径）
  TG 里显示的文件名 = 上传时间 + ".bin"，切片时追加 .0/.1/...
  超过 SLICE_MB 的文件自动切成多条消息，索引层拼回一个文件。
"""
import asyncio
import io
import logging
import random
import os
import time

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.upload import SaveBigFilePartRequest
from telethon.tl.types import DocumentAttributeFilename, InputFileBig

log = logging.getLogger("store")

SLICE = int(os.environ.get("SLICE_MB", "1900")) * 1024 * 1024
PART = 512 * 1024                    # Telegram 单 part 上限
WORKERS = int(os.environ.get("UPLOAD_WORKERS", "2"))


async def upload_parallel(client, src, workers=None, part=PART):
    """把文件的 part 并发上传（telethon 默认是顺序的），返回 InputFileBig 供 send_file 直接发。"""
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
    step = max(1, n // 200)         # 每 0.5% 报一次进度

    async def one(i):
        nonlocal done
        async with sem:
            f.seek(i * part)
            data = f.read(part)
            await client(SaveBigFilePartRequest(fid, i, n, data))
        done += 1
        if done % step == 0 or done == n:
            log.info("  %d/%d part (%.1f%%, 共 %.0f MB)", done, n, 100.0 * done / n, size / 1048576)

    try:
        await asyncio.gather(*[one(i) for i in range(n)])
    finally:
        if own:
            f.close()
    return InputFileBig(fid, n, "blob")


class SliceReader(io.RawIOBase):
    """把一个文件的 [start, start+length) 当成独立文件交给 telethon，流式读，不落盘不进内存。"""

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


def is_path_session(val: str) -> bool:
    """TG_SESSION 是文件路径，还是 base64 的 StringSession。"""
    return val.startswith("/") or val.startswith("./") or val.endswith(".session") or "/" in val


def make_session(val: str):
    """文件路径原样返回；否则当作 StringSession 字符串解码，无需 sqlite。"""
    return val if is_path_session(val) else StringSession(val)


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


def slice_index_of(msg):
    """从 TG 文件名 "2026-10-23 18:23:16.2" 里取最后一段作为分片序号。"""
    doc = getattr(msg, "document", None)
    if doc is None:
        return None
    for a in doc.attributes:
        fn = getattr(a, "file_name", None)
        if not fn:
            continue
        tail = fn.rsplit(".", 1)[-1]
        if tail.isdigit():
            return int(tail)
        return None
    return None


def parse_caption(txt: str):
    """返回 (path, is_dir)；目录以 / 结尾，文件是完整路径。"""
    txt = (txt or "").strip()
    if not txt.startswith("/"):
        return None
    if txt.endswith("/"):
        return norm(txt), True
    return norm(txt), False


class Entry:
    __slots__ = ("path", "is_dir", "mtime", "parts")

    def __init__(self, path, is_dir, mtime=0, msg_id=0, idx=0, size=0):
        self.path = path
        self.is_dir = is_dir
        self.mtime = mtime
        self.parts: dict[int, tuple[int, int]] = {}
        if msg_id:
            self.parts[idx] = (msg_id, size)

    @property
    def size(self):
        return sum(sz for _, sz in self.parts.values())

    @property
    def slices(self):
        return [self.parts[k][0] for k in sorted(self.parts)]

    @property
    def msg_id(self):
        return self.slices[0] if self.parts else 0

    @property
    def name(self):
        return "/" if self.path == "/" else base_of(self.path)

    def ordered_slices(self):
        return self.slices

    def __repr__(self):
        return f"<{'D' if self.is_dir else 'F'} {self.path} {self.size} x{len(self.parts)}>"


class Store:
    def __init__(self, client: TelegramClient, chat):
        self.client = client
        self.chat = chat
        self.entries: dict[str, Entry] = {}
        self.last_id = 0
        self.lock = asyncio.Lock()
        self.entries["/"] = Entry("/", True, int(time.time()))

    # ---------- 索引 ----------
    def _apply(self, m):
        r = parse_caption(m.message)
        if not r:
            return
        p, is_dir = r
        mt = int(m.date.timestamp()) if m.date else int(time.time())
        if is_dir:
            self.entries[p] = Entry(p, True, mt, m.id)
            return
        doc = getattr(m, "document", None)
        size = int(doc.size or 0) if doc is not None else 0
        idx = slice_index_of(m)
        e = self.entries.get(p)
        if e and not e.is_dir:
            if idx is None:
                idx = len(e.parts)
            old = e.parts.get(idx)
            e.parts[idx] = (m.id, size)      # 同序号后传的覆盖先传的
            return old[0] if old else None
        else:
            self.entries[p] = Entry(p, False, mt, m.id,
                                    idx if idx is not None else 0, size)
            return None

    async def sync(self, full: bool = True):
        kw = dict(reverse=True)
        if not full and self.last_id:
            kw["min_id"] = self.last_id
        n = 0
        orphans = []
        async for m in self.client.iter_messages(self.chat, limit=None, **kw):
            dead = self._apply(m)
            if dead:
                orphans.append(dead)
            if m.id > self.last_id:
                self.last_id = m.id
            n += 1
        if orphans:
            await self._del_ids(orphans)
            log.info("清理被覆盖的旧分片 %d 条", len(orphans))
        log.info("扫描 %d 条消息，索引 %d 个节点", n, len(self.entries))
        return n

    # ---------- 查询 ----------
    def get(self, path) -> Entry | None:
        return self.entries.get(norm(path))

    def children(self, path):
        p = norm(path)
        prefix = p.rstrip("/") + "/"
        out = []
        for k, e in self.entries.items():
            if k == p or not k.startswith(prefix):
                continue
            if "/" not in k[len(prefix):]:
                out.append(e)
        out.sort(key=lambda x: (not x.is_dir, x.name.lower()))
        return out

    # ---------- 写入 ----------
    async def put_file(self, path: str, local_path: str, mtime: int | None = None,
                       slice_size: int | None = None, append: bool = False,
                       start_index: int | None = None):
        """上传文件。slice_size 指定分片大小；append 时作为新分片并入已有文件。"""
        p = norm(path)
        size = os.path.getsize(local_path)
        S = slice_size or SLICE
        async with self.lock:
            old = self.entries.get(p)
            if old and not append:
                await self._del_ids(old.slices)
                old = None
            await self._ensure_parents(p)
            mt = int(mtime) if mtime else int(os.path.getmtime(local_path))
            # 文件名用文件 mtime，且冒号用全角，避免落到 Windows 等半角文件系统时炸掉
            now = time.strftime("%Y-%m-%d %H：%M：%S", time.localtime(mt))
            e = old if (old and not old.is_dir) else Entry(p, False, int(time.time()))
            base_idx = start_index if start_index is not None else (
                (max(e.parts) + 1) if e.parts else 0)

            async def send(src, idx_no):
                attrs = [DocumentAttributeFilename(f"{now}.{idx_no}")]
                try:
                    ref = await upload_parallel(self.client, src)
                except Exception as ex:            # 并发失败就退回 telethon 顺序上传
                    log.warning("并行上传失败，退回顺序: %s", ex)
                    msg = await self.client.send_file(self.chat, src, caption=p,
                                                      force_document=True, attributes=attrs)
                else:
                    msg = await self.client.send_file(self.chat, ref, caption=p,
                                                      force_document=True, attributes=attrs)
                sz = int(msg.document.size or 0) if msg.document else 0
                e.parts[idx_no] = (msg.id, sz)
                if msg.id > self.last_id:
                    self.last_id = msg.id
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
            e.mtime = mt
            self.entries[p] = e
            return e

    async def _ensure_parents(self, p: str):
        par = parent_of(p)
        chain = []
        while par and par != "/" and par not in self.entries:
            chain.append(par)
            par = parent_of(par)
        for d in reversed(chain):
            await self.mkdir(d, _force=True)

    async def mkdir(self, path: str, _force=False):
        p = norm(path)
        if p == "/":
            return self.entries["/"]
        e = self.entries.get(p)
        if e and e.is_dir:
            return e
        if e and not e.is_dir and not _force:
            raise FileExistsError(p)
        if not _force:
            await self._ensure_parents(p)
        msg = await self.client.send_message(self.chat, p.rstrip("/") + "/")
        mt = int(msg.date.timestamp()) if msg.date else int(time.time())
        self.entries[p] = Entry(p, True, mt, msg.id)
        if msg.id > self.last_id:
            self.last_id = msg.id
        return self.entries[p]

    async def _del_ids(self, ids):
        ids = [i for i in ids if i]
        for i in range(0, len(ids), 100):
            try:
                await self.client.delete_messages(self.chat, ids[i:i + 100])
            except Exception as e:
                log.warning("删除消息失败: %s", e)

    async def delete(self, path: str, recursive=True):
        p = norm(path)
        e = self.entries.get(p)
        if not e:
            raise FileNotFoundError(p)
        if e.is_dir:
            if self.children(p) and not recursive:
                raise OSError("目录非空")
            targets = [x for x in self.entries.values()
                       if x.path == p or x.path.startswith(p.rstrip("/") + "/")]
        else:
            targets = [e]
        ids = []
        for t in targets:
            ids.extend(t.slices)
        await self._del_ids(ids)
        for t in targets:
            self.entries.pop(t.path, None)

    async def move(self, src: str, dst: str):
        s, d = norm(src), norm(dst)
        e = self.entries.get(s)
        if not e:
            raise FileNotFoundError(s)
        if d in self.entries:
            raise FileExistsError(d)
        await self._ensure_parents(d)
        if e.is_dir:
            moving = sorted([x for x in self.entries.values()
                             if x.path == s or x.path.startswith(s.rstrip("/") + "/")],
                            key=lambda x: len(x.path))
            for x in moving:
                await self._rewrite_path(x, d + x.path[len(s):])
        else:
            await self._rewrite_path(e, d)

    async def _rewrite_path(self, e: Entry, newp: str):
        cap = (newp.rstrip("/") + "/") if e.is_dir else newp
        for mid in e.slices:
            await self.client.edit_message(self.chat, mid, cap)
        self.entries.pop(e.path, None)
        e.path = newp
        self.entries[newp] = e

    async def copy(self, src: str, dst: str):
        s, d = norm(src), norm(dst)
        e = self.entries.get(s)
        if not e:
            raise FileNotFoundError(s)
        if e.is_dir:
            await self.mkdir(d)
            for c in self.children(s):
                await self.copy(c.path, d.rstrip("/") + "/" + c.name)
        else:
            tmp = await self.download_to_temp(s)
            try:
                await self.put_file(d, tmp, e.mtime)
            finally:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass

    # ---------- 读取 ----------
    async def iter_content(self, path: str):
        e = self.entries.get(norm(path))
        if not e or e.is_dir:
            raise FileNotFoundError(path)
        msgs = await self.client.get_messages(self.chat, ids=e.ordered_slices())
        if not isinstance(msgs, list):
            msgs = [msgs]
        for msg in msgs:
            if msg is None:
                continue
            async for c in self.client.iter_download(msg):
                yield c

    async def download_to_temp(self, path: str) -> str:
        import tempfile
        fd, tmp = tempfile.mkstemp(prefix="tgnd_")
        with open(fd, "wb") as f:
            async for c in self.iter_content(path):
                f.write(c)
        return tmp
