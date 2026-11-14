"""Telegram 侧的存储：分片上传、下载、快照、锁、重建索引。

对象存储的约定（和旧版一致，TG 上已有的数据必须还能读）：
    分片消息  文件名 "<hash>.<序号>"，hash 是 SHAKE-128 128bit 的 hex
    快照消息  文件名 "tgnd.db.xz"，置顶，caption 里放锁状态
    caption   "lock:<token>" / "lock:null"

这里只管「字节怎么进出 TG」，路径和索引在 index.py，本地缓存在 cache.py。
"""
import asyncio
import hashlib
import logging
import os
import random
import tempfile
import time
import lzma

from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.upload import SaveBigFilePartRequest
from telethon.tl.types import DocumentAttributeFilename, InputFileBig, InputMessagesFilterPinned

from index import STATUS_DONE, STATUS_PENDING

log = logging.getLogger("tg")

SLICE = int(os.environ.get("SLICE_MB", "2000")) * 1024 * 1024
PART = int(os.environ.get("UPLOAD_PART_KB", "512")) * 1024
WORKERS = int(os.environ.get("UPLOAD_WORKERS", "2"))
DB_NAME = "tgnd.db.xz"
LOCK_IDLE = "lock:null"
LOCK_PREFIX = "lock:"


def digest_of(path: str, bufsize: int = 4 << 20) -> str:
    """内容摘要：SHAKE-128 取 128bit（32 hex）。用于识别、校验、秒传。"""
    h = hashlib.shake_128()
    with open(path, "rb") as f:
        while True:
            b = f.read(bufsize)
            if not b:
                break
            h.update(b)
    return h.hexdigest(16)


class Digest:
    """边收字节边算摘要（PUT 时用，省一次全量读）。"""

    def __init__(self):
        self._h = hashlib.shake_128()
        self.size = 0

    def update(self, b: bytes):
        self._h.update(b)
        self.size += len(b)
        return self.size

    def hex(self) -> str:
        return self._h.hexdigest(16)


def is_db_msg(msg) -> bool:
    doc = getattr(msg, "document", None)
    if doc is None:
        return False
    for a in getattr(doc, "attributes", None) or []:
        if getattr(a, "file_name", None) == DB_NAME:
            return True
    return False


def slice_name(h: str, idx: int) -> str:
    return "%s.%d" % (h, idx)


def parse_slice_name(name: str):
    """从 "hash.序号" 里拆出来；不是这个形状返回 (None, -1)。"""
    if not name or "." not in name:
        return None, -1
    h, _, n = name.rpartition(".")
    if len(h) != 32 or not n.isdigit():
        return None, -1
    return h, int(n)


async def upload_parallel(client, src, workers=None, part=PART):
    """并发上传各 part，返回 InputFileBig。src 是路径或可 seek 的文件对象。"""
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

    async def one(i):
        async with sem:
            f.seek(i * part)
            data = await asyncio.to_thread(f.read, part)
            await client(SaveBigFilePartRequest(fid, i, n, data))

    try:
        await asyncio.gather(*[one(i) for i in range(n)])
    finally:
        if own:
            f.close()
    return InputFileBig(fid, n, "blob")


class SliceReader:
    """把一个区间包成有 read() 的对象，交给 send_file 当文件用。"""

    def __init__(self, f, off: int, length: int):
        self.f = f
        self.off = off
        self.length = length
        self.pos = 0

    def read(self, n=-1):
        left = self.length - self.pos
        if left <= 0:
            return b""
        if n is None or n < 0 or n > left:
            n = left
        self.f.seek(self.off + self.pos)
        b = self.f.read(n)
        self.pos += len(b)
        return b

    def seek(self, pos, whence=0):
        if whence == 0:
            self.pos = pos
        elif whence == 1:
            self.pos += pos
        else:
            self.pos = self.length + pos
        return self.pos

    def tell(self):
        return self.pos

    def close(self):
        pass


class TGStore:
    def __init__(self, client, chat, index, cache):
        self.client = client
        self.chat = chat
        self.index = index
        self.cache = cache
        self.token = self._make_token()
        self.holding = False
        self.snap_msg_id = int(self.index.meta("snap_msg_id", 0) or 0)
        self.snap_doc_id = int(self.index.meta("snap_doc_id", 0) or 0)
        self.last_id = int(self.index.meta("last_id", 0) or 0)
        self.dirty = False
        self._last_write = 0.0

    # ---------- 锁 ----------
    @staticmethod
    def _make_token() -> str:
        """锁标识：由 session 派生，同一个 session 重启后 token 不变。

        这样崩溃留下的锁能认出是自己的，直接接管，不需要 TTL。
        """
        sess = os.environ.get("TG_SESSION", "")
        return hashlib.sha256(("tgnd|" + sess).encode()).hexdigest()[:32]

    def _lock_text(self) -> str:
        return (LOCK_PREFIX + self.token) if self.holding else LOCK_IDLE

    @staticmethod
    def _cap(msg) -> str:
        return (getattr(msg, "message", "") or "").strip() if msg else ""

    async def snap_msg(self):
        """置顶的快照消息。先按记下的 id 直读，读不到再走置顶列表。"""
        if self.snap_msg_id:
            try:
                m = await self.client.get_messages(self.chat, ids=self.snap_msg_id)
                if m and is_db_msg(m):
                    return m
            except Exception as e:
                log.warning("按 id 读快照失败: %s", e)
        return await self._find_snapshot()

    async def _pinned(self, limit: int = 50):
        try:
            msgs = await self.client.get_messages(
                self.chat, filter=InputMessagesFilterPinned(), limit=limit)
            return [m for m in msgs if m]
        except Exception as e:
            log.warning("读置顶消息失败: %s", e)
            return []

    async def _find_snapshot(self):
        for m in await self._pinned():
            if is_db_msg(m):
                return m
        try:
            ent = await self.client.get_entity(self.chat)
            full = await self.client(GetFullChannelRequest(ent))
            pid = getattr(full.full_chat, "pinned_msg_id", None)
            if pid:
                m = await self.client.get_messages(self.chat, ids=pid)
                if m and is_db_msg(m):
                    return m
        except Exception as e:
            log.warning("读置顶失败: %s", e)
        async for m in self.client.iter_messages(self.chat, limit=200):
            if is_db_msg(m):
                return m
        return None

    async def acquire(self, timeout: float = 120):
        """抢 TG 锁：把 caption 改成自己的 token 并回读确认。"""
        mine = LOCK_PREFIX + self.token
        end = time.time() + timeout
        while True:
            m = await self.snap_msg()
            if m is None:
                # 空频道：先建一条空快照，锁才有地方挂
                log.info("频道里没有快照，先建一条")
                await self.push_snapshot(caption=LOCK_IDLE)
                await asyncio.sleep(0.5)
                m = await self.snap_msg()
                if m is None and time.time() > end:
                    raise RuntimeError("建不出快照消息")
                continue
            cur = self._cap(m)
            if cur == mine:
                self.snap_msg_id = m.id
                self.holding = True
                return True
            if cur in ("", LOCK_IDLE):
                await self.client.edit_message(self.chat, m.id, text=mine)
                m2 = await self.client.get_messages(self.chat, ids=m.id)
                if self._cap(m2) == mine:
                    self.snap_msg_id = m.id
                    self.holding = True
                    log.info("拿到 TG 锁 (msg %d)", m.id)
                    return True
            if time.time() > end:
                raise TimeoutError("TG 锁被占用，等待超时")
            await asyncio.sleep(2)

    async def release(self):
        if not self.holding:
            return
        try:
            if self.snap_msg_id:
                await self.client.edit_message(self.chat, self.snap_msg_id, text=LOCK_IDLE)
        except Exception as e:
            log.warning("释放锁失败: %s", e)
        self.holding = False

    async def check_lock(self) -> str:
        """看一眼锁现在是谁的。返回 own / idle / other / gone。

        网络错误抛异常，调用方自行决定（不能因为一次 RPC 失败就退出）。
        """
        m = await self.snap_msg()
        if m is None:
            return "gone"
        cap = self._cap(m)
        if cap == LOCK_PREFIX + self.token:
            return "own"
        if cap in ("", LOCK_IDLE):
            return "idle"
        return "other"

    # ---------- 快照 ----------
    async def _pinned_id(self):
        try:
            ent = await self.client.get_entity(self.chat)
            full = await self.client(GetFullChannelRequest(ent))
            return getattr(full.full_chat, "pinned_msg_id", None)
        except Exception:
            return None

    async def push_snapshot(self, caption: str | None = None, final: bool = False):
        """把索引压成 tgnd.db.xz 推到置顶消息上（原地 edit，TG 上只有一份）。

        final=True 时 caption 写成 lock:null 并清掉持有标记——一次 edit 同时
        完成「落库 + 放锁」。
        """
        cap = (LOCK_IDLE if final else self._lock_text()) if caption is None else caption
        fd, tmp = tempfile.mkstemp(prefix="tgnd_db_", suffix=".xz")
        os.close(fd)
        try:
            raw = tempfile.mktemp(suffix=".db")
            try:
                self.index.dump(raw)
                with open(raw, "rb") as f, lzma.open(tmp, "wb") as out:
                    out.write(f.read())
            finally:
                try:
                    os.unlink(raw)
                except OSError:
                    pass
            size = os.path.getsize(tmp)
            if self.snap_msg_id:
                try:
                    m = await self.client.edit_message(
                        self.chat, self.snap_msg_id, file=tmp, force_document=True,
                        attributes=[DocumentAttributeFilename(DB_NAME)], text=cap)
                    self.snap_doc_id = int(getattr(getattr(m, "document", None), "id", 0) or 0)
                    self.index.set_meta("snap_msg_id", self.snap_msg_id)
                    self.index.set_meta("snap_doc_id", self.snap_doc_id)
                    if final:
                        self.holding = False
                    self.dirty = False
                    self.index.set_meta("dirty", "0")
                    log.info("快照已更新 (msg %d, %.1f MB)", self.snap_msg_id, size / 1048576)
                    return
                except Exception as e:
                    log.warning("原地替换快照失败，改发新的: %s", e)
            msg = await self.client.send_file(
                self.chat, tmp, force_document=True, caption=cap,
                attributes=[DocumentAttributeFilename(DB_NAME)])
            old = self.snap_msg_id
            self.snap_msg_id = msg.id
            self.snap_doc_id = int(getattr(getattr(msg, "document", None), "id", 0) or 0)
            self.index.set_meta("snap_msg_id", msg.id)
            self.index.set_meta("snap_doc_id", self.snap_doc_id)
            try:
                await self.client.pin_message(self.chat, msg, notify=False)
            except Exception as e:
                log.warning("置顶失败: %s", e)
            if old and old != msg.id:
                try:
                    await self.client.delete_messages(self.chat, [old])
                except Exception:
                    pass
            if final:
                self.holding = False
            self.dirty = False
            self.index.set_meta("dirty", "0")
            log.info("快照已上传 (msg %d, %.1f MB)", msg.id, size / 1048576)
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass

    async def pull_snapshot(self, merge: bool = False):
        """从 TG 拉快照覆盖本地索引。

        merge=True 时把本地独有的路径追加进去（锁不在自己手里时用：对方是
        当前写者，但它在看到我之前可能漏掉了我的新路径）。
        """
        m = await self.snap_msg()
        if not m:
            log.info("TG 上没有快照，保留本地索引")
            return False
        fd, tmp = tempfile.mkstemp(prefix="tgnd_db_dl_", suffix=".xz")
        os.close(fd)
        raw = tempfile.mktemp(suffix=".db")
        try:
            await self.client.download_media(m, file=tmp)
            with lzma.open(tmp) as src, open(raw, "wb") as out:
                out.write(src.read())
            mine_files, mine_dirs = [], []
            if merge:
                mine_files = self.index.all_files()
                mine_dirs = self.index.all_dirs()
            self.index.restore(raw)
            kept = 0
            if merge:
                kept = self._merge_back(mine_files, mine_dirs)
            self.snap_msg_id = m.id
            self.snap_doc_id = int(getattr(getattr(m, "document", None), "id", 0) or 0)
            self.index.set_meta("snap_msg_id", m.id)
            self.index.set_meta("snap_doc_id", self.snap_doc_id)
            if kept:
                log.warning("远端快照缺了本地 %d 条路径，已补回并标记待推", kept)
                self.dirty = True
                self.index.set_meta("dirty", "1")
            log.info("已从 TG 载入索引快照 (msg %d)", m.id)
            return True
        except Exception as e:
            log.warning("载入快照失败，继续用本地索引: %s", e)
            return False
        finally:
            for p in (tmp, raw):
                try:
                    os.unlink(p)
                except OSError:
                    pass

    def _merge_back(self, files, dirs) -> int:
        """把远端快照里没有的本地路径补回去（同路径以远端为准）。

        返回补回去的条数：>0 说明本地还有远端不知道的东西，调用方要标脏再推一次。
        """
        n = 0
        for path, mtime in dirs:
            if self.index.get(path) is None:
                self.index.put_dir(path, mtime)
                n += 1
        for path, h, mtime, status in files:
            if self.index.get(path) is None:
                self.index.put_file(path, h, mtime, status)
                n += 1
        return n

    # ---------- 分片上传 ----------
    async def _send_slices(self, local_path: str, h: str, size: int):
        """按 SLICE 切片发上 TG，返回 {序号: 消息id}。"""
        parts = {}
        off = 0
        idx = 0
        with open(local_path, "rb") as f:
            while off < size:
                length = min(SLICE, size - off)
                name = slice_name(h, idx)
                src = (local_path if length == size and idx == 0
                       else SliceReader(f, off, length))
                msg = await self._send_one(src, name)
                parts[idx] = msg.id
                self.last_id = max(self.last_id, msg.id)
                off += length
                idx += 1
        return parts

    async def _send_one(self, src, name: str):
        attrs = [DocumentAttributeFilename(name)]
        try:
            ref = await upload_parallel(self.client, src)
        except Exception as e:
            log.warning("并行上传失败，退回顺序: %s", e)
            return await self.client.send_file(self.chat, src, force_document=True,
                                               attributes=attrs)
        # 字节已经在 DC 上，失败的是建消息：拿 ref 重试，别把整片重传
        for attempt in range(3):
            try:
                return await self.client.send_file(self.chat, ref, force_document=True,
                                                   attributes=attrs)
            except Exception as e:
                if attempt == 2:
                    raise
                log.warning("建消息失败（第 %d 次）: %s", attempt + 1, e)
                await asyncio.sleep(2 ** attempt)

    async def upload(self, h: str, local_path: str, size: int) -> dict:
        """确保这份内容的字节在 TG 上，返回 parts。已有就秒传。"""
        got = self.index.content(h)
        if got and got[1]:
            return got[1]
        parts = await self._send_slices(local_path, h, size)
        self.index.content_put(h, size, parts)
        self.last_id = max(self.last_id, max(parts.values()) if parts else 0)
        self.index.set_meta("last_id", self.last_id)
        return parts

    # ---------- 下载 ----------
    async def iter_slices(self, parts: dict, size: int, offset: int = 0, length: int = None):
        """按字节区间流式读分片。parts 的序号必须是连续的。"""
        ids = [parts[i] for i in sorted(parts)]
        start = offset
        end = size if length is None else min(size, offset + length)
        sent = 0
        pos = 0
        for mid in ids:
            piece = min(SLICE, size - pos)
            if start >= pos + piece:
                pos += piece
                continue
            while sent < end - offset:
                want = end - offset - sent
                chunk_size = min(1 << 20, want)
                data = await self._read_slice(mid, start - pos, chunk_size)
                if not data:
                    return
                start += len(data)
                sent += len(data)
                yield data
                if start >= pos + piece:
                    break
            pos += piece

    async def _read_slice(self, mid: int, off: int, n: int) -> bytes:
        m = await self.client.get_messages(self.chat, ids=mid)
        if not m:
            return b""
        out = b""
        async for chunk in self.client.iter_download(m, offset=off, limit=n):
            out += chunk
        return out

    async def read_all(self, h: str, parts: dict, size: int):
        """整份内容：本地优先，没有就边下边落缓存。"""
        p = self.cache.find(h)
        if p:
            self.cache.touch(p)
            with open(p, "rb") as f:
                while True:
                    b = await asyncio.to_thread(f.read, 1 << 20)
                    if not b:
                        return
                    yield b
        tmp = os.path.join(self.cache.staging_dir(), ".dl-" + h)
        f = open(tmp, "wb")
        try:
            async for chunk in self.iter_slices(parts, size):
                await asyncio.to_thread(f.write, chunk)
                yield chunk
            f.close()
            self.cache.put(h, tmp)
        finally:
            try:
                if not f.closed:
                    f.close()
            except Exception:
                pass
            try:
                if os.path.exists(tmp):
                    os.unlink(tmp)
            except OSError:
                pass

    # ---------- 重建索引 ----------
    async def reindex(self):
        """扫全部消息重建索引：认分片、认快照，无主消息进 /lost+found。"""
        by_hash = {}
        snap_max = 0
        n = 0
        async for m in self.client.iter_messages(self.chat):
            n += 1
            if is_db_msg(m):
                snap_max = max(snap_max, m.id)
                continue
            doc = getattr(m, "document", None)
            if doc is None:
                continue
            for a in getattr(doc, "attributes", None) or []:
                h, idx = parse_slice_name(getattr(a, "file_name", "") or "")
                if h and idx >= 0:
                    by_hash.setdefault(h, {}).setdefault(idx, m.id)
                break
        if snap_max:
            self.snap_msg_id = snap_max
            self.index.set_meta("snap_msg_id", snap_max)
        now = int(time.time())
        for h, parts in by_hash.items():
            idxs = sorted(parts)
            whole = idxs == list(range(len(idxs)))
            size = 0
            if whole:
                # 片数 × SLICE，最后一片按实际大小修正
                last = await self.client.get_messages(self.chat, ids=parts[idxs[-1]])
                size = (len(idxs) - 1) * SLICE + int(getattr(getattr(last, "document", None), "size", 0) or 0)
            self.index.content_put(h, size, parts)
        # 已有路径如果内容变完整了，把上传中标记改成完成
        for path, h, mtime, status in self.index.all_files():
            if status == STATUS_PENDING and h and self.index.content(h):
                self.index.set_status(path, STATUS_DONE)

        # 没有任何路径引用的内容挂到 /lost+found/<hash>：TG 消息里只有 hash，
        # 原始路径恢复不了，但至少内容还能列出来、还能下载。
        refs = self.index.referenced(list(by_hash))
        lost = 0
        if by_hash:
            self.index.put_dir("/lost+found", now)
        for h, parts in by_hash.items():
            if refs.get(h):
                continue
            got = self.index.content(h)
            if not got or not got[1]:
                continue
            path = f"/lost+found/{h}"
            if self.index.get(path) is None:
                self.index.put_file(path, h, now, STATUS_DONE)
                lost += 1
        if lost:
            log.warning("有 %d 份内容没有路径引用，已挂到 /lost+found 下", lost)
        log.info("重建索引：扫 %d 条消息，%d 份内容", n, len(by_hash))
        return n
