"""把索引、缓存、TG 三个部件拼成「文件系统」的那一层。

上层（WebDAV）只看见路径和字节，不关心分片消息、缓存或后台队列。
"""
import asyncio
import logging
import os
import time

import cache
from index import STATUS_DONE, STATUS_PENDING
from paths import norm, parent
import tg

log = logging.getLogger("vfs")


class VFS:
    def __init__(self, index, store):
        self.index = index
        self.tg = store
        self.dirty = False
        self._graveyard = []          # 等着去 TG 删的分片消息 id
        self._dl_locks = {}           # 同一份内容同时只让一个协程回 TG 下载
        self._task = None
        self._wake = asyncio.Event()

    # ---------- 读 ----------
    def get(self, path):
        return self.index.get(norm(path))

    def children(self, path):
        return self.index.children(norm(path))

    def exists(self, path):
        return self.index.get(norm(path)) is not None

    # ---------- 目录 ----------
    async def mkdir(self, path):
        p = norm(path)
        if p == "/":
            return self.index.get("/")
        cur = self.index.get(p)
        if cur:
            if cur.is_dir:
                return cur
            raise FileExistsError(p)
        self._ensure_parents(p)
        self.index.put_dir(p, int(time.time()))
        self.mark_dirty()
        return self.index.get(p)

    def _ensure_parents(self, path):
        chain = []
        cur = parent(path)
        while cur != "/" and self.index.get(cur) is None:
            chain.append(cur)
            cur = parent(cur)
        for d in reversed(chain):
            self.index.put_dir(d, int(time.time()))

    # ---------- 移动 / 复制 ----------
    async def move(self, src, dst):
        s, d = norm(src), norm(dst)
        if s == d:
            return
        if d == "/" or d.startswith(s.rstrip("/") + "/"):
            raise OSError("不能移动到自己或自己的子树里")
        if self.index.get(s) is None:
            raise FileNotFoundError(s)
        if self.index.get(d) is not None:
            self._drop_one(d)                  # 覆盖目标：旧内容回收
        self._ensure_parents(d)
        for path, is_dir in self._subtree(s):
            newp = d + path[len(s):]
            if is_dir:
                self.index.put_dir(newp, int(time.time()))
            else:
                e = self.index.get(path)
                self.index.put_file(newp, e.hash, e.mtime, e.status)
        for path, _ in reversed(self._subtree(s)):
            self.index.drop(path)
        self.mark_dirty()

    async def copy(self, src, dst):
        s, d = norm(src), norm(dst)
        if s == d:
            return
        if d == "/" or d.startswith(s.rstrip("/") + "/"):
            raise OSError("不能复制到自己或自己的子树里")
        if self.index.get(s) is None:
            raise FileNotFoundError(s)
        if self.index.get(d) is not None:
            self._drop_one(d)
        self._ensure_parents(d)
        for path, is_dir in self._subtree(s):
            newp = d + path[len(s):]
            if is_dir:
                self.index.put_dir(newp, int(time.time()))
            else:
                e = self.index.get(path)
                self.index.put_file(newp, e.hash, e.mtime, e.status)
        self.mark_dirty()

    def _subtree(self, path):
        """[(path, is_dir)]，含自身，按路径排序（父在前）。"""
        out = [(p, is_dir) for p, _h, is_dir in self.index.list_under(norm(path))]
        out.sort()
        return out

    def _drop_one(self, path):
        e = self.index.get(path)
        if e is None:
            return
        rows = self.index.take_under(path)
        self._reclaim(rows)

    # ---------- 删除 ----------
    async def delete(self, path):
        p = norm(path)
        e = self.index.get(p)
        if e is None:
            raise FileNotFoundError(p)
        rows = self.index.take_under(p)
        self._reclaim(rows)
        self.mark_dirty()

    def _reclaim(self, rows):
        """被摘掉的路径：没人引用的内容，把分片消息丢进回收站，本地缓存也删掉。"""
        hashes = [h for _p, h, is_dir in rows if h and not is_dir]
        if not hashes:
            return
        alive = self.index.referenced(hashes)
        for h in set(hashes):
            if alive.get(h):
                continue
            got = self.index.content(h)
            if got and got[1]:
                self._graveyard.extend(got[1].values())
            self.index.content_drop(h)
            cache.drop(h)
            self._kick()

    # ---------- 写 ----------
    def mark_dirty(self):
        self.dirty = True
        self.tg.dirty = True
        try:
            self.index.set_meta("dirty", "1")
        except Exception:
            pass
        self.tg._last_write = time.time()
        self._wake.set()

    def staging_tmp(self, tag: str = "put") -> str:
        return os.path.join(cache.staging_dir(),
                            ".%s-%d-%s" % (tag, os.getpid(), os.urandom(6).hex()))

    def finish_put(self, path: str, tmp: str, h: str, size: int, mtime: int):
        """PUT 收完字节：能秒传就秒传，否则登记成「上传中」并交给后台。"""
        p = norm(path)
        old = self.index.get(p)
        have = self.index.content(h)
        self._ensure_parents(p)
        if have and have[1]:
            self.index.put_file(p, h, mtime, STATUS_DONE)
            try:
                os.unlink(tmp)
            except OSError:
                pass
            if old and old.hash and old.hash != h:
                self._reclaim_hash(old.hash)
        else:
            dst = cache.staging_path(h)
            try:
                os.replace(tmp, dst)
            except OSError as e:
                raise IOError(f"落盘失败: {e}")
            self.index.content_put(h, size, {})      # 分片消息还没有
            self.index.put_file(p, h, mtime, STATUS_PENDING)
            if old and old.hash and old.hash != h:
                self._reclaim_hash(old.hash)
        self.mark_dirty()
        return self.index.get(p)

    def _reclaim_hash(self, h: str):
        if not h or self.index.referenced([h]).get(h):
            return
        got = self.index.content(h)
        if got and got[1]:
            self._graveyard.extend(got[1].values())
        self.index.content_drop(h)
        cache.drop(h)
        self._kick()

    def _kick(self):
        if self._task is None or self._task.done():
            try:
                self._task = asyncio.create_task(self._flusher())
            except RuntimeError:
                pass

    async def _flusher(self):
        """去 TG 删消息，不持锁、分批：慢就慢慢来。"""
        while self._graveyard:
            batch, self._graveyard = self._graveyard[:200], self._graveyard[200:]
            try:
                await self.tg.client.delete_messages(self.tg.chat, batch)
            except Exception as e:
                log.warning("删分片消息失败（%d 条）: %s", len(batch), e)

    # ---------- 后台上传 ----------
    def pending(self):
        return [(p, h, m) for p, h, m, st in self.index.all_files() if st == STATUS_PENDING]

    async def upload_pending(self, path: str):
        """把一条「上传中」的记录真正送上 TG，然后转正。"""
        p = norm(path)
        e = self.index.get(p)
        if e is None or e.is_dir or not e.pending:
            return e
        src = cache.staging_path(e.hash)
        if not os.path.exists(src):
            log.warning("暂存没了，标记失败并删除: %s", p)
            self.index.drop(p)
            self.index.content_drop(e.hash)
            self.mark_dirty()
            return None
        parts = await self.tg.upload(e.hash, src, e.size)
        self.index.content_put(e.hash, e.size, parts)
        self.index.set_status(p, STATUS_DONE)
        cache.commit(e.hash)
        self.mark_dirty()
        return self.index.get(p)

    # ---------- 读字节 ----------
    async def iter_content(self, path: str, offset: int = 0, length: int = None):
        e = self.index.get(norm(path))
        if e is None or e.is_dir:
            raise FileNotFoundError(path)
        if e.size == 0:
            return

        async def from_disk(p: str):
            cache.touch(p)
            left = e.size - offset if length is None else min(length, e.size - offset)
            with open(p, "rb") as f:
                f.seek(offset)
                while left > 0:
                    b = await asyncio.to_thread(f.read, min(1 << 20, left))
                    if not b:
                        return
                    left -= len(b)
                    yield b

        p = cache.find(e.hash)
        if p:
            async for b in from_disk(p):
                yield b
            return
        if offset or length is not None:
            # Range：直接回 TG 取那一段，不落缓存
            async for chunk in self.tg.iter_slices(e.parts, e.size, offset, length):
                yield chunk
            return
        # 整份下载：同一份内容只让一个协程回 TG，其余等它落盘后读缓存
        lock = self._dl_locks.setdefault(e.hash, asyncio.Lock())
        async with lock:
            p = cache.find(e.hash)
            if p:
                async for b in from_disk(p):
                    yield b
                return
            async for chunk in self.tg.read_all(e.hash, e.parts, e.size):
                yield chunk
