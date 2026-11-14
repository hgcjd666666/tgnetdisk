#!/usr/bin/env python3
"""自测：数据层 + WebDAV 协议，全部跑内存版 Telegram。

    python tests.py            # 全部
    python tests.py -k 缓存     # 按名字过滤
    python tests.py --real     # 连真 Telegram（会产生真实消息）
"""
import asyncio
import datetime
import hashlib
import os
import sys
import tempfile
import time
import traceback
from types import SimpleNamespace

os.environ.setdefault("TGND_CACHE", tempfile.mkdtemp(prefix="tgnd_test_cache_"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import cache  # noqa: E402
import index as IDX  # noqa: E402
import paths  # noqa: E402
import tg  # noqa: E402
from index import Index  # noqa: E402

RESULTS = []
D = "/_test"


class FakeClient:
    """内存版 Telegram：send_file / get_messages / edit / delete / iter_download。"""

    def __init__(self):
        self._id = 0
        self._msgs = {}
        self._blobs = {}
        self._uploads = {}
        self._pinned = []
        self.downloads = 0
        self.rpc = 0

    def _alloc(self):
        self._id += 1
        return self._id

    def _read(self, src, attrs=None):
        name = "blob"
        for a in (attrs or []):
            fn = getattr(a, "file_name", None)
            if fn:
                name = fn
        if isinstance(src, (bytes, bytearray)):
            return bytes(src), name
        if type(src).__name__ in ("InputFileBig", "InputFile"):
            parts = self._uploads.get(src.id, {})
            return b"".join(parts[i] for i in sorted(parts)), name
        if hasattr(src, "read"):
            return src.read(), name
        with open(src, "rb") as f:
            return f.read(), name

    async def send_file(self, chat, file, caption=None, force_document=True,
                        attributes=None, **kw):
        self.rpc += 1
        data, name = self._read(file, attributes)
        mid = self._alloc()
        doc = SimpleNamespace(id=mid, size=len(data),
                              attributes=[SimpleNamespace(file_name=name)])
        m = SimpleNamespace(id=mid, document=doc, message=caption or "",
                            date=datetime.datetime.now(datetime.timezone.utc))
        self._msgs[mid] = m
        self._blobs[mid] = data
        return m

    async def edit_message(self, chat, msg, file=None, caption=None, text=None,
                           attributes=None, **kw):
        self.rpc += 1
        mid = msg if isinstance(msg, int) else msg.id
        m = self._msgs[mid]
        if file is not None:
            data, name = self._read(file, attributes)
            m.document.size = len(data)
            m.document.attributes = [SimpleNamespace(file_name=name)]
            self._blobs[mid] = data
        cap = text if text is not None else caption
        if cap is not None:
            m.message = cap
        return m

    async def get_messages(self, chat, ids=None, limit=None, filter=None, **kw):
        self.rpc += 1
        if ids is not None:
            if isinstance(ids, (list, tuple)):
                return [self._msgs[i] for i in ids if i in self._msgs]
            return self._msgs.get(ids)
        if filter is not None and "Pinned" in type(filter).__name__:
            msgs = [self._msgs[i] for i in self._pinned if i in self._msgs]
            return msgs[:limit] if limit else msgs
        return [self._msgs[i] for i in sorted(self._msgs, reverse=True)[:limit or 1]]

    async def iter_messages(self, chat, limit=None, min_id=0, **kw):
        ids = sorted(self._msgs)
        if min_id:
            ids = [i for i in ids if i > min_id]
        if not kw.get("reverse"):
            ids = ids[::-1]
        if limit:
            ids = ids[:limit]
        for i in ids:
            yield self._msgs[i]

    async def delete_messages(self, chat, ids):
        self.rpc += 1
        for i in ids:
            self._msgs.pop(i, None)
            self._blobs.pop(i, None)
            if i in self._pinned:
                self._pinned.remove(i)

    async def pin_message(self, chat, msg, notify=False, **kw):
        self.rpc += 1
        mid = msg if isinstance(msg, int) else msg.id
        self._pinned = [mid]

    async def download_media(self, m, file=None, **kw):
        self.rpc += 1
        data = self._blobs.get(m if isinstance(m, int) else m.id, b"")
        if hasattr(file, "write"):
            file.write(data)
            return file
        with open(file, "wb") as f:
            f.write(data)
        return file

    async def iter_download(self, m, *, offset=0, limit=None, **kw):
        self.downloads += 1
        data = self._blobs.get(m.id, b"")
        if offset:
            data = data[offset:]
        if limit is not None:
            data = data[:limit]
        yield data

    async def connect(self):
        pass

    async def disconnect(self):
        pass

    async def get_entity(self, x):
        return x

    async def is_user_authorized(self):
        return True

    async def __call__(self, req):
        if type(req).__name__ == "SaveBigFilePartRequest":
            self._uploads.setdefault(req.file_id, {})[req.file_part] = req.bytes
            return True
        return SimpleNamespace(full_chat=SimpleNamespace(
            pinned_msg_id=self._pinned[-1] if self._pinned else None))


def rec(name, ok, detail=""):
    RESULTS.append((name, ok, detail))
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"   <<< {detail}" if not ok else ""),
          flush=True)


def rand(n):
    return os.urandom(n)


class Ctx:
    def __init__(self):
        self.client = FakeClient()
        self.index = Index(tempfile.mktemp(suffix=".db"))
        self.store = tg.TGStore(self.client, 1, self.index, cache)
        import vfs as V
        self.vfs = V.VFS(self.index, self.store)


async def make_ctx():
    c = Ctx()
    return c


# ---------------- 用例 ----------------
async def t_paths(ctx):
    assert paths.norm("") == "/"
    assert paths.norm("/") == "/"
    assert paths.norm("//a///b/") == "/a/b"
    assert paths.norm("/a/./b/../c") == "/a/c"
    assert paths.norm("/..") == "/"
    assert paths.parent("/a/b") == "/a"
    assert paths.base("/a/b") == "b"
    assert paths.under("/a/b", "/a") and not paths.under("/ab", "/a")
    # NFC/NFD 必须归一成同一个键
    nfc = paths.norm("/café")
    nfd = paths.norm("/cafe\u0301")
    assert nfc == nfd, f"NFC 归一失败: {nfc!r} != {nfd!r}"
    assert paths.bad("/a\x00b") and not paths.bad("/a b")


async def t_index(ctx):
    idx = ctx.index
    idx.put_dir("/d", 1)
    idx.put_file("/d/f", "a" * 32, 2)
    e = idx.get("/d/f")
    assert e and not e.is_dir and e.hash == "a" * 32
    assert [x.path for x in idx.children("/d")] == ["/d/f"]
    idx.content_put("a" * 32, 10, {0: 5})
    e = idx.get("/d/f")
    assert e.size == 10 and e.parts == {0: 5}
    rows = idx.take_under("/d")
    assert len(rows) == 2, rows
    assert idx.get("/d") is None and idx.get("/d/f") is None


async def t_put_get(ctx):
    data = rand(300000)
    v = ctx.vfs
    await v.mkdir("/p")
    tmp = v.staging_tmp()
    with open(tmp, "wb") as f:
        f.write(data)
    h = tg.digest_of(tmp)
    e = v.finish_put("/p/a.bin", tmp, h, len(data), 100)
    assert e.pending, "第一次写应该是上传中"
    await v.upload_pending("/p/a.bin")
    e = v.get("/p/a.bin")
    assert not e.pending and e.size == len(data), e
    out = b""
    async for c in v.iter_content("/p/a.bin"):
        out += c
    assert out == data, "内容不一致"


async def t_dedup(ctx):
    data = rand(5000)
    v = ctx.vfs
    for name in ("/x/1", "/x/2"):
        tmp = v.staging_tmp()
        with open(tmp, "wb") as f:
            f.write(data)
        h = tg.digest_of(tmp)
        v.finish_put(name, tmp, h, len(data), 1)
        await v.upload_pending(name)
    a, b = v.get("/x/1"), v.get("/x/2")
    assert a.hash == b.hash
    # 同一份内容只应该有一组分片消息
    assert a.parts == b.parts
    nm = len([m for m in ctx.client._msgs.values()
              if any(getattr(at, "file_name", "").startswith(a.hash)
                     for at in m.document.attributes)])
    assert nm == 1, f"秒传产生了 {nm} 条分片消息"


async def t_move_copy_delete(ctx):
    data = rand(4000)
    v = ctx.vfs
    tmp = v.staging_tmp()
    with open(tmp, "wb") as f:
        f.write(data)
    h = tg.digest_of(tmp)
    v.finish_put("/m/a.bin", tmp, h, len(data), 1)
    await v.upload_pending("/m/a.bin")
    await v.mkdir("/m/sub")
    await v.move("/m/a.bin", "/m/sub/a.bin")
    assert v.get("/m/a.bin") is None and v.get("/m/sub/a.bin") is not None
    await v.copy("/m/sub/a.bin", "/m/b.bin")
    assert v.get("/m/b.bin") is not None
    await v.delete("/m")
    assert v.get("/m") is None and v.get("/m/sub/a.bin") is None
    assert v.get("/m/b.bin") is None


async def t_bad_move(ctx):
    v = ctx.vfs
    await v.mkdir("/g")
    await v.mkdir("/g/sub")
    try:
        await v.move("/g", "/g/sub")
        raise AssertionError("应该拒绝移动到自己子树")
    except OSError:
        pass
    assert v.get("/g") is not None


async def t_migrate_legacy(ctx):
    """旧库（files 里带 size/parts）打开后要自动拆成 content + files。"""
    import sqlite3, tempfile as tf
    p = tf.mktemp(suffix=".db")
    db = sqlite3.connect(p)
    db.executescript("""
        CREATE TABLE files(path TEXT PRIMARY KEY, hash TEXT NOT NULL,
                           size INTEGER DEFAULT 0, mtime INTEGER DEFAULT 0,
                           parts TEXT DEFAULT '{}');
        CREATE TABLE dirs(path TEXT PRIMARY KEY, mtime INTEGER DEFAULT 0);
        CREATE TABLE meta(k TEXT PRIMARY KEY, v TEXT);
        CREATE INDEX idx_files_hash ON files(hash);
    """)
    db.execute("ALTER TABLE files ADD COLUMN status INTEGER DEFAULT 0")
    db.execute("ALTER TABLE files ADD COLUMN expect INTEGER DEFAULT 0")
    db.execute("INSERT INTO files VALUES('/old/a.bin', ?, 123, 5, ?, 0, 1)",
               ("b" * 32, '{"0": 77}'))
    db.execute("INSERT INTO files VALUES('/old/c.bin', ?, 123, 6, ?, 0, 1)",
               ("b" * 32, '{"0": 77}'))
    db.commit()
    db.close()
    idx = IDX.Index(p)
    e = idx.get("/old/a.bin")
    assert e and e.hash == "b" * 32 and e.size == 123 and e.parts == {0: 77}, e
    assert idx.get("/old/c.bin").hash == "b" * 32
    assert idx.referenced(["b" * 32]) == {"b" * 32: 2}
    # 迁移后内容表只有一行
    import sqlite3 as s3
    c = s3.connect(p)
    n = c.execute("SELECT COUNT(*) FROM content").fetchone()[0]
    cols = {r[1] for r in c.execute("PRAGMA table_info(files)")}
    c.close()
    assert n == 1, f"content 应有 1 行，实际 {n}"
    assert "size" not in cols and "parts" not in cols, f"files 还有旧列: {cols}"


async def t_snapshot_roundtrip(ctx):
    """推一份快照，再用另一个索引从 TG 拉回来。"""
    v = ctx.vfs
    tmp = v.staging_tmp()
    with open(tmp, "wb") as f:
        f.write(rand(2048))
    h = tg.digest_of(tmp)
    v.finish_put("/s/a.bin", tmp, h, 2048, 1)
    await ctx.store.push_snapshot()
    assert ctx.store.snap_msg_id
    idx2 = Index(tempfile.mktemp(suffix=".db"))
    st2 = tg.TGStore(ctx.client, 1, idx2, cache)
    assert await st2.pull_snapshot()
    e = idx2.get("/s/a.bin")
    assert e and e.hash == h and e.status == 1, e


async def t_lock_roundtrip(ctx):
    """抢锁 → 自己是持有者 → 释放后空闲。"""
    st = ctx.store
    st.snap_msg_id = 0
    await st.acquire(timeout=20)
    assert st.holding
    assert await st.check_lock() == "own"
    await st.push_snapshot()
    assert await st.check_lock() == "own"
    await st.release()
    assert await st.check_lock() == "idle"


async def t_multislice(ctx):
    """切片小一点，走多分片路径：上传、乱序读、Range 都要对。"""
    old = tg.SLICE
    tg.SLICE = 64 * 1024
    try:
        data = rand(200000)                    # 4 片，最后一片不足
        v = ctx.vfs
        tmp = v.staging_tmp()
        with open(tmp, "wb") as f:
            f.write(data)
        h = tg.digest_of(tmp)
        v.finish_put("/ms/a.bin", tmp, h, len(data), 1)
        e = await v.upload_pending("/ms/a.bin")
        assert len(e.parts) == 4, e.parts
        out = b""
        async for c in v.iter_content("/ms/a.bin"):
            out += c
        assert out == data, "多分片内容不一致"
        part = b""
        async for c in v.iter_content("/ms/a.bin", 100000, 500):
            part += c
        assert part == data[100000:100500], "跨分片 Range 不对"
    finally:
        tg.SLICE = old


async def t_concurrent_put(ctx):
    """并发写不同路径，索引不能串。"""
    async def one(i):
        v = ctx.vfs
        data = rand(3000 + i)
        tmp = v.staging_tmp()
        with open(tmp, "wb") as f:
            f.write(data)
        h = tg.digest_of(tmp)
        v.finish_put(f"/conc/{i}.bin", tmp, h, len(data), 1)
        await v.upload_pending(f"/conc/{i}.bin")
        return data
    data = await asyncio.gather(*[one(i) for i in range(8)])
    for i in range(8):
        out = b""
        async for c in ctx.vfs.iter_content(f"/conc/{i}.bin"):
            out += c
        assert out == data[i], f"第 {i} 个内容对不上"


async def t_uploader(ctx):
    """后台队列：登记完就不用管，worker 会把它传完。"""
    import main as M
    v = ctx.vfs
    part = b"queued" * 500
    tmp = v.staging_tmp()
    with open(tmp, "wb") as f:
        f.write(part)
    h = tg.digest_of(tmp)
    v.finish_put("/q/a.bin", tmp, h, len(part), 1)
    up = M.Uploader(v, 2)
    up.start()
    up.nudge()
    ok = False
    for _ in range(200):
        await asyncio.sleep(0.02)
        e = v.get("/q/a.bin")
        if e and not e.pending:
            ok = True
            break
    await up.stop()
    assert ok, "后台 worker 没传完"
    out = b""
    async for c in v.iter_content("/q/a.bin"):
        out += c
    assert out == part


async def t_reindex(ctx):
    """扫 TG 消息重建 content 表。"""
    v = ctx.vfs
    data = rand(3000)
    tmp = v.staging_tmp()
    with open(tmp, "wb") as f:
        f.write(data)
    h = tg.digest_of(tmp)
    v.finish_put("/ri/a.bin", tmp, h, len(data), 1)
    await v.upload_pending("/ri/a.bin")
    # 把 content 表清掉，模拟索引丢失（files 还引用着这份内容）
    import sqlite3
    c = sqlite3.connect(ctx.index.path)
    c.execute("DELETE FROM content WHERE hash=?", (h,))
    c.commit()
    c.close()
    assert ctx.index.content(h) is None
    await ctx.store.reindex()
    got = ctx.index.content(h)
    assert got and got[1], f"重建后没有分片: {got}"
    # 路径还在，不该进 lost+found
    assert ctx.vfs.get("/ri/a.bin") is not None
    # 把路径也删掉，模拟索引彻底丢失：内容应挂到 /lost+found 下
    ctx.index.drop("/ri/a.bin")
    await ctx.store.reindex()
    e = ctx.vfs.get(f"/lost+found/{h}")
    assert e is not None and e.hash == h, "没挂进 lost+found"
    out = b""
    async for c in ctx.vfs.iter_content(f"/lost+found/{h}"):
        out += c
    assert out == data, "lost+found 里的内容下载不出来"


async def t_lostfound_fragments(ctx):
    """序号不连续的残片：每个分片单独列出来，各自能下载到真实字节。"""
    from types import SimpleNamespace as NS
    h = "d" * 32
    p0 = rand(4000)
    p2 = rand(3000)
    await ctx.client.send_file(1, p0, attributes=[NS(file_name=f"{h}.0")])
    await ctx.client.send_file(1, p2, attributes=[NS(file_name=f"{h}.2")])
    await ctx.store.reindex()
    e0 = ctx.vfs.get(f"/lost+found/{h}.0")
    e2 = ctx.vfs.get(f"/lost+found/{h}.2")
    assert e0 and e0.size == 4000, e0
    assert e2 and e2.size == 3000, e2
    got0 = b""
    async for c in ctx.vfs.iter_content(f"/lost+found/{h}.0"):
        got0 += c
    assert got0 == p0, "分片 0 的内容不对"
    got2 = b""
    async for c in ctx.vfs.iter_content(f"/lost+found/{h}.2"):
        got2 += c
    assert got2 == p2, "分片 2 的内容不对"
    # 不该凭空合成一个完整的 /lost+found/<hash>
    assert ctx.vfs.get(f"/lost+found/{h}") is None
    # 内容键要带原始序号，不是重编号成 0
    assert ctx.index.content(f"{h}.0") is not None
    assert ctx.index.content(f"{h}.2") is not None
    assert ctx.index.content(f"{h}.1") is None


# ---------------- WebDAV ----------------
async def make_dav(ctx, readonly=False):
    from aiohttp.test_utils import TestClient, TestServer
    import webdav
    import main as M
    dav = webdav.DAV(ctx.vfs, None, readonly=readonly)
    app = M.make_app(dav)
    cli = TestClient(TestServer(app))
    await cli.start_server()
    return cli


async def t_dav_basic(ctx):
    cli = await make_dav(ctx)
    try:
        r = await cli.put("/dav/a.txt", data=b"hello")
        assert r.status == 201, r.status
        r = await cli.put("/dav/a.txt", data=b"hello2")
        assert r.status == 204, r.status
        r = await cli.get("/dav/a.txt")
        body = await r.read()
        assert r.status == 200 and body == b"hello2", (r.status, body)
        r = await cli.head("/dav/a.txt")
        assert r.headers.get("Content-Length") == "6"
        r = await cli.request("PROPFIND", "/dav", headers={"Depth": "1"})
        txt = await r.text()
        assert r.status == 207 and "a.txt" in txt, txt[:200]
        r = await cli.request("OPTIONS", "/dav")
        assert r.headers.get("DAV") == "1"
        r = await cli.request("LOCK", "/dav/a.txt")
        assert r.status == 405
        # 上传中的文件也要能读
        r = await cli.put("/dav/pending.bin", data=b"x" * 100)
        assert r.status == 201
        r = await cli.get("/dav/pending.bin")
        assert await r.read() == b"x" * 100
    finally:
        await cli.close()


async def t_dav_range(ctx):
    cli = await make_dav(ctx)
    try:
        data = bytes(range(256)) * 4000
        assert (await cli.put("/r/x.bin", data=data)).status == 201
        r = await cli.get("/r/x.bin", headers={"Range": "bytes=100-199"})
        assert r.status == 206, r.status
        assert await r.read() == data[100:200]
        assert r.headers.get("Content-Range") == f"bytes 100-199/{len(data)}"
        r = await cli.get("/r/x.bin", headers={"Range": "bytes=-50"})
        assert r.status == 206 and await r.read() == data[-50:]
        r = await cli.get("/r/x.bin", headers={"Range": f"bytes={len(data)}-"})
        assert r.status == 416, r.status
        r = await cli.get("/r/x.bin", headers={"If-None-Match": r.headers.get("ETag", "")})
        # 上面 r 是 416 的响应，重新取一次 ETag
        etag = (await cli.head("/r/x.bin")).headers.get("ETag")
        r = await cli.get("/r/x.bin", headers={"If-None-Match": etag})
        assert r.status == 304, r.status
    finally:
        await cli.close()


async def t_dav_content_range(ctx):
    cli = await make_dav(ctx)
    try:
        data = rand(200000)
        cut, cut2 = 80000, 150000
        r = await cli.put("/c/x.bin", data=data[:cut],
                          headers={"Content-Range": f"bytes 0-{cut-1}/{len(data)}"})
        assert r.status == 308, (r.status, await r.text())
        assert int(r.headers.get("X-Upload-Offset")) == cut
        # 进度查询：不写字节，只回已收长度
        r = await cli.put("/c/x.bin", data=b"",
                          headers={"Content-Range": f"bytes */{len(data)}"})
        assert r.status == 308 and int(r.headers.get("X-Upload-Offset")) == cut
        assert "X-Upload-Offset" in r.headers
        # 顺序错了要 400 并告诉真实位置
        r = await cli.put("/c/x.bin", data=data[cut2:],
                          headers={"Content-Range": f"bytes {cut2}-{len(data)-1}/{len(data)}"})
        assert r.status == 400 and int(r.headers.get("X-Upload-Offset")) == cut
        r = await cli.put("/c/x.bin", data=data[cut:cut2],
                          headers={"Content-Range": f"bytes {cut}-{cut2-1}/{len(data)}"})
        assert r.status == 308 and int(r.headers.get("X-Upload-Offset")) == cut2
        # 查询在第二块之后也对
        r = await cli.put("/c/x.bin", data=b"", headers={"Content-Range": "bytes */*"})
        assert int(r.headers.get("X-Upload-Offset")) == cut2
        r = await cli.put("/c/x.bin", data=data[cut2:],
                          headers={"Content-Range": f"bytes {cut2}-{len(data)-1}/{len(data)}"})
        assert r.status == 201, (r.status, await r.text())
        await ctx.vfs.upload_pending("/c/x.bin")
        r = await cli.get("/c/x.bin")
        assert await r.read() == data
        # 跨三块的累计摘要必须和整份一致
        e = ctx.index.get("/c/x.bin")
        assert e.size == len(data)
        assert e.hash == hashlib.shake_128(data).hexdigest(16), "跨请求累计摘要不一致"
        # 没有任何状态时查询回 0，而不是把文件写坏
        r = await cli.put("/c/none.bin", data=b"",
                          headers={"Content-Range": "bytes */999"})
        assert r.status == 308 and r.headers.get("X-Upload-Offset") == "0"
        assert ctx.index.get("/c/none.bin") is None
    finally:
        await cli.close()


class FakeBody:
    """会中途断线的 request body：吐完 parts 后按需抛 ConnectionResetError。"""

    def __init__(self, parts, boom=False):
        self.parts = parts
        self.boom = boom

    async def iter_chunked(self, n):
        for p in self.parts:
            yield p
        if self.boom:
            raise ConnectionResetError("peer reset")


class FakeReq:
    def __init__(self, parts, boom=False):
        self.content = FakeBody(parts, boom)
        self.headers = {}
        self.content_length = None
        self.path = "/"


def _req(path, parts, boom=False, cr=None):
    req = FakeReq(parts, boom)
    req.path = path
    if cr:
        req.headers["Content-Range"] = cr
    return req


async def t_dav_abort_resume(ctx):
    """传到一半断线：续传留住字节，普通 PUT 全丢且不写索引。"""
    import webdav
    dav = webdav.DAV(ctx.vfs, None)
    data = rand(300000)
    total, cut = len(data), 100000

    # 1) 普通 PUT 断线：字节全丢，索引不登记
    r = await dav.put(_req("/ab/plain.bin", [data[:cut]], boom=True))
    assert r.status == 400, r.status
    assert ctx.index.get("/ab/plain.bin") is None, "断线的普通 PUT 不该登记"

    # 2) 续传断线：已收字节留住，offset 可查
    r = await dav.put(_req("/ab/cr.bin", [data[:cut]], cr=f"bytes 0-{cut-1}/{total}"))
    assert r.status == 308, r.status
    r = await dav.put(_req("/ab/cr.bin", [data[cut:200000]], boom=True,
                           cr=f"bytes {cut}-199999/{total}"))
    assert r.status == 400, r.status
    st = dav._resume["/ab/cr.bin"]
    assert st["have"] == 200000, st["have"]
    assert os.path.getsize(st["tmp"]) == 200000, "断线后文件长度必须等于已算入摘要的字节"

    r = await dav.put(_req("/ab/cr.bin", [], cr=f"bytes */{total}"))
    assert r.status == 308 and r.headers["X-Upload-Offset"] == "200000", r.headers

    # 3) 临时文件被多写了字节，续传要能自愈（ftruncate 对齐）
    with open(st["tmp"], "ab") as f:
        f.write(b"garbage" * 100)
    r = await dav.put(_req("/ab/cr.bin", [data[200000:]],
                           cr=f"bytes 200000-{total-1}/{total}"))
    assert r.status == 201, (r.status, r.body)
    e = ctx.index.get("/ab/cr.bin")
    assert e.size == total, e.size
    assert e.hash == hashlib.shake_128(data).hexdigest(16), "断线重传后内容与原始字节不一致"
    assert not dav._resume, "收尾后状态要清干净"


async def t_dav_teleport(ctx):
    """秒传：客户端给 hash，服务端验过货就直接建路径。"""
    cli = await make_dav(ctx)
    try:
        data = rand(120000)
        r = await cli.put("/tp/a.bin", data=data)
        assert r.status == 201, r.status
        etag = r.headers["ETag"]
        h = etag.strip('"').rsplit("-", 1)[0]
        assert len(h) == 32 and h == hashlib.shake_128(data).hexdigest(16), etag
        await ctx.vfs.upload_pending("/tp/a.bin")

        # 同内容秒传到另一个路径：一个字节都不该收
        r = await cli.put("/tp/b.bin", data=b"", headers={"X-Content-Hash": h})
        assert r.status == 201, (r.status, await r.text())
        assert r.headers["ETag"] == etag, "秒传后的 ETag 必须和原文件一致"
        assert (await (await cli.get("/tp/b.bin")).read()) == data
        assert ctx.index.referenced([h])[h] == 2, "两条路径共用一份内容"
        # 覆盖已存在的路径回 204
        r = await cli.put("/tp/b.bin", data=b"", headers={"X-Content-Hash": h})
        assert r.status == 204, r.status
        # 父目录自动补
        r = await cli.put("/tp/deep/x/y.bin", data=b"", headers={"X-Content-Hash": h})
        assert r.status == 201, r.status
        assert ctx.vfs.get("/tp/deep") is not None

        # 大小对不上：拒绝
        r = await cli.put("/tp/sz.bin", data=b"", headers={
            "X-Content-Hash": h, "X-Content-Length": "1"})
        assert r.status == 412, r.status
        # 大小对上：通过
        r = await cli.put("/tp/sz.bin", data=b"", headers={
            "X-Content-Hash": h, "X-Content-Length": str(len(data))})
        assert r.status == 201, r.status
        # 库里没有的 hash
        r = await cli.put("/tp/no.bin", data=b"",
                          headers={"X-Content-Hash": "0" * 32})
        assert r.status == 412 and r.headers["X-Content-Hash"] == "0" * 32
        # 畸形 hash
        for bad_h in ("xyz", h[:31], h + "0", ""):
            r = await cli.put("/tp/no.bin", data=b"",
                              headers={"X-Content-Hash": bad_h})
            if bad_h == "":
                assert r.status == 201, "空 header 等于没带，走普通空文件上传"
            else:
                assert r.status == 400, (bad_h, r.status)
        # 目标是个目录
        r = await cli.put("/tp", data=b"", headers={"X-Content-Hash": h})
        assert r.status == 409, r.status
        # 带 body 又带 hash：body 说了算，走普通上传，内容不被 hash 顶掉
        r = await cli.put("/tp/body.bin", data=b"payload",
                          headers={"X-Content-Hash": h})
        assert r.status == 201, r.status
        assert (await (await cli.get("/tp/body.bin")).read()) == b"payload"

        # 内容还没上 TG（只在本地暂存里）也能秒传，落地状态是「上传中」
        pend = rand(5000)
        r = await cli.put("/tp/pend0.bin", data=pend)
        assert r.status == 201
        ph = r.headers["ETag"].strip('"').rsplit("-", 1)[0]
        assert ctx.vfs.get("/tp/pend0.bin").pending
        r = await cli.put("/tp/pend1.bin", data=b"", headers={"X-Content-Hash": ph})
        assert r.status == 201, (r.status, await r.text())
        assert ctx.vfs.get("/tp/pend1.bin").pending, "内容没上 TG，路径要用上传中状态"
        await ctx.vfs.upload_pending("/tp/pend1.bin")
        assert (await (await cli.get("/tp/pend1.bin")).read()) == pend

        # 0 字节内容也能秒传
        r = await cli.put("/tp/empty0", data=b"")
        assert r.status == 201, r.status
        eh = r.headers["ETag"].strip('"').rsplit("-", 1)[0]
        r = await cli.put("/tp/empty1", data=b"", headers={"X-Content-Hash": eh})
        assert r.status == 201, (r.status, await r.text())
        r = await cli.get("/tp/empty1")
        assert r.status == 200 and await r.read() == b""
        assert r.headers.get("ETag") == f'"{eh}-0"', r.headers.get("ETag")

        # 只读模式一律 403
        import webdav
        ro = webdav.DAV(ctx.vfs, None, readonly=True)
        req = _req("/tp/ro.bin", [])
        req.headers["X-Content-Hash"] = h
        assert (await ro.put(req)).status == 403
    finally:
        await cli.close()


async def t_dav_move_copy(ctx):
    cli = await make_dav(ctx)
    try:
        await cli.put("/mv/a.txt", data=b"1")
        url = str(cli.make_url("/mv/b.txt"))
        r = await cli.request("MOVE", "/mv/a.txt", headers={"Destination": url})
        assert r.status == 201, (r.status, await r.text())
        assert ctx.vfs.get("/mv/b.txt") is not None
        r = await cli.request("MOVE", "/mv/b.txt", headers={"Destination": url})
        assert r.status == 204, r.status
        r = await cli.request("MKCOL", "/mv/d")
        assert r.status == 201
        r = await cli.request("MOVE", "/mv/d", headers={"Destination": str(cli.make_url("/mv/b.txt"))})
        assert r.status == 409, r.status
        r = await cli.request("COPY", "/mv/b.txt", headers={"Destination": str(cli.make_url("/mv/c.txt"))})
        assert r.status == 201, r.status
        r = await cli.delete("/mv")
        assert r.status == 204 and ctx.vfs.get("/mv/b.txt") is None
    finally:
        await cli.close()


async def t_dav_depth_infinity(ctx):
    cli = await make_dav(ctx)
    try:
        r = await cli.request("PROPFIND", "/", headers={"Depth": "infinity"})
        assert r.status == 403, r.status
    finally:
        await cli.close()


# ---------------- 边界 / 异常 ----------------
async def t_paths_edge(ctx):
    n = paths.norm
    assert n("") == "/" and n("/") == "/" and n("//") == "/" and n("///") == "/"
    assert n("a") == "/a" and n("a/") == "/a" and n("./a") == "/a"
    assert n("/a/./b/") == "/a/b"
    assert n("/a//b") == "/a/b"
    assert n("/a/../b") == "/b"
    assert n("/a/../../b") == "/b", "越出根之后必须夹回根"
    assert n("..") == "/" and n("../..") == "/"
    assert n("/..") == "/"
    assert n("/a/.") == "/a"
    assert n("/a/..") == "/"
    assert n("/e\u0301") == n("/é"), "NFC 归一化"
    assert n("/a\\b") == "/a\\b", "反斜杠不是分隔符"
    assert n("/a/%2e%2e/b") == "/a/%2e%2e/b", "百分号编码不在这一层解码"
    assert n("/中文/文件.txt") == "/中文/文件.txt"
    assert n("/🎉/x") == "/🎉/x"
    assert n("/ ") == "/ ", "空格是合法文件名字符"
    assert n("/a b") == "/a b"
    assert n("/a" * 500) == "/a" * 500

    assert not paths.bad("/a/b.txt")
    assert not paths.bad("/中文/x")
    assert paths.bad("/a\x00b")
    assert paths.bad("/a\nb")
    assert paths.bad("/a\tb")
    assert paths.bad("/a\x7fb")
    assert paths.bad("/a\x01")
    assert not paths.bad("")

    assert paths.base("/") == ""
    assert paths.base("/a") == "a"
    assert paths.base("/a/b/c") == "c"
    assert paths.parent("/") == "/"
    assert paths.parent("/a") == "/"
    assert paths.parent("/a/b") == "/a"
    assert paths.under("/a/b", "/a")
    assert not paths.under("/a", "/a")
    assert not paths.under("/ab", "/a"), "前缀不等于子树"
    assert paths.under("/a", "/")
    assert not paths.under("/", "/")
    assert not paths.under("/x", "/x/")


async def t_index_edge(ctx):
    db = IDX.Index(tempfile.mktemp(suffix=".db"))
    db.put_dir("/d", 1)
    db.put_file("/d", "h1", 2)             # 目录位置直接放文件
    e = db.get("/d")
    assert e is not None and not e.is_dir and e.hash == "h1"
    db.put_dir("/d", 3)                    # 又变回目录
    e = db.get("/d")
    assert e is not None and e.is_dir

    db.put_file("/f.txt", "h2", 10, IDX.STATUS_PENDING)
    assert db.get("/f.txt").pending
    db.set_status("/f.txt", IDX.STATUS_DONE)
    assert not db.get("/f.txt").pending
    db.set_status("/f.txt", IDX.STATUS_PENDING)
    assert db.get("/f.txt").pending

    db.put_dir("/dir", 1)
    db.put_dir("/dir/sub", 1)
    db.put_file("/dir/a", "ha", 1)
    db.put_file("/dir/b", "hb", 1)
    db.put_file("/dir/sub/c", "hc", 1)
    kids = db.children("/dir")
    assert [k.path for k in kids] == ["/dir/a", "/dir/b", "/dir/sub"], kids
    assert kids[-1].is_dir
    assert [k.path for k in db.children("/dir/sub")] == ["/dir/sub/c"]
    assert db.children("/nope") == []
    root = db.children("/")
    assert all(k.path.count("/") == 1 for k in root)

    under = db.list_under("/dir")
    assert ("/dir", "", True) in under
    assert ("/dir/sub/c", "hc", False) in under
    assert len(under) == 5, under
    assert db.list_under("/nope") == [], db.list_under("/nope")

    taken = db.take_under("/dir")
    assert len(taken) == 5
    assert db.get("/dir/sub/c") is None
    assert db.get("/dir") is None
    assert db.take_under("/nope") == []

    db.put_file("/keep", "hk", 1)
    allrows = db.take_under("/")
    assert ("/keep", "hk", False) in allrows
    assert db.get("/keep") is not None, "摘根本身不该删东西"

    # referenced 分批
    hs = ["%032x" % i for i in range(1200)]
    for i, h in enumerate(hs[:1100]):
        db.put_file(f"/m/{i}", h, 1)
    ref = db.referenced(hs)
    assert len(ref) == 1100, len(ref)
    assert ref[hs[0]] == 1
    assert db.referenced([]) == {}

    # 内容回收
    db.content_put("hz", 5, {0: 11})
    assert db.content("hz") == (5, {0: 11})
    assert db.content("") is None
    assert db.content("不存在") is None
    assert db.content_drop("hz") is True
    assert db.content("hz") is None
    db.put_file("/zz", "hz2", 1)
    db.content_put("hz2", 1, {})
    assert db.content_drop("hz2") is False, "还有人引用就不能删"
    assert db.content("hz2") == (1, {})
    assert db.content_exists("hz2")

    # meta
    assert db.meta("nope") is None
    assert db.meta("nope", "dflt") == "dflt"
    db.set_meta("k", 1)
    assert db.meta("k") == "1"
    db.set_meta("k", "two")
    assert db.meta("k") == "two"
    db.remove_meta("k")
    assert db.meta("k") is None
    db.remove_meta("k")

    # drop
    assert db.drop("/") == 0
    assert db.drop("/nothere") == 0
    assert db.drop("/keep") == 1
    assert db.drop("/keep") == 0

    # parts 编解码
    assert IDX.load_parts("") == {}
    assert IDX.load_parts("{}") == {}
    assert IDX.load_parts("{bad json") == {}
    assert IDX.load_parts('{"0": 5, "1": 7}') == {0: 5, 1: 7}
    assert IDX.load_parts('{"0": 0}') == {}, "0 是无效消息 id，要丢掉"
    assert IDX.dump_parts({0: 5, 1: 0}) == '{"0":5}'
    assert IDX.dump_parts({}) == "{}"

    # dump / restore
    d = tempfile.mktemp(suffix=".db")
    db.dump(d)
    db2 = IDX.Index(tempfile.mktemp(suffix=".db"))
    db2.restore(d)
    assert db2.get("/zz") is not None
    assert db2.content("hz2") == (1, {})
    try:
        db2.restore("/不存在/的文件")
        raise AssertionError("应当抛 FileNotFoundError")
    except FileNotFoundError:
        pass
    bad_db = tempfile.mktemp(suffix=".db")
    with open(bad_db, "wb") as f:
        f.write(b"not a database at all")
    before = db2.get("/zz")
    try:
        db2.restore(bad_db)
        raise AssertionError("坏库应当拒绝")
    except Exception:
        pass
    assert db2.get("/zz") is not None and before is not None, "坏库不能污染现有库"

    # 迁移只跑一次
    db3 = IDX.Index(tempfile.mktemp(suffix=".db"))
    assert db3.all_files() == []


async def t_cache_edge(ctx):
    old_dir = cache.CACHE_DIR
    old_env = os.environ.get("CACHE_MAX_GB")
    cache.CACHE_DIR = tempfile.mkdtemp(prefix="tgnd_ce_")
    try:
        assert cache.limit() == int(float(cache.DEFAULT_LIMIT_GB) * 2 ** 30)
        os.environ["CACHE_MAX_GB"] = "0"
        assert cache.limit() == 0, "0 表示不限制"
        os.environ["CACHE_MAX_GB"] = "-3"
        assert cache.limit() == 0
        os.environ["CACHE_MAX_GB"] = "abc"
        assert cache.limit() == int(cache.DEFAULT_LIMIT_GB * 2 ** 30), "非法值回默认"
        os.environ["CACHE_MAX_GB"] = "1"
        assert cache.limit() == 2 ** 30
        os.environ["CACHE_MAX_GB"] = "" 
        assert cache.limit() == int(cache.DEFAULT_LIMIT_GB * 2 ** 30)
        os.environ["CACHE_MAX_GB"] = old_env or "16"

        assert cache.usage()[0] == 0
        assert cache.find("nope") == ""
        assert not cache.has_staging("nope")

        src = tempfile.mktemp()
        with open(src, "wb") as f:
            f.write(b"x" * 100)
        cache.put("h1", src)
        assert os.path.exists(cache.blob_path("h1"))
        assert cache.find("h1") == cache.blob_path("h1")
        assert not os.path.exists(src), "put 是移动不是复制"

        s2 = tempfile.mktemp()
        with open(s2, "wb") as f:
            f.write(b"y" * 50)
        os.replace(s2, cache.staging_path("h2"))
        assert cache.find("h2") == cache.staging_path("h2"), "blobs 没有就看 staging"
        assert cache.has_staging("h2")
        assert cache.commit("h2")
        assert not cache.has_staging("h2")
        assert cache.find("h2") == cache.blob_path("h2")
        assert cache.commit("nope") == ""

        # 同 hash 两边都有：blobs 优先
        with open(cache.staging_path("h1"), "wb") as f:
            f.write(b"z")
        assert cache.find("h1") == cache.blob_path("h1")

        assert cache.usage()[0] == 151, cache.usage()
        cache.drop("h1")
        assert cache.find("h1") == ""
        assert not os.path.exists(cache.staging_path("h1"))
        cache.drop("nope")

        # 点文件计入占用但不进淘汰列表
        with open(os.path.join(cache.staging_dir(), ".tmp-1"), "wb") as f:
            f.write(b"9" * 30)
        total, items = cache.usage()
        assert total == 80 and len(items) == 1, (total, items)
        os.environ["CACHE_MAX_GB"] = "1e-9"
        assert cache.trim() > 0
        assert not os.path.exists(cache.blob_path("h2")), "预算压到最低要淘汰 blobs"
        assert os.path.exists(os.path.join(cache.staging_dir(), ".tmp-1")), "点文件不淘汰"
        cache.sweep(0)
        assert not os.path.exists(os.path.join(cache.staging_dir(), ".tmp-1"))
        with open(os.path.join(cache.staging_dir(), ".tmp-2"), "wb") as f:
            f.write(b"1")
        assert cache.sweep(3600) == 0
        assert cache.sweep(0) == 1

        os.environ["CACHE_MAX_GB"] = "0"
        assert cache.ensure(10 ** 12), "不限制时永远够"
        os.environ["CACHE_MAX_GB"] = "1e-9"
        assert not cache.ensure(10 ** 6)
        with cache.hold(10 ** 6) as ok:
            assert not ok
        with cache.hold(0) as ok:
            assert ok
        os.environ["CACHE_MAX_GB"] = "1e-6"      # 约 1073 字节
        with cache.hold(10) as ok:
            assert ok
            with cache.hold(10) as ok2:
                assert ok2
        assert cache._reserved == 0, "预留必须释放干净"
        try:
            with cache.hold(10) as ok:
                assert ok
                raise RuntimeError("boom")
        except RuntimeError:
            pass
        assert cache._reserved == 0, "异常路径也要释放预留"
    finally:
        cache.CACHE_DIR = old_dir
        if old_env is None:
            os.environ.pop("CACHE_MAX_GB", None)
        else:
            os.environ["CACHE_MAX_GB"] = old_env


async def t_tg_digest(ctx):
    for n in (0, 1, 2, 7, 100, 4095, 4096, 4097, 65537):
        data = rand(n)
        p = tempfile.mktemp()
        with open(p, "wb") as f:
            f.write(data)
        ref = hashlib.shake_128(data).hexdigest(16)
        assert len(ref) == 32
        assert tg.digest_of(p) == ref, n
        for buf in (1, 3, 64, 4096, 1 << 20):
            assert tg.digest_of(p, bufsize=buf) == ref, (n, buf)
        dg = tg.Digest()
        if n == 0:
            assert dg.size == 0 and dg.hex() == ref
        step = max(1, n // 7)
        for i in range(0, n, step):
            dg.update(data[i:i + step])
        assert dg.size == n and dg.hex() == ref, n
        dg2 = tg.Digest()
        for i in range(min(n, 50)):
            dg2.update(bytes([data[i]]))
        assert dg2.hex() == hashlib.shake_128(data[:50]).hexdigest(16)
        os.unlink(p)


async def t_tg_names(ctx):
    from types import SimpleNamespace as NS
    h = "0123456789abcdef0123456789abcdef"
    assert tg.slice_name(h, 0) == h + ".0"
    assert tg.slice_name(h, 12345) == h + ".12345"
    assert tg.fragment_key(h, 7) == tg.slice_name(h, 7)
    assert tg.parse_slice_name(h + ".0") == (h, 0)
    assert tg.parse_slice_name(h + ".999") == (h, 999)
    assert tg.parse_slice_name("A" * 32 + ".1") == ("A" * 32, 1), "大小写不在这里归一"
    for bad in ("", h, h + ".", h + ".-1", h + ".1.2", h + ". 1", h + ".a",
                "short.1", ".1", "x" * 33 + ".1", h + ".1 ", None):
        assert tg.parse_slice_name(bad) == (None, -1), bad
    assert tg.is_db_msg(NS(document=NS(attributes=[NS(file_name=tg.DB_NAME)])))
    assert not tg.is_db_msg(NS(document=NS(attributes=[NS(file_name="x.txt")])))
    assert not tg.is_db_msg(NS(document=NS(attributes=[])))
    assert not tg.is_db_msg(NS(document=None))
    assert not tg.is_db_msg(None)


async def t_tg_slicereader(ctx):
    import io
    f = io.BytesIO(b"0123456789")
    r = tg.SliceReader(f, 2, 5)
    assert r.tell() == 0
    assert r.read(3) == b"234" and r.tell() == 3
    assert r.read() == b"56" and r.tell() == 5
    assert r.read() == b"" and r.read(100) == b""
    r.seek(0)
    assert r.read(2) == b"23"
    r.seek(1, 1)
    assert r.read(2) == b"56", "相对定位"
    r.seek(-2, 2)
    assert r.read() == b"56", "从末尾相对定位"
    r.seek(100)
    assert r.read() == b""
    r.seek(-3, 2)
    assert r.read(1) == b"4"
    r.close()
    r2 = tg.SliceReader(io.BytesIO(b"abc"), 0, 0)
    assert r2.read() == b""
    assert r2.tell() == 0


async def t_tg_upload(ctx):
    from types import SimpleNamespace as NS
    cli = FakeClient()
    db = IDX.Index(tempfile.mktemp(suffix=".db"))
    st = tg.TGStore(cli, 1, db, cache)
    data = rand(tg.PART * 2 + 1234)
    p = tempfile.mktemp()
    with open(p, "wb") as f:
        f.write(data)
    ref = await tg.upload_parallel(cli, p)
    parts = cli._uploads[ref.id]
    assert len(parts) == 3, parts
    assert b"".join(parts[i] for i in sorted(parts)) == data
    f = open(p, "rb")
    try:
        ref2 = await tg.upload_parallel(cli, f)
        assert cli._uploads[ref2.id][0] == data[:tg.PART]
        assert len(cli._uploads[ref2.id]) == 3
    finally:
        f.close()
    empty = tempfile.mktemp()
    open(empty, "wb").close()
    ref3 = await tg.upload_parallel(cli, empty)
    assert len(cli._uploads.get(ref3.id, {})) == 1, "空文件也发一个 part"

    h = tg.digest_of(p)
    got = await st.upload(h, p, len(data))
    assert set(got) == {0}, got
    msg = await cli.get_messages(1, ids=got[0])
    assert msg.document.attributes[0].file_name == f"{h}.0"
    n1 = len(cli._msgs)
    got2 = await st.upload(h, p, len(data))
    assert got2 == got and len(cli._msgs) == n1, "已有内容不该重复上传"
    assert db.content(h) == (len(data), got), db.content(h)

    # SaveBigFilePart 失败 → 退回顺序 send_file
    class BadCli(FakeClient):
        async def __call__(self, req):
            if type(req).__name__ == "SaveBigFilePartRequest":
                raise RuntimeError("dc down")
            return await super().__call__(req)

    data2 = rand(7000)
    p2 = tempfile.mktemp()
    with open(p2, "wb") as f:
        f.write(data2)
    bc = BadCli()
    st2 = tg.TGStore(bc, 1, IDX.Index(tempfile.mktemp(suffix=".db")), cache)
    h2 = tg.digest_of(p2)
    parts2 = await st2.upload(h2, p2, len(data2))
    assert bc._blobs[parts2[0]] == data2, "退路也要把字节传对"

    # 建消息失败：拿 ref 重试，不重传字节
    class FlakyCli(FakeClient):
        def __init__(self, fail):
            super().__init__()
            self.fail = fail
            self.calls = 0

        async def send_file(self, *a, **k):
            self.calls += 1
            if self.calls <= self.fail:
                raise RuntimeError("flaky")
            return await super().send_file(*a, **k)

    data3 = rand(9000)
    p3 = tempfile.mktemp()
    with open(p3, "wb") as f:
        f.write(data3)
    h3 = tg.digest_of(p3)
    real_sleep = asyncio.sleep
    asyncio.sleep = lambda *a, **k: real_sleep(0)
    try:
        fc = FlakyCli(2)
        st3 = tg.TGStore(fc, 1, IDX.Index(tempfile.mktemp(suffix=".db")), cache)
        parts3 = await st3.upload(h3, p3, len(data3))
        assert fc.calls == 3, fc.calls
        assert fc._blobs[parts3[0]] == data3
        fc2 = FlakyCli(99)
        st4 = tg.TGStore(fc2, 1, IDX.Index(tempfile.mktemp(suffix=".db")), cache)
        try:
            await st4.upload(h3, p3, len(data3))
            raise AssertionError("重试 3 次都失败就必须抛出来")
        except RuntimeError:
            pass
        assert fc2.calls == 3, fc2.calls
    finally:
        asyncio.sleep = real_sleep


async def t_tg_lock(ctx):
    cli = FakeClient()
    db = IDX.Index(tempfile.mktemp(suffix=".db"))
    st = tg.TGStore(cli, 1, db, cache)
    assert await st.check_lock() == "gone"
    await st.push_snapshot(caption=tg.LOCK_IDLE)
    m = await st.snap_msg()
    assert m is not None
    assert await st.check_lock() == "idle"
    assert st.snap_msg_id == m.id
    assert await st.acquire(timeout=5) is True
    assert st.holding and await st.check_lock() == "own"
    assert st._lock_text() == tg.LOCK_PREFIX + st.token
    await st.release()
    assert not st.holding and await st.check_lock() == "idle"
    await cli.edit_message(1, m.id, text="lock:deadbeefdeadbeefdeadbeefdeadbeef")
    assert await st.check_lock() == "other"
    await cli.edit_message(1, m.id, text="")
    assert await st.check_lock() == "idle"
    await cli.edit_message(1, m.id, text="lock:deadbeef")
    try:
        await st.acquire(timeout=0.05)
        raise AssertionError("别人持有锁时必须超时")
    except TimeoutError:
        pass
    assert tg.TGStore._make_token() == tg.TGStore._make_token(), "同一 session token 必须稳定"
    old = os.environ.get("TG_SESSION")
    try:
        os.environ["TG_SESSION"] = "aaa"
        t1 = tg.TGStore._make_token()
        os.environ["TG_SESSION"] = "bbb"
        assert tg.TGStore._make_token() != t1, "换 session 就换 token"
    finally:
        if old is None:
            os.environ.pop("TG_SESSION", None)
        else:
            os.environ["TG_SESSION"] = old
    # release 没拿锁时是空操作
    await st.release()
    assert not st.holding


async def t_tg_snapshot(ctx):
    cli = FakeClient()
    db = IDX.Index(tempfile.mktemp(suffix=".db"))
    st = tg.TGStore(cli, 1, db, cache)
    db.put_dir("/sn", 1)
    db.put_file("/sn/a", "ha", 5)
    await st.push_snapshot(caption=tg.LOCK_IDLE)
    mid = st.snap_msg_id
    assert mid and db.meta("snap_msg_id") == str(mid)
    assert db.meta("dirty") == "0"
    db.put_file("/sn/b", "hb", 6)
    n_msgs = len(cli._msgs)
    await st.push_snapshot()
    assert st.snap_msg_id == mid, "原地替换，不新建消息"
    assert len(cli._msgs) == n_msgs

    db2 = IDX.Index(tempfile.mktemp(suffix=".db"))
    st2 = tg.TGStore(cli, 1, db2, cache)
    assert await st2.pull_snapshot() is True
    assert db2.get("/sn/a") is not None and db2.get("/sn/b") is not None
    assert db2.meta("snap_msg_id") == str(mid)

    # merge：本地独有的路径要补回来
    db2.put_file("/sn/local", "hl", 7)
    db2.put_file("/sn/a", "ZZ", 99)
    db.put_file("/sn/remote2", "hr", 8)
    await st.push_snapshot()
    assert await st2.pull_snapshot(merge=True) is True
    assert db2.get("/sn/remote2") is not None, "远端新增的要有"
    assert db2.get("/sn/local") is not None, "本地独有的要补回"
    assert db2.get("/sn/a").hash == "ha", "同路径以远端为准"
    assert db2.meta("dirty") == "1", "补回之后要标脏"

    empty = FakeClient()
    db3 = IDX.Index(tempfile.mktemp(suffix=".db"))
    db3.put_file("/keep", "hx", 1)
    st3 = tg.TGStore(empty, 1, db3, cache)
    assert await st3.pull_snapshot() is False
    assert db3.get("/keep") is not None, "TG 上没快照就保留本地"

    # 坏快照：不能把本地库搞坏
    bad = FakeClient()
    db4 = IDX.Index(tempfile.mktemp(suffix=".db"))
    db4.put_file("/keep", "hx", 1)
    st4 = tg.TGStore(bad, 1, db4, cache)
    junk = tempfile.mktemp()
    with open(junk, "wb") as f:
        f.write(b"not an xz file")
    await bad.send_file(1, junk, caption=tg.LOCK_IDLE, attributes=[])
    assert await st4.pull_snapshot() is False
    assert db4.get("/keep") is not None


async def t_tg_slices(ctx):
    old = tg.SLICE
    tg.SLICE = 1024
    try:
        cli = FakeClient()
        db = IDX.Index(tempfile.mktemp(suffix=".db"))
        st = tg.TGStore(cli, 1, db, cache)
        data = rand(3000)
        p = tempfile.mktemp()
        with open(p, "wb") as f:
            f.write(data)
        h = tg.digest_of(p)
        parts = await st.upload(h, p, len(data))
        assert len(parts) == 3, parts
        out = b"".join([c async for c in st.iter_slices(parts, len(data))])
        assert out == data
        for off, ln in [(0, 1), (0, 1023), (1, 1023), (1023, 2), (1024, 1),
                        (1024, 2048), (2999, 1), (0, 3000)]:
            out = b"".join([c async for c in st.iter_slices(parts, len(data), off, ln)])
            assert out == data[off:off + ln], (off, ln)
        out = b"".join([c async for c in st.iter_slices(parts, len(data), 3000, 10)])
        assert out == b""
        out = b"".join([c async for c in st.iter_slices(parts, len(data), 1000, 10 ** 6)])
        assert out == data[1000:]
        assert st.cache.find(h) == ""
        out = b"".join([c async for c in st.read_all(h, parts, len(data))])
        assert out == data
        assert st.cache.find(h) != "", "整份读完要落缓存"
        out = b"".join([c async for c in st.read_all(h, parts, len(data))])
        assert out == data, "第二次走缓存"
        # 分片消息被删掉：读要安静地结束，不能抛
        await cli.delete_messages(1, list(parts.values()))
        out = b"".join([c async for c in st.iter_slices(parts, len(data))])
        assert out == b""
    finally:
        tg.SLICE = old


async def t_tg_reindex_extra(ctx):
    from types import SimpleNamespace as NS
    cli = FakeClient()
    db = IDX.Index(tempfile.mktemp(suffix=".db"))
    st = tg.TGStore(cli, 1, db, cache)
    data = rand(5000)
    p = tempfile.mktemp()
    with open(p, "wb") as f:
        f.write(data)
    h = tg.digest_of(p)
    msg = await cli.send_file(1, p, attributes=[NS(file_name=f"{h}.0")])
    db.content_put(h, len(data), {})
    db.put_file("/re/a.bin", h, 1, IDX.STATUS_PENDING)
    snap = await cli.send_file(1, p, caption=tg.LOCK_IDLE,
                               attributes=[NS(file_name=tg.DB_NAME)])
    n = await st.reindex()
    assert n == 2, n
    e = db.get("/re/a.bin")
    assert e and not e.pending, "分片齐了要把上传中改成完成"
    assert db.content(h) == (len(data), {0: msg.id}), db.content(h)
    assert db.get(f"/lost+found/{h}") is None, "有路径引用就不进 lost+found"
    assert st.snap_msg_id == snap.id
    # 不是分片名字的普通消息要无视
    await cli.send_file(1, p, attributes=[NS(file_name="random.txt")])
    n2 = await st.reindex()
    assert n2 == 3
    assert len(db.all_files()) == 1, db.all_files()
async def _mkfile(v, path, data, mtime=1, upload=True):
    """把一个文件写进 VFS：走正常路径（staging → finish_put → 可选上传）。"""
    tmp = v.staging_tmp()
    with open(tmp, "wb") as f:
        f.write(data)
    h = tg.digest_of(tmp)
    v.finish_put(path, tmp, h, len(data), mtime)
    if upload:
        await v.upload_pending(path)
    return h


async def t_vfs_move_matrix(ctx):
    v, ix = ctx.vfs, ctx.index
    await v.mkdir("/mv2")
    h = await _mkfile(v, "/mv2/a", b"hello world")
    h2 = await _mkfile(v, "/mv2/d1/f", b"nested")
    await v.mkdir("/mv2/d1/empty")

    await v.move("/mv2/a", "/mv2/b")
    assert v.get("/mv2/a") is None and v.get("/mv2/b").hash == h

    await v.move("/mv2/d1", "/mv2/d2")
    assert v.get("/mv2/d1") is None
    assert v.get("/mv2/d2/f").hash == h2
    assert v.get("/mv2/d2/empty").is_dir, "空目录也要跟着搬"
    assert v.get("/mv2/d2/d1") is None

    await v.move("/mv2/b", "/mv2/new/deep/b")
    assert v.get("/mv2/new/deep").is_dir
    assert v.get("/mv2/new/deep/b").hash == h

    h3 = await _mkfile(v, "/mv2/x", b"xyz")
    h4 = await _mkfile(v, "/mv2/y", b"4321")
    assert h3 != h4
    await v.move("/mv2/x", "/mv2/y")
    assert v.get("/mv2/y").hash == h3
    assert ix.content(h4) is None, "被覆盖的内容没人引用就要回收"

    before = v.get("/mv2/y").hash
    await v.move("/mv2/y", "/mv2/y")
    assert v.get("/mv2/y").hash == before, "自己移到自己应该是空操作"

    for src, dst in (("/mv2/d2", "/mv2/d2/f/x"),
                     ("/mv2/y", "/"),
                     ("/mv2/d2/f", "/mv2/d2")):
        try:
            await v.move(src, dst)
            raise AssertionError(f"必须拒绝 {src} → {dst}")
        except OSError:
            pass
    assert v.get("/mv2/d2/f") is not None, "被拒绝的移动不能动数据"
    try:
        await v.move("/mv2/不存在", "/mv2/z")
        raise AssertionError("源不存在要抛")
    except FileNotFoundError:
        pass
    assert v.get("/mv2/z") is None


async def t_vfs_copy_matrix(ctx):
    v, ix = ctx.vfs, ctx.index
    h = await _mkfile(v, "/cp/a", b"copy me")
    await v.copy("/cp/a", "/cp/b")
    a, b = v.get("/cp/a"), v.get("/cp/b")
    assert a.hash == b.hash == h
    assert ix.referenced([h])[h] == 2, "复制是引用，不是再存一份"
    await v.delete("/cp/a")
    assert ix.content(h) is not None, "还有一条引用，内容不能删"
    assert v.get("/cp/b") is not None

    h2 = await _mkfile(v, "/cp/dir/f", b"deep")
    await v.copy("/cp/dir", "/cp/dir2")
    assert v.get("/cp/dir2/f").hash == h2
    assert v.get("/cp/dir/f").hash == h2

    h3 = await _mkfile(v, "/cp/dst", b"old")
    await v.copy("/cp/b", "/cp/dst")
    assert v.get("/cp/dst").hash == h
    assert ix.content(h3) is None, "覆盖掉的旧内容要回收"

    for src, dst in (("/cp/dir", "/cp/dir/sub/x"),
                     ("/cp/b", "/"),
                     ("/cp/dir/f", "/cp/dir"),
                     ("/cp/b", "/cp/b")):
        try:
            await v.copy(src, dst)
        except OSError:
            continue
        assert src == dst, f"{src} → {dst} 本该被拒绝"
    assert v.get("/cp/b") is not None
    try:
        await v.copy("/cp/nope", "/cp/x")
        raise AssertionError("源不存在要抛")
    except FileNotFoundError:
        pass


async def t_vfs_delete_matrix(ctx):
    v, ix = ctx.vfs, ctx.index
    h = await _mkfile(v, "/de/a", b"1")
    h2 = await _mkfile(v, "/de/sub/b", b"22")
    await v.mkdir("/de/sub/empty")
    await v.delete("/de/a")
    assert v.get("/de/a") is None
    assert ix.content(h) is None
    await v.delete("/de/sub")
    assert v.get("/de/sub") is None and v.get("/de/sub/b") is None
    assert ix.content(h2) is None
    try:
        await v.delete("/de/nope")
        raise AssertionError("删不存在的东西要抛")
    except FileNotFoundError:
        pass
    # 删根：不能被当成「删光整个盘」
    n_files = len(ix.all_files())
    await v.delete("/")
    assert len(ix.all_files()) == n_files, "delete('/') 不该删任何路径"
    assert v.get("/") is not None and v.get("/").is_dir


async def t_vfs_reclaim(ctx):
    v, ix = ctx.vfs, ctx.index
    data = b"shared bytes"
    h = await _mkfile(v, "/rc/a", data)
    await v.copy("/rc/a", "/rc/b")
    assert v.get("/rc/b").hash == h
    cache.put(h, _dupfile(data))
    assert cache.find(h) != ""
    await v.delete("/rc/a")
    assert ix.content(h) is not None and cache.find(h) != "", "还有人用就不回收"
    await v.delete("/rc/b")
    assert ix.content(h) is None, "最后一个引用没了才回收内容"
    assert cache.find(h) == "", "本地缓存也要删"
    assert v._graveyard, "分片消息要进回收站"

    # 覆盖成同一份内容时不能误删
    h2 = await _mkfile(v, "/rc/c", b"same")
    await v.copy("/rc/c", "/rc/d")
    await v.delete("/rc/d")
    assert ix.content(h2) is not None


def _dupfile(data: bytes) -> str:
    p = tempfile.mktemp()
    with open(p, "wb") as f:
        f.write(data)
    return p


async def t_vfs_iter(ctx):
    v = ctx.vfs
    data = rand(70000)
    h = await _mkfile(v, "/it/a", data)
    cache.drop(h)
    out = b""
    async for c in v.iter_content("/it/a"):
        out += c
    assert out == data, "没缓存时要回 TG 取"
    assert cache.find(h) != "", "整份读完要落缓存"
    out = b""
    async for c in v.iter_content("/it/a"):
        out += c
    assert out == data

    # Range 不落缓存
    data_b = rand(5000)
    h2 = await _mkfile(v, "/it/b", data_b)
    await v.upload_pending("/it/b")
    cache.drop(h2)
    out = b""
    async for c in v.iter_content("/it/b", offset=10, length=100):
        out += c
    assert out == data_b[10:110]
    assert cache.find(h2) == "", "Range 读取不该落整份缓存"

    for off, ln in ((0, 0), (5000, 0), (5001, 10), (4999, 10)):
        out = b""
        async for c in v.iter_content("/it/b", offset=off, length=ln):
            out += c
        assert out == b"" or len(out) == min(ln, 5000 - off), (off, ln, len(out))

    # 空文件
    await _mkfile(v, "/it/empty", b"")
    out = b""
    async for c in v.iter_content("/it/empty"):
        out += c
    assert out == b""
    for bad in ("/it/nope", "/it"):
        try:
            async for _c in v.iter_content(bad):
                pass
            raise AssertionError(f"{bad} 应该抛")
        except FileNotFoundError:
            pass

    # 同一份内容并发读：只回 TG 下载一次
    h3 = await _mkfile(v, "/it/c", rand(9000))
    await v.upload_pending("/it/c")
    cache.drop(h3)
    ctx.client.downloads = 0
    got = []

    async def reader():
        b = b""
        async for c in v.iter_content("/it/c"):
            b += c
        got.append(b)

    await asyncio.gather(reader(), reader(), reader())
    assert len(got) == 3 and all(len(x) == 9000 for x in got)
    assert got[0] == got[1] == got[2]
    assert ctx.client.downloads == 1, f"单飞失败，下载了 {ctx.client.downloads} 次"


async def await_read(v, path):
    out = b""
    async for c in v.iter_content(path):
        out += c
    return out


async def t_vfs_finish(ctx):
    v, ix = ctx.vfs, ctx.index
    data = rand(3000)
    h = await _mkfile(v, "/fp/a", data, mtime=123)
    e = v.get("/fp/a")
    assert e.size == len(data) and e.mtime == 123 and not e.pending
    # 同一份内容再写一遍：秒传，直接 done，不产生新消息
    n_msgs = len(ctx.client._msgs)
    tmp = v.staging_tmp()
    with open(tmp, "wb") as f:
        f.write(data)
    e2 = v.finish_put("/fp/b", tmp, h, len(data), 5)
    assert not e2.pending, "内容已经在 TG 上，新路径直接完成"
    assert len(ctx.client._msgs) == n_msgs
    assert not os.path.exists(tmp), "秒传路径要把临时文件清掉"
    # 空文件
    e3 = await _mkfile(v, "/fp/empty", b"", upload=False)
    assert v.get("/fp/empty").pending
    await v.upload_pending("/fp/empty")
    e4 = v.get("/fp/empty")
    assert not e4.pending and e4.size == 0
    # 父目录自动建
    await _mkfile(v, "/fp/x/y/z", b"deep")
    assert v.get("/fp/x/y").is_dir
    # 覆盖已有路径：旧内容回收
    old = v.get("/fp/a").hash
    await _mkfile(v, "/fp/a", b"brand new content")
    assert v.get("/fp/a").hash != old
    assert ix.content(old) is not None, "还有 /fp/b 引用着，不能删"
    await v.delete("/fp/b")
    assert ix.content(old) is None


async def t_vfs_pending(ctx):
    v, ix = ctx.vfs, ctx.index
    h = await _mkfile(v, "/pd/a", b"pending bytes", upload=False)
    pend = [p for p, _h, _m in v.pending()]
    assert "/pd/a" in pend
    src = cache.staging_path(h)
    assert os.path.exists(src)
    await v.upload_pending("/pd/a")
    assert not v.get("/pd/a").pending
    assert not os.path.exists(src), "上完要挪出 staging"
    assert cache.find(h) != ""
    # 暂存丢了：记录要清掉，不能留一条读不出字节的路径
    h2 = await _mkfile(v, "/pd/b", b"will be lost", upload=False)
    os.unlink(cache.staging_path(h2))
    assert await v.upload_pending("/pd/b") is None
    assert v.get("/pd/b") is None
    assert ix.content(h2) is None
    # 对已完成/不存在/目录的调用是空操作
    assert v.get("/pd/a") is not None
    await v.upload_pending("/pd/a")
    assert await v.upload_pending("/pd/nope") in (None, v.index.get("/pd/a"))


async def t_vfs_teleport(ctx):
    v, ix = ctx.vfs, ctx.index
    data = b"teleport payload"
    h = await _mkfile(v, "/tp0/a", data)
    n = len(data)

    e, err = v.teleport("/tp0/b", h, 7)
    assert err is None and e.hash == h and not e.pending
    assert ix.referenced([h])[h] == 2

    _, err = v.teleport("/tp0/c", "f" * 32, 7)
    assert err == "missing"
    _, err = v.teleport("/tp0/c", h, 7, n + 1)
    assert err == "size"
    _, err = v.teleport("/tp0/c", h, 7, n)
    assert err is None
    _, err = v.teleport("/", h, 7)
    assert err == "dir"
    _, err = v.teleport("/tp0/a", h, 7)
    assert v.get("/tp0/a").hash == h

    # 只在暂存里、还没上 TG 的内容
    h2 = await _mkfile(v, "/tp0/p0", b"local only", upload=False)
    e, err = v.teleport("/tp0/p1", h2, 7)
    assert err is None and e.pending, "还没上 TG 的内容，新路径也是上传中"

    # 有 content 行但字节哪都没有
    ix.content_put("e" * 32, 10, {})
    _, err = v.teleport("/tp0/z", "e" * 32, 7)
    assert err == "unreadable"


async def t_vfs_flusher(ctx):
    v = ctx.vfs
    h = await _mkfile(v, "/fl/a", b"flush me")
    msg_ids = list(v.get("/fl/a").parts.values())
    assert msg_ids
    await v.delete("/fl/a")
    assert msg_ids[0] in v._graveyard, "要进回收站"
    for _ in range(50):
        if not v._graveyard:
            break
        await asyncio.sleep(0.01)
    assert not v._graveyard, "回收站要被清空"
    assert msg_ids[0] not in ctx.client._msgs, "TG 消息要真的删掉"


async def t_dav_options(ctx):
    cli = await make_dav(ctx)
    try:
        r = await cli.request("OPTIONS", "/")
        assert r.status == 200
        assert r.headers["DAV"] == "1"
        assert r.headers["MS-Author-Via"] == "DAV"
        allow = r.headers["Allow"]
        for m in ("OPTIONS", "PROPFIND", "GET", "PUT", "DELETE", "MKCOL", "MOVE", "COPY"):
            assert m in allow, (m, allow)
        assert "LOCK" not in allow
        for method in ("LOCK", "UNLOCK", "PATCH", "REPORT", "ACL", "TRACE", "POST"):
            r = await cli.request(method, "/")
            assert r.status == 405, (method, r.status)
            assert "Allow" in r.headers
    finally:
        await cli.close()


async def t_dav_auth(ctx):
    import base64
    import webdav

    class Req:
        def __init__(self, h=None):
            self.headers = {"Authorization": h} if h else {}

    def b64(s):
        return "Basic " + base64.b64encode(s.encode()).decode()

    dav = webdav.DAV(ctx.vfs, None)
    assert dav.authorized(Req()), "没设密码就放行"
    old = {k: os.environ.get(k) for k in ("DAV_USER", "DAV_PASS")}
    try:
        os.environ["DAV_USER"] = "u"
        os.environ["DAV_PASS"] = "p@ss:word"
        dav = webdav.DAV(ctx.vfs, None)
        assert not dav.authorized(Req())
        assert dav.authorized(Req(b64("u:p@ss:word"))), "密码里的冒号要保留"
        assert dav.authorized(Req("basic " + b64("u:p@ss:word")[6:])), "scheme 大小写不敏感"
        assert not dav.authorized(Req(b64("u:wrong")))
        assert not dav.authorized(Req(b64("x:p@ss:word")))
        assert not dav.authorized(Req("Bearer token"))
        assert not dav.authorized(Req("Basic !!!not-base64!!!"))
        assert not dav.authorized(Req(b64("nocolon")))
        assert not dav.authorized(Req(b64("u:")))
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        dav2 = webdav.DAV(ctx.vfs, None)
        assert dav2.authorized(Req())


async def t_dav_propfind_edge(ctx):
    import webdav
    cli = await make_dav(ctx)
    try:
        assert (await cli.put("/pf/a", data=b"1")).status == 201
        assert (await cli.put("/pf/b", data=b"22")).status == 201
        await cli.request("MKCOL", "/pf/sub")
        r = await cli.request("PROPFIND", "/pf", headers={"Depth": "0"})
        txt = await r.text()
        assert r.status == 207
        assert txt.count("<D:response>") == 1, txt
        assert "/pf/" in txt and "/pf/a</D:href>" not in txt
        r = await cli.request("PROPFIND", "/pf", headers={"Depth": "1"})
        txt = await r.text()
        assert txt.count("<D:response>") == 4, txt
        assert all(x in txt for x in ("/pf/a", "/pf/b", "/pf/sub"))
        r = await cli.request("PROPFIND", "/pf/a", headers={"Depth": "1"})
        txt = await r.text()
        assert txt.count("<D:response>") == 1, "文件没有子项"
        for depth in ("infinity", "2", "abc", "-1", ""):
            r = await cli.request("PROPFIND", "/pf", headers={"Depth": depth})
            assert r.status in (207, 403), (depth, r.status)
            if depth in ("infinity", "2", "abc", "-1"):
                assert r.status == 403, (depth, r.status)
                assert r.headers.get("X-DAV-Error") == "propfind-finite-depth"
        r = await cli.request("PROPFIND", "/pf", headers={})
        assert r.status == 207, "没给 Depth 默认看一层"
        r = await cli.request("PROPFIND", "/nope")
        assert r.status == 404
        r = await cli.request("PROPFIND", "/")
        assert r.status == 207 and "/pf/" in await r.text()

        # XML 转义
        await cli.put("/pf/we&ird <\"name\">.txt", data=b"x")
        r = await cli.request("PROPFIND", "/pf", headers={"Depth": "1"})
        txt = await r.text()
        assert "we&amp;ird" in txt, txt
        assert "<\"name\">" not in txt
        # 中文与空格
        await cli.put("/pf/中文 名.txt", data=b"x")
        r = await cli.request("PROPFIND", "/pf", headers={"Depth": "1"})
        txt = await r.text()
        assert "中文" in txt or "%E4%B8%AD" in txt, txt[:400]
        r = await cli.get("/pf/中文 名.txt")
        assert r.status == 200 and await r.read() == b"x"

        old = webdav.PROPFIND_MAX
        webdav.PROPFIND_MAX = 1
        try:
            r = await cli.request("PROPFIND", "/pf", headers={"Depth": "1"})
            assert r.status == 507, r.status
        finally:
            webdav.PROPFIND_MAX = old
    finally:
        await cli.close()


async def t_dav_range_matrix(ctx):
    cli = await make_dav(ctx)
    try:
        data = bytes(range(256)) * 4
        assert (await cli.put("/rg/f", data=data)).status == 201
        n = len(data)

        def want(start, end):
            return data[start:end + 1]

        cases = [
            ("bytes=0-0", 206, 0, 0),
            ("bytes=0-9", 206, 0, 9),
            (f"bytes={n-1}-{n-1}", 206, n - 1, n - 1),
            (f"bytes=0-{n+100}", 206, 0, n - 1),
            (f"bytes={n-100}-", 206, n - 100, n - 1),
            ("bytes=-1", 206, n - 1, n - 1),
            ("bytes=-100", 206, n - 100, n - 1),
            ("bytes=-999999", 206, 0, n - 1),
            ("bytes = 0-1", 206, 0, 1),
            ("bytes= 0 - 1", 206, 0, 1),
        ]
        for spec, status, s, e in cases:
            r = await cli.get("/rg/f", headers={"Range": spec})
            assert r.status == status, (spec, r.status)
            assert await r.read() == want(s, e), spec
            assert r.headers["Content-Range"] == f"bytes {s}-{e}/{n}", r.headers
            assert r.headers["Content-Length"] == str(e - s + 1)
        for spec in (f"bytes={n}-", f"bytes={n}-{n+5}", "bytes=-0", "bytes=abc", "bytes=1-0"):
            r = await cli.get("/rg/f", headers={"Range": spec})
            assert r.status in (200, 416), (spec, r.status)
            if spec in (f"bytes={n}-", f"bytes={n}-{n+5}", "bytes=-0", "bytes=1-0"):
                assert r.status == 416, (spec, r.status)
                assert r.headers["Content-Range"] == f"bytes */{n}"
        for spec in ("bytes=", "items=0-1", "", "bytes 0-1", "bytes=0-1,5-6"):
            r = await cli.get("/rg/f", headers={"Range": spec})
            assert r.status == 200, (spec, r.status)
            assert await r.read() == data, spec
        # 空文件
        await cli.put("/rg/empty", data=b"")
        r = await cli.get("/rg/empty", headers={"Range": "bytes=0-1"})
        assert r.status == 200 and await r.read() == b""
        r = await cli.get("/rg/nope", headers={"Range": "bytes=0-1"})
        assert r.status == 404
        r = await cli.get("/rg")
        assert r.status == 405, "目录不能 GET"
        r = await cli.head("/rg/f", headers={"Range": "bytes=0-1"})
        assert r.status == 200 and r.headers["Content-Length"] == str(n), "HEAD 不看 Range"
        assert r.headers["Accept-Ranges"] == "bytes"
        assert r.headers["ETag"].startswith('"')
    finally:
        await cli.close()


async def t_dav_cond(ctx):
    cli = await make_dav(ctx)
    try:
        data = rand(500)
        assert (await cli.put("/cd/f", data=data)).status == 201
        etag = (await cli.head("/cd/f")).headers["ETag"]
        lm = (await cli.head("/cd/f")).headers["Last-Modified"]
        r = await cli.get("/cd/f", headers={"If-None-Match": etag})
        assert r.status == 304 and not await r.read()
        r = await cli.get("/cd/f", headers={"If-None-Match": '"other"'})
        assert r.status == 200
        r = await cli.get("/cd/f", headers={"If-None-Match": "*"})
        assert r.status == 304, "星号表示任何存在的资源都算命中"
        r = await cli.get("/cd/f", headers={"If-None-Match": "W/" + etag})
        assert r.status == 304, "弱比较"
        r = await cli.get("/cd/f", headers={"If-None-Match": '"x", ' + etag})
        assert r.status == 304, "列表里任意一个命中就算"
        r = await cli.get("/cd/f", headers={"If-None-Match": "W/\"x\", W/" + etag})
        assert r.status == 304
        r = await cli.get("/cd/f", headers={"If-Modified-Since": lm})
        assert r.status == 304
        r = await cli.get("/cd/f", headers={"If-Modified-Since": "Thu, 01 Jan 1970 00:00:00 GMT"})
        assert r.status == 200
        r = await cli.get("/cd/f", headers={"If-Modified-Since": "not a date"})
        assert r.status == 200
        r = await cli.get("/cd/f", headers={"Range": "bytes=0-9", "If-Range": etag})
        assert r.status == 206 and await r.read() == data[:10]
        r = await cli.get("/cd/f", headers={"Range": "bytes=0-9", "If-Range": '"stale"'})
        assert r.status == 200 and await r.read() == data
        r = await cli.get("/cd/f", headers={"Range": "bytes=0-9",
                                           "If-Range": "Thu, 01 Jan 1970 00:00:00 GMT"})
        assert r.status == 200, "校验器过期就当没给 Range"
        r = await cli.get("/cd/f", headers={"Range": "bytes=0-9", "If-Range": lm})
        assert r.status in (206, 200)
    finally:
        await cli.close()


async def t_dav_put_edge(ctx):
    cli = await make_dav(ctx)
    try:
        assert (await cli.request("MKCOL", "/pe")).status == 201
        assert (await cli.put("/pe/e0", data=b"")).status == 201
        r = await cli.put("/pe/e0", data=b"")
        assert r.status == 204, "空 body 覆盖是 204"
        r = await cli.put("/pe/e0/sub", data=b"x")
        assert r.status == 409, "文件底下不能挂文件"
        assert ctx.vfs.get("/pe/e0/sub") is None
        r = await cli.put("/pe/x", data=b"ok")
        assert r.status == 201
        r = await cli.put("/pe/a/b/c/d", data=b"deep")
        assert r.status == 201
        assert ctx.vfs.get("/pe/a/b/c").is_dir
        # 只读
        ro = await make_dav(ctx, readonly=True)
        try:
            for path in ("/pe/ro1", "/pe/x"):
                r = await ro.put(path, data=b"nope")
                assert r.status == 403, (path, r.status)
            assert (await ro.request("DELETE", "/pe/x")).status == 403
            assert (await ro.request("MKCOL", "/pe/ro2")).status == 403
            assert (await ro.request("MOVE", "/pe/x", headers={
                "Destination": "/pe/y"})).status == 403
            assert (await ro.request("PROPPATCH", "/pe/x")).status == 403
            assert (await ro.get("/pe/x")).status == 200, "只读照样能读"
        finally:
            await ro.close()
        # mtime 头
        for raw, want in (("1234567890", 1234567890), ("0", None), ("-5", None),
                          ("2147483648", None), ("abc", None), ("false", None),
                          ("1.9", 1), ("", None)):
            path = f"/pe/m{abs(hash(raw)) % 9999}"
            r = await cli.put(path, data=b"1", headers={"X-OC-Mtime": raw})
            assert r.status == 201, (raw, r.status)
            e = ctx.vfs.get(path)
            if want is not None:
                assert e.mtime == want, (raw, e.mtime)
            else:
                assert e.mtime > 0, (raw, "非法 mtime 应该退回当前时间")
        r = await cli.put("/pe/m2", data=b"1", headers={"X-Update-Mtime": "555"})
        assert r.status == 201 and ctx.vfs.get("/pe/m2").mtime == 555
        # 根是目录
        r = await cli.put("/", data=b"x")
        assert r.status == 409, r.status
    finally:
        await cli.close()


async def t_dav_delete_mkcol(ctx):
    cli = await make_dav(ctx)
    try:
        await cli.put("/dm/f", data=b"1")
        await cli.put("/dm/sub/g", data=b"2")
        r = await cli.request("DELETE", "/dm/sub")
        assert r.status == 204
        assert ctx.vfs.get("/dm/sub/g") is None
        r = await cli.request("DELETE", "/dm/sub")
        assert r.status == 404
        r = await cli.request("DELETE", "/")
        assert r.status == 403, "不能删根"
        r = await cli.request("DELETE", "/dm/f")
        assert r.status == 204

        r = await cli.request("MKCOL", "/dm/new")
        assert r.status == 201
        r = await cli.request("MKCOL", "/dm/new")
        assert r.status == 405, "已存在"
        r = await cli.request("MKCOL", "/dm/new/deep/more")
        assert r.status == 201, "父目录会自动补"
        assert ctx.vfs.get("/dm/new/deep").is_dir
        await cli.put("/dm/file", data=b"x")
        r = await cli.request("MKCOL", "/dm/file/sub")
        assert r.status == 409, "文件底下不能建目录"
        # DELETE 带 query / 尾部斜杠
        await cli.put("/dm/q", data=b"1")
        r = await cli.request("DELETE", "/dm/q?x=1")
        assert r.status == 204, r.status
    finally:
        await cli.close()


async def t_dav_move_copy_edge(ctx):
    cli = await make_dav(ctx)
    try:
        await cli.put("/mc/a", data=b"aaa")
        await cli.put("/mc/b", data=b"bbb")
        await cli.put("/mc/sub/c", data=b"ccc")
        url = str(cli.make_url("/mc/t1"))
        r = await cli.request("MOVE", "/mc/a")
        assert r.status == 400, "没有 Destination"
        r = await cli.request("MOVE", "/mc/a", headers={"Destination": url})
        assert r.status == 201
        assert ctx.vfs.get("/mc/t1") is not None
        r = await cli.request("MOVE", "/mc/t1", headers={"Destination": url})
        assert r.status in (204, 201)
        r = await cli.request("MOVE", "/mc/b", headers={"Destination": url,
                                                        "Overwrite": "F"})
        assert r.status == 412, "Overwrite: F 且目标已存在"
        r = await cli.request("MOVE", "/mc/b", headers={"Destination": url,
                                                        "Overwrite": "T"})
        assert r.status == 204
        r = await cli.request("MOVE", "/mc/nope", headers={"Destination": url})
        assert r.status == 404
        r = await cli.request("MOVE", "/mc/sub", headers={
            "Destination": str(cli.make_url("/mc/sub/x"))})
        assert r.status == 409, "目标在源子树里"
        r = await cli.request("MOVE", "/mc/sub/c", headers={
            "Destination": str(cli.make_url("/mc/sub"))})
        assert r.status == 409, "目标是源的祖先"
        await cli.request("MKCOL", "/mc/d1")
        r = await cli.request("MOVE", "/mc/t1", headers={
            "Destination": str(cli.make_url("/mc/d1"))})
        assert r.status == 409, "目标是目录"
        # 相对 Destination + 编码
        r = await cli.request("MOVE", "/mc/t1", headers={"Destination": "/mc/sp%20ace"})
        assert r.status == 201, r.status
        assert ctx.vfs.get("/mc/sp ace") is not None, "Destination 要 URL 解码"
        # COPY 同语义
        r = await cli.request("COPY", "/mc/sp ace", headers={"Destination": "/mc/copy1"})
        assert r.status == 201
        r = await cli.request("COPY", "/mc/copy1", headers={"Destination": "/mc/copy1"})
        assert r.status == 204, "自己复制到自己应该是空操作"
        r = await cli.request("COPY", "/mc/sp ace", headers={"Destination": "/mc/copy1"})
        assert r.status == 204
        assert ctx.vfs.get("/mc/sp ace").hash == ctx.vfs.get("/mc/copy1").hash
        # 目录复制
        r = await cli.request("COPY", "/mc/sub", headers={"Destination": "/mc/sub2"})
        assert r.status == 201
        assert ctx.vfs.get("/mc/sub2/c") is not None
        # Destination 是空串
        r = await cli.request("COPY", "/mc/sub", headers={"Destination": ""})
        assert r.status == 400
    finally:
        await cli.close()


async def t_dav_paths_edge(ctx):
    cli = await make_dav(ctx)
    try:
        assert (await cli.put("/pp/a.txt", data=b"1")).status == 201
        # 尾部斜杠 / 双斜杠 / 点段
        for p in ("/pp/a.txt/", "/pp//a.txt", "/pp/./a.txt", "/pp/x/../a.txt"):
            r = await cli.get(p)
            assert r.status == 200, (p, r.status)
        r = await cli.get("/pp/../pp/a.txt")
        assert r.status == 200
        # 根之外的 .. 不能穿出去
        r = await cli.get("/../../pp/a.txt")
        assert r.status == 200
        # 百分号编码的文件名
        r = await cli.put("/pp/with%20space.txt", data=b"2")
        assert r.status == 201, r.status
        assert ctx.vfs.get("/pp/with space.txt") is not None
        r = await cli.get("/pp/with%20space.txt")
        assert r.status == 200 and await r.read() == b"2"
        # 中文
        r = await cli.put("/pp/中文.txt", data=b"3")
        assert r.status == 201
        r = await cli.get("/pp/中文.txt")
        assert r.status == 200
        # 不存在的深路径
        r = await cli.get("/pp/no/such/file")
        assert r.status == 404
        # OPTIONS 对任意路径都行
        r = await cli.request("OPTIONS", "/pp/nope")
        assert r.status == 200
    finally:
        await cli.close()


async def t_dav_mixed(ctx):
    cli = await make_dav(ctx)
    try:
        await cli.put("/mx/a", data=b"12345")
        # PROPPATCH
        r = await cli.request("PROPPATCH", "/mx/a")
        assert r.status == 207
        assert b"multistatus" in await r.read()
        # 进度查询与秒传互不干扰
        h = (await cli.head("/mx/a")).headers["ETag"].strip('"').rsplit("-", 1)[0]
        r = await cli.put("/mx/b", data=b"", headers={
            "Content-Range": "bytes */5", "X-Content-Hash": h})
        assert r.status == 308, r.status
        assert ctx.vfs.get("/mx/b") is None, "进度查询不能建文件"
        # HEAD 不存在 / 目录
        assert (await cli.head("/mx/nope")).status == 404
        assert (await cli.head("/mx")).status == 405
        # PUT 后立刻 HEAD 拿到正确长度
        r = await cli.head("/mx/a")
        assert r.headers["Content-Length"] == "5"
        assert r.headers["ETag"].endswith('-5"')
        # 上传中的文件 PROPFIND 也能列出来
        assert (await cli.put("/mx/pend", data=b"zz")).status == 201
        r = await cli.request("PROPFIND", "/mx", headers={"Depth": "1"})
        txt = await r.text()
        assert "pend" in txt
    finally:
        await cli.close()


async def t_local_lock(ctx):
    import main as M
    path = tempfile.mktemp()
    a = M.LocalLock(path)
    assert await a.acquire(tries=1) is True
    assert a.check() is True
    b = M.LocalLock(path)
    assert await b.acquire(tries=1) is False, "同一个锁文件不能被两次持有"
    a.release()
    assert a.check() is False
    assert await b.acquire(tries=1) is True
    b.release()
    # 文件被换掉 → 锁失效
    c = M.LocalLock(path)
    assert await c.acquire(tries=1) is True
    os.replace(path, path + ".bak")
    assert c.check() is False, "inode 变了就是锁没了"
    c.release()
    os.replace(path + ".bak", path)
    d = M.LocalLock(path)
    assert await d.acquire(tries=1) is True
    os.unlink(path)
    assert d.check() is False, "文件被删也是锁没了"
    d.release()
    # 无法创建锁文件
    bad = M.LocalLock("/不存在/目录/x.lock")
    assert await bad.acquire(tries=1) is False


async def t_restart(ctx):
    dbp = tempfile.mktemp(suffix=".db")
    ix = IDX.Index(dbp)
    ix.content_put("r" * 32, 8, {0: 999})
    ix.put_file("/rs/a", "r" * 32, 7, IDX.STATUS_DONE)
    ix.put_dir("/rs", 3)
    ix.set_meta("dirty", "1")
    ix.close()
    ix2 = IDX.Index(dbp)
    e = ix2.get("/rs/a")
    assert e is not None and e.hash == "r" * 32 and e.mtime == 7
    assert ix2.get("/rs").is_dir
    assert ix2.meta("dirty") == "1", "重启后 dirty 还在，下次启动会补推快照"
    assert ix2.content("r" * 32) == (8, {0: 999})
    # 老库迁移不会在第二次打开时重跑（schema 已经是新的）
    assert "size" not in {r[1] for r in
                          __import__("sqlite3").connect(dbp).execute("PRAGMA table_info(files)")}


async def t_integration_slices(ctx):
    old = tg.SLICE
    tg.SLICE = 1000
    try:
        v = ctx.vfs
        data = rand(4800)
        h = await _mkfile(v, "/big/f", data)
        e = v.get("/big/f")
        assert e.size == len(data) and sorted(e.parts) == [0, 1, 2, 3, 4], e.parts
        assert await await_read(v, "/big/f") == data
        cache.drop(h)
        out = b""
        async for c in v.iter_content("/big/f", offset=1500, length=2000):
            out += c
        assert out == data[1500:3500]
        # 索引丢了，reindex 要能把 size 和分片恢复出来
        import sqlite3
        raw = sqlite3.connect(ctx.index.path)
        raw.execute("DELETE FROM content WHERE hash=?", (h,))
        raw.commit()
        raw.close()
        await ctx.store.reindex()
        got = ctx.index.content(h)
        assert got is not None and got[0] == len(data), got
        assert sorted(got[1]) == [0, 1, 2, 3, 4], got
        cache.drop(h)
        assert await await_read(v, "/big/f") == data, "重建之后还能读回原字节"
    finally:
        tg.SLICE = old


async def t_concurrent_same(ctx):
    import main as M
    v = ctx.vfs
    datas = [rand(2000 + i) for i in range(6)]
    up = M.Uploader(v, 2)
    up.start()
    try:
        async def put(i):
            tmp = v.staging_tmp()
            with open(tmp, "wb") as f:
                f.write(datas[i])
            h = tg.digest_of(tmp)
            v.finish_put("/same/x", tmp, h, len(datas[i]), 1)
            up.nudge()

        await asyncio.gather(*[put(i) for i in range(6)])
        for _ in range(200):
            e = v.get("/same/x")
            if e and not e.pending:
                break
            await asyncio.sleep(0.02)
        e = v.get("/same/x")
        assert e is not None and not e.pending
        out = await await_read(v, "/same/x")
        assert out in datas, "最终内容必须是某一次完整写入的字节"
    finally:
        await up.stop()


async def t_race_delete_pending(ctx):
    v = ctx.vfs
    h = await _mkfile(v, "/rz/a", b"x" * 1000, upload=False)
    await v.delete("/rz/a")
    assert ctx.index.content(h) is None
    assert not os.path.exists(cache.staging_path(h)), "暂存也要跟着删"
    assert await v.upload_pending("/rz/a") is None
    assert v.get("/rz/a") is None


async def t_dav_stream_limit(ctx):
    import webdav
    cli = await make_dav(ctx)
    try:
        await cli.put("/sl/big", data=rand(5000))
        old = webdav.MAX_STREAMS
        old_n = webdav._active_streams
        webdav.MAX_STREAMS = 1
        webdav._active_streams = 1
        try:
            r = await cli.get("/sl/big")
            assert r.status == 503, r.status
            assert r.headers.get("Retry-After") == "2"
        finally:
            webdav.MAX_STREAMS = old
            webdav._active_streams = old_n
        r = await cli.get("/sl/big")
        assert r.status == 200, "放开之后要能读"
    finally:
        await cli.close()


async def t_dav_many(ctx):
    cli = await make_dav(ctx)
    try:
        for i in range(60):
            assert (await cli.put(f"/many/f{i:03d}", data=b"x")).status == 201
        r = await cli.request("PROPFIND", "/many", headers={"Depth": "1"})
        txt = await r.text()
        assert txt.count("<D:response>") == 61, txt.count("<D:response>")
        r = await cli.request("PROPFIND", "/many", headers={"Depth": "0"})
        assert (await r.text()).count("<D:response>") == 1
    finally:
        await cli.close()


async def t_index_bulk(ctx):
    db = IDX.Index(tempfile.mktemp(suffix=".db"))
    for d in range(15):
        db.put_dir(f"/bulk/{d}", 1)
    for i in range(1500):
        db.put_file(f"/bulk/{i // 100}/{i:04d}", "%032x" % i, i)
    assert len(db.all_files()) == 1500
    kids = db.children("/bulk")
    assert len(kids) == 15
    assert all(k.is_dir for k in kids)
    assert len(db.children("/bulk/0")) == 100
    ref = db.referenced(["%032x" % i for i in range(0, 1500, 2)])
    assert len(ref) == 750
    d = tempfile.mktemp(suffix=".db")
    db.dump(d)
    db2 = IDX.Index(tempfile.mktemp(suffix=".db"))
    db2.restore(d)
    assert len(db2.all_files()) == 1500


async def t_dav_helpers(ctx):
    import webdav as W
    assert W.etag_of(None) == ""
    assert W.etag_of(IDX.Entry("/a", True)) == ""
    e = IDX.Entry("/a", False, "h" * 32, 10)
    assert W.etag_of(e) == '"%s-10"' % ("h" * 32)
    assert W.http_date(0).startswith("Thu, 01 Jan 1970")
    assert W.http_date("不是数字").startswith(("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"))
    assert W._ts("Thu, 01 Jan 1970 00:00:00 GMT") == 0
    assert W._ts("garbage") is None
    assert W._ts(None) is None
    assert W.mime_of("a.txt") == "text/plain"
    assert W.mime_of("a.weird-ext") == "application/octet-stream"
    assert W.mime_of("") == "application/octet-stream"
    assert W.parse_range("bytes=0-1", 10) == (0, 1)
    assert W.parse_range("bytes=-5", 10) == (5, 9)
    assert W.parse_range("bytes=5-", 10) == (5, 9)
    assert W.parse_range("bytes=9-100", 10) == (9, 9)
    assert W.parse_range("", 10) is None
    assert W.parse_range("bytes=0-1", 0) is None
    assert W.parse_range("bytes=10-", 10) == "unsat"
    assert W.parse_range("bytes=-0", 10) == "unsat"
    assert W.parse_content_range("bytes 0-9/10") == (0, 9, 10)
    assert W.parse_content_range("bytes 0-9/9") is None
    assert W.parse_content_range("bytes 5-1/10") is None
    assert W.parse_content_range("bytes 0-9") is None
    assert W.parse_content_range("bytes 0-/10") is None
    assert W.parse_content_range("") is None
    assert W.parse_progress_query("bytes */10") == 10
    assert W.parse_progress_query("bytes */*") is None
    assert W.parse_progress_query("bytes */") is False
    assert W.parse_progress_query("bytes 0-1/10") is False
    assert W.parse_progress_query("") is False
    assert W.href_for("/a b", False) == "/a%20b"
    assert W.href_for("/d", True) == "/d/"
    assert W.href_for("/d/", True) == "/d/"
    assert W.href_for("/", True) == "/"
    assert W.href_for("/中文", False).startswith("/%E4")
    assert W.xesc('a<b>&"') == "a&lt;b&gt;&amp;&quot;"
    assert W.xesc("普通") == "普通"


async def t_misc(ctx):
    db = IDX.Index(tempfile.mktemp(suffix=".db"))
    db.put_file("/mh/a", "h" * 32, 1)
    assert db.hash_of("/mh/a") == "h" * 32
    assert db.hash_of("/mh/nope") is None
    assert db.hash_of("/mh/../mh/a") == "h" * 32

    assert cache.blob_path("") == "" and cache.staging_path("") == ""
    p = _dupfile(b"12345")
    cache.put("touching", p)
    target = cache.blob_path("touching")
    os.utime(target, (1, 1))
    cache.touch(target)
    assert os.path.getmtime(target) > 1, "touch 要把 mtime 拉到现在（LRU 用）"
    cache.touch("/不存在/文件")
    cache.drop("touching")

    v = __import__("vfs").VFS(IDX.Index(tempfile.mktemp(suffix=".db")), ctx.store)
    assert v.exists("/") and not v.exists("/nope")
    assert v.get("/nope") is None and v.children("/nope") == []
    v.mark_dirty()
    assert v.dirty and v.index.meta("dirty") == "1"
    assert v.tg.dirty is True


async def t_keeper(ctx):
    import main as M
    import vfs as V
    cli = FakeClient()
    db = IDX.Index(tempfile.mktemp(suffix=".db"))
    st = tg.TGStore(cli, 1, db, cache)
    v = V.VFS(db, st)
    await st.push_snapshot(caption=tg.LOCK_IDLE)
    assert await st.acquire(timeout=5)
    old = M.SNAPSHOT_SEC
    old_lock = M.LOCK_CHECK_SEC
    M.SNAPSHOT_SEC = 0
    M.LOCK_CHECK_SEC = 0.1
    k = M.Keeper(st, v)
    k.start()
    try:
        db.put_file("/kp/a", "k" * 32, 1)
        db.content_put("k" * 32, 1, {})
        st.dirty = True
        st._last_write = time.time() - 10
        for _ in range(100):
            await asyncio.sleep(0.05)
            if not st.dirty:
                break
        assert not st.dirty, "空闲够久要自动推快照"
        assert db.meta("snap_msg_id"), "推完要记住快照消息 id"
        assert not k.stop.is_set(), "锁还是自己的，不该停"
        m = await st.snap_msg()
        await cli.edit_message(1, m.id, text="lock:someone-else")
        for _ in range(100):
            await asyncio.sleep(0.05)
            if k.stop.is_set():
                break
        assert k.stop.is_set(), "锁被别人拿走必须停"
    finally:
        M.SNAPSHOT_SEC = old
        M.LOCK_CHECK_SEC = old_lock
        await k.close()


async def t_logger(ctx):
    import subprocess
    d = tempfile.mkdtemp()
    ring = os.path.join(d, "ring.log")
    app = os.path.join(d, "app.log")
    here = os.path.dirname(os.path.abspath(__file__))
    script = (
        "import sys, logging\n"
        f"sys.path.insert(0, {here!r})\n"
        "import logger\n"
        f"logger.configure(log_file={ring!r}, log_max=512, append_file={app!r})\n"
        "logger.log('line-1')\n"
        "logger.log('line-2')\n"
        "logger.progress('进度 50%')\n"
        "logging.getLogger('x').warning('来自 logging')\n"
        f"sys.stdout.write(logger.read_log({ring!r}))\n"
    )
    out = subprocess.run([sys.executable, "-c", script],
                         capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr
    txt = out.stdout
    for want in ("line-1", "line-2", "进度 50%", "来自 logging"):
        assert want in txt, (want, txt)
    assert os.path.getsize(ring) == 16 + 512, os.path.getsize(ring)
    assert os.path.exists(app)
    with open(app, encoding="utf-8") as f:
        ap = f.read()
    assert "line-1" in ap and "来自 logging" in ap, ap
    assert "进度" not in ap, "追加日志不收进度"

    dump = os.path.join(d, "dump.txt")
    script2 = (
        "import sys\n"
        f"sys.path.insert(0, {here!r})\n"
        "import logger\n"
        f"logger.configure(log_file={ring!r}, log_max=512)\n"
        "for i in range(200):\n"
        "    logger.log('Y' * 40 + str(i))\n"
        f"open({dump!r}, 'w').write(logger.read_log({ring!r}))\n"
    )
    out2 = subprocess.run([sys.executable, "-c", script2],
                          capture_output=True, text=True, timeout=120)
    assert out2.returncode == 0, out2.stderr
    with open(dump, encoding="utf-8") as f:
        tail = f.read()
    assert len(tail) <= 512, len(tail)
    assert "199" in tail, "最新的日志必须还在"
    assert "Y" * 40 + "0" not in tail, "最旧的应该被覆盖掉"
    assert os.path.getsize(ring) == 16 + 512


async def t_index_like(ctx):
    """路径里的 % 和 _ 不能被当成 LIKE 通配符，否则会连带匹配别的目录。"""
    db = IDX.Index(tempfile.mktemp(suffix=".db"))
    db.put_dir("/a%b", 1)
    db.put_file("/a%b/x", "h1", 1)
    db.put_dir("/aXb", 1)
    db.put_file("/aXb/y", "h2", 1)
    assert [k.path for k in db.children("/a%b")] == ["/a%b/x"]
    assert db.list_under("/a%b") == [("/a%b/x", "h1", False), ("/a%b", "", True)]
    db.put_dir("/c_d", 1)
    db.put_file("/c_d/z", "h3", 1)
    db.put_dir("/cXd", 1)
    db.put_file("/cXd/w", "h4", 1)
    assert [k.path for k in db.children("/c_d")] == ["/c_d/z"]
    taken = db.take_under("/c_d")
    assert ("/c_d/z", "h3", False) in taken
    assert all(p.startswith("/c_d") for p, _h, _d in taken), taken
    assert db.get("/cXd/w") is not None, "隔壁目录不能被一起摘掉"
    db.put_dir("/e\\f", 1)
    db.put_file("/e\\f/g", "h5", 1)
    assert [k.path for k in db.children("/e\\f")] == ["/e\\f/g"]
    assert IDX.like_esc("a%b_c\\d").count("\\") == 4, IDX.like_esc("a%b_c\\d")


async def t_vfs_special_dirs(ctx):
    """特殊字符目录的移动/删除不能波及邻居。"""
    v = ctx.vfs
    h1 = await _mkfile(v, "/sp%a/f", b"one")
    h2 = await _mkfile(v, "/spXa/g", b"two")
    await v.move("/sp%a", "/sp%a2")
    assert v.get("/sp%a2/f").hash == h1
    assert v.get("/spXa/g").hash == h2, "邻居必须原样不动"
    await v.delete("/sp%a2")
    assert v.get("/sp%a2/f") is None
    assert v.get("/spXa/g") is not None
    await v.copy("/spXa", "/sp_c")
    await _mkfile(v, "/sp_c/own", b"three")
    await v.delete("/sp_c")
    assert v.get("/sp_c/own") is None
    assert v.get("/spXa/g") is not None
    await v.delete("/spXa")


async def t_resume_state(ctx):
    import webdav
    dav = webdav.DAV(ctx.vfs, None)
    data = rand(4000)
    r = await dav.put(_req("/rsv/x", [data[:1000]], cr="bytes 0-999/%d" % len(data)))
    assert r.status == 308
    st = dav._resume["/rsv/x"]
    old_tmp = st["tmp"]
    assert os.path.exists(old_tmp)
    r = await dav.put(_req("/rsv/x", [data[:1000]], cr="bytes 0-999/%d" % len(data)))
    assert r.status == 308
    assert dav._resume["/rsv/x"]["tmp"] != old_tmp
    assert not os.path.exists(old_tmp), "重置状态时旧临时文件要删"
    dav._resume["/rsv/x"]["ts"] = time.time() - 10 ** 6
    tmp = dav._resume["/rsv/x"]["tmp"]
    r = await dav.put(_req("/rsv/z", [b"x" * 10], cr="bytes 0-9/20"))
    assert r.status == 308
    assert "/rsv/x" not in dav._resume
    assert not os.path.exists(tmp)


async def t_dav_507(ctx):
    cli = await make_dav(ctx)
    old = cache.ensure
    cache.ensure = lambda n: False
    try:
        r = await cli.put("/q507/a", data=b"x" * 1000)
        assert r.status == 507, r.status
        assert ctx.vfs.get("/q507/a") is None, "被拒绝的 PUT 不能留记录"
        r = await cli.put("/q507/a", data=b"")
        assert r.status == 201, "0 字节不占预算，照常放行"
    finally:
        cache.ensure = old
        await cli.close()


async def t_dav_401(ctx):
    old = {k: os.environ.get(k) for k in ("DAV_USER", "DAV_PASS")}
    os.environ["DAV_USER"] = "u"
    os.environ["DAV_PASS"] = "p"
    cli = None
    try:
        cli = await make_dav(ctx)
        r = await cli.get("/")
        assert r.status == 401
        assert 'Basic realm="WebDAV"' in r.headers["WWW-Authenticate"]
        r = await cli.request("OPTIONS", "/")
        assert r.status == 401, "认证在方法分发之前"
        import base64
        auth = "Basic " + base64.b64encode(b"u:p").decode()
        r = await cli.get("/", headers={"Authorization": auth})
        assert r.status != 401, r.status
    finally:
        if cli is not None:
            await cli.close()
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


async def t_dav_bad_path(ctx):
    cli = await make_dav(ctx)
    try:
        r = await cli.get("/bad%01name")
        assert r.status == 400, r.status
        r = await cli.put("/bad%00name", data=b"x")
        assert r.status == 400, r.status
        await cli.put("/bp/src", data=b"1")
        r = await cli.request("MOVE", "/bp/src", headers={"Destination": "/dest%01x"})
        assert r.status == 400, r.status
    finally:
        await cli.close()


async def t_snapshot_final(ctx):
    cli = FakeClient()
    db = IDX.Index(tempfile.mktemp(suffix=".db"))
    st = tg.TGStore(cli, 1, db, cache)
    await st.push_snapshot(caption=tg.LOCK_IDLE)
    assert await st.acquire(timeout=5)
    db.put_file("/fin/a", "f" * 32, 1)
    st.dirty = True
    await st.push_snapshot(final=True)
    m = await st.snap_msg()
    assert m.message.strip() == tg.LOCK_IDLE, m.message
    assert st.holding is False, "final 要顺手把锁放掉"
    assert st.dirty is False and db.meta("dirty") == "0"


async def t_pull_merge_dirs(ctx):
    cli = FakeClient()
    db = IDX.Index(tempfile.mktemp(suffix=".db"))
    st = tg.TGStore(cli, 1, db, cache)
    db.put_dir("/mm/remote", 1)
    await st.push_snapshot(caption=tg.LOCK_IDLE)
    db2 = IDX.Index(tempfile.mktemp(suffix=".db"))
    db2.put_dir("/mm", 2)
    db2.put_dir("/mm/localdir", 2)
    st2 = tg.TGStore(cli, 1, db2, cache)
    assert await st2.pull_snapshot(merge=True)
    assert db2.get("/mm/localdir") is not None, "本地独有的目录也要补回"
    assert db2.get("/mm/remote") is not None


async def t_move_pending(ctx):
    v = ctx.vfs
    h = await _mkfile(v, "/mp/d/f", b"pending payload", upload=False)
    await v.move("/mp/d", "/mp/e")
    e = v.get("/mp/e/f")
    assert e is not None and e.pending and e.hash == h
    await v.upload_pending("/mp/e/f")
    assert not v.get("/mp/e/f").pending
    assert await await_read(v, "/mp/e/f") == b"pending payload"
    h2 = await _mkfile(v, "/mp/g/f", b"never uploaded", upload=False)
    assert os.path.exists(cache.staging_path(h2))
    await v.delete("/mp/g")
    assert not os.path.exists(cache.staging_path(h2))
    assert ctx.index.content(h2) is None


async def t_fuzz_norm(ctx):
    import random
    import unicodedata
    rnd = random.Random(20261114)
    alphabet = "/.\\a b中%_🎉\u0301\u0000\n"
    for _ in range(3000):
        raw = "".join(rnd.choice(alphabet) for _ in range(rnd.randrange(0, 24)))
        n = paths.norm(raw)
        assert n.startswith("/")
        assert n == "/" or not n.endswith("/")
        assert "//" not in n
        assert paths.norm(n) == n, raw
        assert unicodedata.is_normalized("NFC", n)
        paths.bad(raw)
        paths.base(raw)
        paths.parent(raw)
        paths.under(n, "/")


async def t_fuzz_parse(ctx):
    import random
    import webdav
    rnd = random.Random(777)
    alphabet = "bytes  =-/*0123456789,Xx"
    for _ in range(2000):
        raw = "".join(rnd.choice(alphabet) for _ in range(rnd.randrange(0, 30)))
        tg.parse_slice_name(raw)
        webdav.parse_range(raw, 1000)
        webdav.parse_content_range(raw)
        webdav.parse_progress_query(raw)
        webdav.xesc(raw)
        webdav.href_for(raw, bool(rnd.getrandbits(1)))
    for raw in ("bytes=0-1", "bytes=-", "bytes=-1-2", "bytes=999999999999999999999-"):
        webdav.parse_range(raw, 10)
    assert webdav.parse_range("bytes=999999999999999999999-", 10) == "unsat"


async def t_cache_lru(ctx):
    old_dir = cache.CACHE_DIR
    old_env = os.environ.get("CACHE_MAX_GB")
    cache.CACHE_DIR = tempfile.mkdtemp(prefix="tgnd_lru_")
    try:
        os.environ["CACHE_MAX_GB"] = "0.0000003"
        assert cache.limit() >= 300, cache.limit()
        for i, name in enumerate(("a", "b", "c")):
            cache.put(name, _dupfile(b"x" * 100))
            os.utime(cache.blob_path(name), (10 + i, 10 + i))
        assert cache.usage()[0] == 300
        cache.touch(cache.blob_path("a"))
        os.environ["CACHE_MAX_GB"] = "0.0000002"
        assert cache.trim() >= 100
        assert not os.path.exists(cache.blob_path("b")), "a 刚被 touch 过，b 才是最旧的"
        assert os.path.exists(cache.blob_path("a")) and os.path.exists(cache.blob_path("c"))
        os.environ["CACHE_MAX_GB"] = "0"
        assert cache.trim() == 0, "不限制时不淘汰"
        assert cache.usage()[0] > 0
    finally:
        cache.CACHE_DIR = old_dir
        if old_env is None:
            os.environ.pop("CACHE_MAX_GB", None)
        else:
            os.environ["CACHE_MAX_GB"] = old_env


async def t_children_depth(ctx):
    db = IDX.Index(tempfile.mktemp(suffix=".db"))
    db.put_dir("/lvl", 1)
    db.put_file("/lvl/a", "h1", 1)
    db.put_dir("/lvl/d", 1)
    db.put_file("/lvl/d/b", "h2", 1)
    db.put_dir("/lvl/d/e", 1)
    db.put_file("/lvl/d/e/c", "h3", 1)
    assert [k.path for k in db.children("/lvl")] == ["/lvl/a", "/lvl/d"]
    assert [k.path for k in db.children("/lvl/d")] == ["/lvl/d/b", "/lvl/d/e"]
    assert [k.path for k in db.children("/lvl/d/e")] == ["/lvl/d/e/c"]
    # 前缀相似的兄弟目录不能串
    db.put_dir("/lv2", 1)
    db.put_file("/lv2/z", "h9", 1)
    assert [k.path for k in db.children("/lvl")] == ["/lvl/a", "/lvl/d"]


async def t_reindex_dup(ctx):
    from types import SimpleNamespace as NS
    cli = FakeClient()
    db = IDX.Index(tempfile.mktemp(suffix=".db"))
    st = tg.TGStore(cli, 1, db, cache)
    data = rand(2000)
    p = tempfile.mktemp()
    with open(p, "wb") as f:
        f.write(data)
    h = tg.digest_of(p)
    await cli.send_file(1, p, attributes=[NS(file_name=f"{h}.0")])
    m2 = await cli.send_file(1, p, attributes=[NS(file_name=f"{h}.0")])
    await st.reindex()
    got = db.content(h)
    # 消息是新的在前扫的，同序号重复时保留最新那条
    assert got is not None and got[1] == {0: m2.id}, got


async def t_read_all_partial(ctx):
    old = tg.SLICE
    tg.SLICE = 1000
    try:
        cli = FakeClient()
        db = IDX.Index(tempfile.mktemp(suffix=".db"))
        st = tg.TGStore(cli, 1, db, cache)
        data = rand(3000)
        p = tempfile.mktemp()
        with open(p, "wb") as f:
            f.write(data)
        h = tg.digest_of(p)
        parts = await st.upload(h, p, len(data))
        assert len(parts) == 3
        cache.drop(h)
        await cli.delete_messages(1, [parts[1]])
        out = b"".join([c async for c in st.read_all(h, parts, len(data))])
        assert out == data[:1000], "缺片只该给到断点为止"
        assert cache.find(h) == "", "不完整的内容绝不能进缓存"
        assert not os.path.exists(os.path.join(cache.staging_dir(), ".dl-" + h))
    finally:
        tg.SLICE = old


async def t_vfs_range_multislice(ctx):
    old = tg.SLICE
    tg.SLICE = 1000
    try:
        v = ctx.vfs
        data = rand(3500)
        h = await _mkfile(v, "/ms/f", data)
        cache.drop(h)
        for off, ln in ((0, 10), (990, 20), (1000, 1), (1500, 1500), (3499, 1)):
            out = b""
            async for c in v.iter_content("/ms/f", offset=off, length=ln):
                out += c
            assert out == data[off:off + ln], (off, ln)
        assert cache.find(h) == "", "Range 读取即使跨片也不落缓存"
    finally:
        tg.SLICE = old


async def t_propfind_xml(ctx):
    cli = await make_dav(ctx)
    try:
        await cli.put("/xml/a.txt", data=b"12345")
        await cli.request("MKCOL", "/xml/d")
        r = await cli.request("PROPFIND", "/xml", headers={"Depth": "1"})
        txt = await r.text()
        assert r.headers["Content-Type"].startswith("application/xml")
        assert txt.startswith("<?xml")
        assert "<D:multistatus" in txt and txt.rstrip().endswith("</D:multistatus>")
        for tag in ("<D:response>", "<D:href>", "<D:propstat>", "<D:prop>",
                    "<D:displayname>", "<D:resourcetype>", "<D:getcontentlength>",
                    "<D:getcontenttype>", "<D:getlastmodified>", "<D:getetag>",
                    "<D:creationdate>", "<D:status>HTTP/1.1 200 OK</D:status>"):
            assert tag in txt, tag
        assert "<D:collection/>" in txt, "目录要有 collection 标记"
        assert "/xml/a.txt</D:href>" in txt
        assert "/xml/d/</D:href>" in txt, "目录 href 带尾斜杠"
        assert "text/plain" in txt
        assert "httpd/unix-directory" in txt
        assert "<D:getcontentlength>5</D:getcontentlength>" in txt
        # 文件的 resourcetype 是空的
        r = await cli.request("PROPFIND", "/xml/a.txt", headers={"Depth": "0"})
        txt = await r.text()
        assert "<D:resourcetype/>" in txt
        assert "<D:collection/>" not in txt
        assert "text/plain" in txt
    finally:
        await cli.close()


async def t_snap_fallback(ctx):
    cli = FakeClient()
    db = IDX.Index(tempfile.mktemp(suffix=".db"))
    st = tg.TGStore(cli, 1, db, cache)
    await st.push_snapshot(caption=tg.LOCK_IDLE)
    good = st.snap_msg_id
    assert good
    st.snap_msg_id = 999999                      # 记下的 id 已经不存在了
    m = await st.snap_msg()
    assert m is not None and m.id == good, "要回退到置顶列表里找"
    # 连置顶都没了
    await cli.delete_messages(1, [good])
    assert await st.snap_msg() is None
    assert await st.check_lock() == "gone"


async def t_push_fallback(ctx):
    class EditFail(FakeClient):
        def __init__(self):
            super().__init__()
            self.fail = 1

        async def edit_message(self, *a, **k):
            if self.fail:
                self.fail -= 1
                raise RuntimeError("edit failed")
            return await super().edit_message(*a, **k)

    cli = EditFail()
    db = IDX.Index(tempfile.mktemp(suffix=".db"))
    st = tg.TGStore(cli, 1, db, cache)
    await st.push_snapshot(caption=tg.LOCK_IDLE)
    first = st.snap_msg_id
    db.put_file("/pf2/a", "p" * 32, 1)
    await st.push_snapshot()
    assert st.snap_msg_id != first, "原地改失败要改发新消息"
    assert first not in cli._msgs, "旧的快照消息要删掉"
    assert db.meta("snap_msg_id") == str(st.snap_msg_id)
    m = await st.snap_msg()
    assert m is not None and m.id == st.snap_msg_id


async def t_acquire_empty(ctx):
    cli = FakeClient()
    db = IDX.Index(tempfile.mktemp(suffix=".db"))
    st = tg.TGStore(cli, 1, db, cache)
    assert await st.snap_msg() is None
    assert await st.acquire(timeout=10) is True, "空频道要先建快照再抢锁"
    assert st.holding
    assert await st.check_lock() == "own"


async def t_concurrent_move(ctx):
    v = ctx.vfs
    await _mkfile(v, "/cm/a", b"1")
    res = await asyncio.gather(v.move("/cm/a", "/cm/b"),
                               v.move("/cm/a", "/cm/b"),
                               return_exceptions=True)
    assert any(r is None for r in res), res
    assert all(r is None or isinstance(r, FileNotFoundError) for r in res), res
    assert v.get("/cm/b") is not None
    await v.delete("/cm/b")


async def t_overwrite_variants(ctx):
    cli = await make_dav(ctx)
    try:
        await cli.put("/ow/a", data=b"1")
        await cli.put("/ow/b", data=b"2")
        for flag, want in (("F", 412), ("f", 412), ("T", 204), ("t", 204),
                           ("", 204), ("x", 204)):
            await cli.put("/ow/a", data=b"1")
            await cli.put("/ow/b", data=b"2")
            h = {"Destination": "/ow/b"}
            if flag:
                h["Overwrite"] = flag
            r = await cli.request("MOVE", "/ow/a", headers=h)
            assert r.status == want, (flag, r.status)
    finally:
        await cli.close()


async def t_cache_put_zero(ctx):
    p = tempfile.mktemp()
    open(p, "wb").close()
    old = cache.CACHE_DIR
    cache.CACHE_DIR = tempfile.mkdtemp(prefix="tgnd_z_")
    try:
        assert cache.put("empty", p) == "", "空文件不进缓存"
        assert cache.put("", _dupfile(b"x")) == ""
        assert cache.put("nofile", "/不存在/x") == ""
        big = _dupfile(b"x" * 200)
        os.environ["CACHE_MAX_GB"] = "0.0000001"     # 约 107 字节
        assert cache.put("too-big", big) == "", "超过整个预算的单文件直接放弃"
        os.environ["CACHE_MAX_GB"] = "0"
    finally:
        os.environ.pop("CACHE_MAX_GB", None)
        cache.CACHE_DIR = old


async def t_flusher_resilience(ctx):
    import vfs as V

    class BadDel(FakeClient):
        async def delete_messages(self, chat, ids):
            raise RuntimeError("tg 删不动")

    cli = BadDel()
    db = IDX.Index(tempfile.mktemp(suffix=".db"))
    st = tg.TGStore(cli, 1, db, cache)
    v = V.VFS(db, st)
    h = await _mkfile(v, "/fr/a", b"flush")
    await v.delete("/fr/a")
    assert v._graveyard
    for _ in range(50):
        if not v._graveyard:
            break
        await asyncio.sleep(0.01)
    assert not v._graveyard, "删失败也要把回收站清空，不能死循环"


async def t_dav_teleport_more(ctx):
    cli = await make_dav(ctx)
    try:
        data = rand(3000)
        r = await cli.put("/tp2/a", data=data)
        h = r.headers["ETag"].strip('"').rsplit("-", 1)[0]
        await ctx.vfs.upload_pending("/tp2/a")
        r = await cli.put("/tp2/b", data=b"", headers={"X-Content-Hash": h.upper()})
        assert r.status == 201, "hash 大小写不敏感"
        r = await cli.put("/tp2/c", data=b"", headers={"X-Content-Hash": "  " + h + "  "})
        assert r.status == 201, "两端空白要忽略"
        r = await cli.put("/tp2/d", data=b"", headers={
            "X-Content-Hash": h, "X-Content-Length": "abc"})
        assert r.status == 400
        r = await cli.put("/tp2/d", data=b"", headers={
            "X-Content-Hash": h, "X-Content-Length": "-1"})
        assert r.status == 400
        r = await cli.put("/tp2/x/y/z", data=b"", headers={"X-Content-Hash": h})
        assert r.status == 201 and ctx.vfs.get("/tp2/x/y").is_dir
        # 秒传出现的内容也要能被 PROPFIND / HEAD 看见
        r = await cli.head("/tp2/b")
        assert r.headers["Content-Length"] == str(len(data))
        assert r.headers["ETag"] == r.headers["ETag"].strip()
    finally:
        await cli.close()


async def t_dav_type_mismatch(ctx):
    cli = await make_dav(ctx)
    try:
        await cli.put("/tm/f", data=b"1")
        await cli.request("MKCOL", "/tm/d")
        await cli.put("/tm/d/inner", data=b"2")
        r = await cli.request("MOVE", "/tm/d", headers={"Destination": "/tm/f"})
        assert r.status == 409, "目录不能被文件覆盖"
        r = await cli.request("COPY", "/tm/d", headers={"Destination": "/tm/f"})
        assert r.status == 409
        r = await cli.request("MOVE", "/tm/f", headers={"Destination": "/tm/d"})
        assert r.status == 409
        assert ctx.vfs.get("/tm/d/inner") is not None, "被拒绝的移动不能丢数据"
        assert ctx.vfs.get("/tm/f") is not None
    finally:
        await cli.close()


CASES = [
    ("路径规范化", t_paths),
    ("摘要与增量哈希", t_tg_digest),
    ("分片名解析", t_tg_names),
    ("切片读取器", t_tg_slicereader),
    ("分片上传与重试", t_tg_upload),
    ("TG 锁三态", t_tg_lock),
    ("快照推送与合并", t_tg_snapshot),
    ("分片区间读取", t_tg_slices),
    ("重建索引补强", t_tg_reindex_extra),
    ("重命名矩阵", t_vfs_move_matrix),
    ("复制矩阵", t_vfs_copy_matrix),
    ("删除矩阵", t_vfs_delete_matrix),
    ("内容回收", t_vfs_reclaim),
    ("读取与单飞", t_vfs_iter),
    ("写入收尾", t_vfs_finish),
    ("上传中状态", t_vfs_pending),
    ("秒传底层", t_vfs_teleport),
    ("消息回收站", t_vfs_flusher),
    ("WebDAV 方法与认证", t_dav_options),
    ("WebDAV 认证", t_dav_auth),
    ("WebDAV PROPFIND 边界", t_dav_propfind_edge),
    ("WebDAV Range 矩阵", t_dav_range_matrix),
    ("WebDAV 条件请求", t_dav_cond),
    ("WebDAV PUT 边界", t_dav_put_edge),
    ("WebDAV 删除与建目录", t_dav_delete_mkcol),
    ("WebDAV 移动复制边界", t_dav_move_copy_edge),
    ("WebDAV 路径边界", t_dav_paths_edge),
    ("WebDAV 混合交互", t_dav_mixed),
    ("本机单实例锁", t_local_lock),
    ("重启后状态", t_restart),
    ("多分片端到端", t_integration_slices),
    ("同路径并发写", t_concurrent_same),
    ("上传中删除", t_race_delete_pending),
    ("流上限", t_dav_stream_limit),
    ("大量条目", t_dav_many),
    ("索引批量", t_index_bulk),
    ("协议纯函数", t_dav_helpers),
    ("杂项接口", t_misc),
    ("后台守护", t_keeper),
    ("日志系统", t_logger),
    ("通配符路径", t_index_like),
    ("特殊字符目录", t_vfs_special_dirs),
    ("续传状态回收", t_resume_state),
    ("预算不足", t_dav_507),
    ("认证拦在入口", t_dav_401),
    ("非法路径", t_dav_bad_path),
    ("final 快照", t_snapshot_final),
    ("合并目录", t_pull_merge_dirs),
    ("待上传目录搬删", t_move_pending),
    ("路径规范化模糊", t_fuzz_norm),
    ("解析模糊", t_fuzz_parse),
    ("缓存 LRU", t_cache_lru),
    ("目录层级过滤", t_children_depth),
    ("重复分片", t_reindex_dup),
    ("缺片不污染缓存", t_read_all_partial),
    ("跨片 Range", t_vfs_range_multislice),
    ("PROPFIND XML", t_propfind_xml),
    ("快照消息回退", t_snap_fallback),
    ("快照改写失败", t_push_fallback),
    ("空频道抢锁", t_acquire_empty),
    ("并发搬运", t_concurrent_move),
    ("覆盖头变体", t_overwrite_variants),
    ("缓存拒收", t_cache_put_zero),
    ("回收站容错", t_flusher_resilience),
    ("秒传细节", t_dav_teleport_more),
    ("类型不匹配", t_dav_type_mismatch),
    ("路径边界", t_paths_edge),
    ("索引边界", t_index_edge),
    ("缓存边界", t_cache_edge),
    ("索引基本操作", t_index),
    ("写入后读出", t_put_get),
    ("秒传只存一份", t_dedup),
    ("移动复制删除", t_move_copy_delete),
    ("拒绝移入子树", t_bad_move),
    ("多分片读写", t_multislice),
    ("并发写入", t_concurrent_put),
    ("后台上传队列", t_uploader),
    ("重建索引", t_reindex),
    ("残片按分片暴露", t_lostfound_fragments),
    ("旧库自动迁移", t_migrate_legacy),
    ("快照上传与拉回", t_snapshot_roundtrip),
    ("TG 锁获取与释放", t_lock_roundtrip),
    ("WebDAV 基本语义", t_dav_basic),
    ("WebDAV Range", t_dav_range),
    ("WebDAV 断点续传", t_dav_content_range),
    ("WebDAV 断线续传", t_dav_abort_resume),
    ("WebDAV 秒传", t_dav_teleport),
    ("WebDAV MOVE/COPY/DELETE", t_dav_move_copy),
    ("WebDAV Depth:infinity 拒绝", t_dav_depth_infinity),
]


async def main():
    only = None
    if "-k" in sys.argv:
        only = sys.argv[sys.argv.index("-k") + 1]
    ctx = await make_ctx()
    for label, fn in CASES:
        if only and only not in label:
            continue
        try:
            await fn(ctx)
            rec(label, True)
        except AssertionError as e:
            rec(label, False, str(e) or "断言失败")
        except Exception as e:
            rec(label, False, f"{type(e).__name__}: {e}")
            traceback.print_exc()
    ok = sum(1 for _, o, _ in RESULTS if o)
    print(f"\n===== PASS={ok} FAIL={len(RESULTS) - ok} =====")
    return 0 if ok == len(RESULTS) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
