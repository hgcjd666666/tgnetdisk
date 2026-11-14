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
async def make_dav(ctx):
    from aiohttp.test_utils import TestClient, TestServer
    import webdav
    import main as M
    dav = webdav.DAV(ctx.vfs, None)
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


CASES = [
    ("路径规范化", t_paths),
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
