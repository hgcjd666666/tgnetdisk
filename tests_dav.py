"""WebDAV 方法语义、Range、上传落盘即返回、分片复用。"""

import asyncio
import hashlib
import io
import lzma
import os
import sqlite3
import sys
import tempfile
import time
import traceback
from types import SimpleNamespace

import tests_support  # 必须先于 store：它在 import 时固定缓存目录
import store as S
from store import (Store, SqliteBackend, make_session, is_db_msg, norm,
                   digest_of, LOCK_IDLE, LOCK_PREFIX, LockBusy)
from tests_support import (BLOBS, D, FakeClient, blobs, cache_env, get_bytes,
                           get_slice, make_dav, parts, put_bytes, rand, rec,
                           stop_dav, wait_done, wipe_cache)


async def t_dav_basic(ctx):
    st, client = ctx.st, ctx.client
    if "--real" in sys.argv:
        print("SKIP  WebDAV 用例只跑内存版")
        return
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer
    import server as srv
    c3 = FakeClient()
    st3 = Store(c3, 1, backend=SqliteBackend(tempfile.mktemp(suffix=".db")))
    st3.fast = True
    up3 = srv.Uploader(st3, 1)          # 不 start：PUT 只落盘入队，字节留在本地
    app = web.Application(client_max_size=0)
    app.router.add_route("*", "/{tail:.*}", lambda r: srv.handle(r, st3, up3))
    old = os.environ.get("ONLINE")
    os.environ["ONLINE"] = "0"                   # 锁本身有独立用例，这里只验 HTTP 语义
    try:
        async with TestClient(TestServer(app)) as cli:
            r = await cli.put("/dav/a.txt", data=b"hello")
            assert r.status == 201, f"PUT 新文件应 201，实际 {r.status}"
            r = await cli.put("/dav/a.txt", data=b"hello2")
            assert r.status == 204, f"PUT 覆盖应 204，实际 {r.status}"
            r = await cli.get("/dav/a.txt")
            body = await r.read()
            assert r.status == 200 and body == b"hello2", f"GET 内容不对: {body!r}"
            r = await cli.head("/dav/a.txt")
            assert r.headers.get("Content-Length") == "6", \
                f"HEAD 的 Content-Length 不对: {r.headers.get('Content-Length')}"
            r = await cli.request("MKCOL", "/dav/a.txt")
            assert r.status == 405, f"MKCOL 已存在的路径应 405，实际 {r.status}"
            r = await cli.request("MOVE", "/dav/a.txt",
                                  headers={"Destination": str(cli.make_url("/dav/a.txt"))})
            assert r.status == 204, f"MOVE 到自身应 204（空操作），实际 {r.status}"
            assert st3.get("/dav/a.txt") is not None, "MOVE 到自身把文件弄丢了"
            r = await cli.request("PROPFIND", "/dav", headers={"Depth": "1"})
            assert r.status == 207 and "a.txt" in await r.text(), "PROPFIND 没列出文件"
            r = await cli.request("OPTIONS", "/dav")
            assert r.headers.get("DAV") == "1", f"DAV 头应为 1，实际 {r.headers.get('DAV')}"
            r = await cli.request("LOCK", "/dav/a.txt")
            assert r.status == 405, f"LOCK 应 405（不实现锁），实际 {r.status}"
            await cli.put("/p/q/f.txt", data=b"q")
            r = await cli.request("MOVE", "/p/q", headers={
                "Destination": str(cli.make_url("/p")), "Overwrite": "T"})
            assert r.status == 409, f"MOVE 到祖先目录应 409，实际 {r.status}"
            assert st3.get("/p/q/f.txt") is not None, "MOVE 到祖先把源子树删了"
    finally:
        if old is None:
            os.environ.pop("ONLINE", None)
        else:
            os.environ["ONLINE"] = old

async def t_range_store(ctx):
    st, client = ctx.st, ctx.client
    data = rand(400000)
    p = f"{D}/rng/s.bin"
    await put_bytes(st, data, p, slice_size=64 * 1024)      # 片大小不整的 7 片
    assert await get_slice(st, p, 0, 10) == data[:10]
    assert await get_slice(st, p, 100000, 50000) == data[100000:150000]
    assert await get_slice(st, p, 65530, 20) == data[65530:65550]   # 跨片边界
    assert await get_slice(st, p, 390000, 100000) == data[390000:]  # 超末尾自动截
    assert await get_slice(st, p, 400000, 10) == b""                # 起点在 EOF
    assert await get_slice(st, p, 0, None) == data                  # 等效整份
    assert await get_slice(st, p, 0, 0) == b""

async def t_range_http(ctx):
    st, client = ctx.st, ctx.client
    cli, s2, up, c, srv = await make_dav(start_up=False)
    try:
        data = rand(300000)
        assert (await cli.put("/rng/h.bin", data=data)).status == 201
        # 上传中：走 staging 的本地 fd，seek 后直读
        r = await cli.get("/rng/h.bin", headers={"Range": "bytes=1000-1999"})
        assert r.status == 206, f"staging range 应 206，实际 {r.status}"
        assert r.headers.get("Content-Range") == "bytes 1000-1999/300000", \
            r.headers.get("Content-Range")
        assert r.headers.get("Content-Length") == "1000"
        assert await r.read() == data[1000:2000]
        # 不给 Range：200 且宣告可分段
        r = await cli.get("/rng/h.bin")
        assert r.status == 200 and r.headers.get("Accept-Ranges") == "bytes"
        assert await r.read() == data
        # 尾段
        r = await cli.get("/rng/h.bin", headers={"Range": "bytes=-100"})
        assert r.status == 206 and await r.read() == data[-100:]
        # 开区间
        r = await cli.get("/rng/h.bin", headers={"Range": "bytes=299000-"})
        assert r.status == 206 and await r.read() == data[299000:]
        # 起点越界 → 416
        r = await cli.get("/rng/h.bin", headers={"Range": "bytes=999999-"})
        assert r.status == 416, f"越界应 416，实际 {r.status}"
        assert r.headers.get("Content-Range") == "bytes */300000"
        # 多区间不实现：忽略 Range，回整份
        r = await cli.get("/rng/h.bin", headers={"Range": "bytes=0-9,20-29"})
        assert r.status == 200
        # 乱写的 Range 也当没给
        r = await cli.get("/rng/h.bin", headers={"Range": "bytes=abc"})
        assert r.status == 416 or r.status == 200
        # HEAD 同样宣告 bytes
        r = await cli.head("/rng/h.bin")
        assert r.headers.get("Accept-Ranges") == "bytes"
    finally:
        await stop_dav(cli, up)

async def t_range_after_upload(ctx):
    st, client = ctx.st, ctx.client
    cache_env(None)
    cli, s2, up, c, srv = await make_dav()
    try:
        data = rand(300000)
        assert (await cli.put("/rng/t.bin", data=data)).status == 201
        e = None
        for _ in range(200):
            await asyncio.sleep(0.05)
            e = s2.get("/rng/t.bin")
            if e and e.status == 0:
                break
        assert e and e.status == 0, "后台没传完"
        r = await cli.get("/rng/t.bin", headers={"Range": "bytes=100000-100009"})
        assert r.status == 206
        assert r.headers.get("Content-Range") == "bytes 100000-100009/300000"
        assert await r.read() == data[100000:100010], "从 TG 取的分段内容不对"
        r = await cli.get("/rng/t.bin")
        assert await r.read() == data
    finally:
        await stop_dav(cli, up)

async def t_range_cache_hit(ctx):
    st, client = ctx.st, ctx.client
    cache_env(1)
    wipe_cache()
    cli, s2, up, c, srv = await make_dav()
    try:
        data = rand(300000)
        assert (await cli.put("/rng/k.bin", data=data)).status == 201
        e = None
        for _ in range(200):
            await asyncio.sleep(0.05)
            e = s2.get("/rng/k.bin")
            if e and e.status == 0:
                break
        assert e and e.status == 0
        assert await (await cli.get("/rng/k.bin")).read() == data   # 落缓存
        assert len(blobs()) == 1, f"没落缓存: {blobs()}"
        n = c.downloads
        r = await cli.get("/rng/k.bin", headers={"Range": "bytes=200000-200099"})
        assert r.status == 206
        assert await r.read() == data[200000:200100]
        assert c.downloads == n, "命中缓存时 range 不该再去 TG"
    finally:
        cache_env(None)
        await stop_dav(cli, up)

async def t_edge_staging_range(ctx):
    st, client = ctx.st, ctx.client
    # 还没传完的文件：Range 从 staging 读，不建部分缓存
    cache_env(1)
    wipe_cache()
    cli, s2, up, c, srv = await make_dav(start_up=False)
    try:
        data = rand(50000)
        assert (await cli.put("/edge/stg.bin", data=data)).status == 201
        r = await cli.get("/edge/stg.bin", headers={"Range": "bytes=1000-1999"})
        assert r.status == 206 and await r.read() == data[1000:2000]
        assert not parts(), "上传中的文件不该建部分缓存"
    finally:
        cache_env(None)
        await stop_dav(cli, up)

async def t_sendmedia_retry_reuses_upload(ctx):
    st, client = ctx.st, ctx.client
    """DC 上传成功、建消息失败：用同一个 ref 重试，不重传整个 slice。"""
    c = FakeClient()
    c.send_fail_times = 1
    st2 = Store(c, 1, backend=SqliteBackend(tempfile.mktemp(suffix=".db")))
    st2.fast = True
    await put_bytes(st2, rand(300000), f"{D}/retry")
    assert len(c._uploads) == 1, f"重传了字节，DC 上传 {len(c._uploads)} 次"
    e = st2.get(f"{D}/retry")
    assert e is not None and e.status == 0, "重试后没落地"
    assert await get_bytes(st2, f"{D}/retry") is not None

async def t_slice_reuse_from_lostfound(ctx):
    st, client = ctx.st, ctx.client
    """重传时复用 TG 上已有的分片（lost+found 捡回来的也算），只补缺口。"""
    if "--real" in sys.argv:
        print("SKIP  分片复用用例只跑内存版")
        return
    SZ = 64 * 1024
    data = rand(SZ * 3 + 10000)
    fd, tmp = tempfile.mkstemp()
    with os.fdopen(fd, "wb") as f:
        f.write(data)
    try:
        dig = digest_of(tmp)
        with open(tmp, "rb") as f:
            m0 = await client.send_file(
                1, S.SliceReader(f, 0, SZ),
                attributes=[SimpleNamespace(file_name=f"{dig}.0")])
            m2 = await client.send_file(
                1, S.SliceReader(f, 2 * SZ, SZ),
                attributes=[SimpleNamespace(file_name=f"{dig}.2")])
        await st.sync(full=True)            # 残片收进 /lost+found
        assert st.get("/lost+found/" + dig) is not None, "残片没收进 lost+found"
        before = len(client._msgs)
        e = await st.put_file(f"{D}/reuse", tmp, slice_size=SZ)
        added = len(client._msgs) - before
        assert e.parts.get(0) == m0.id and e.parts.get(2) == m2.id, \
            f"没复用已有分片: {e.parts}"
        assert added == 2, f"应该只补 2 片，实际新传 {added} 片"
        assert await get_bytes(st, f"{D}/reuse") == data
    finally:
        os.unlink(tmp)

async def t_slice_reuse_rejects_mismatch(ctx):
    st, client = ctx.st, ctx.client
    """切片大小变了就不能复用：字节数对不上说明不是同一刀切的。"""
    if "--real" in sys.argv:
        print("SKIP  分片复用用例只跑内存版")
        return
    data = rand(200000)
    fd, tmp = tempfile.mkstemp()
    with os.fdopen(fd, "wb") as f:
        f.write(data)
    try:
        dig = digest_of(tmp)
        with open(tmp, "rb") as f:
            m0 = await client.send_file(
                1, S.SliceReader(f, 0, 64 * 1024),
                attributes=[SimpleNamespace(file_name=f"{dig}.0")])
        await st.sync(full=True)
        e = await st.put_file(f"{D}/reuse2", tmp, slice_size=128 * 1024)
        assert e.parts.get(0) != m0.id, "切片大小不同却复用了旧片"
        assert await get_bytes(st, f"{D}/reuse2") == data
    finally:
        os.unlink(tmp)

async def main(ctx):
    return [
        ("t_dav_basic", t_dav_basic),
        ("t_range_store", t_range_store),
        ("t_range_http", t_range_http),
        ("t_range_after_upload", t_range_after_upload),
        ("t_range_cache_hit", t_range_cache_hit),
        ("t_edge_staging_range", t_edge_staging_range),
        ("t_sendmedia_retry_reuses_upload", t_sendmedia_retry_reuses_upload),
        ("t_slice_reuse_from_lostfound", t_slice_reuse_from_lostfound),
        ("t_slice_reuse_rejects_mismatch", t_slice_reuse_rejects_mismatch),
    ]
