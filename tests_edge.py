"""补洞、稀疏文件、块对齐、以及各种边界/竞态。"""

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


async def t_fill_from_range(ctx):
    st, client = ctx.st, ctx.client
    cache_env(1)
    wipe_cache()
    cli, s2, up, c, srv = await make_dav(start_fill=False)
    try:
        data = rand(300000)
        assert (await cli.put("/fill/a.bin", data=data)).status == 201
        assert await wait_done(
            lambda: (s2.get("/fill/a.bin") or SimpleNamespace(status=1)).status == 0), \
            "后台没传完"
        e = s2.get("/fill/a.bin")
        key = S.cache_key(e)
        pp = S.partial_path(key)
        assert not os.path.exists(pp), "还没读就建出了部分缓存"
        blk = os.statvfs(S.CACHE_DIR).f_frsize or 4096
        lo, hi = 2 * blk, 4 * blk                     # 两个完整块
        r = await cli.get("/fill/a.bin", headers={"Range": f"bytes={lo}-{hi - 1}"})
        assert r.status == 206 and await r.read() == data[lo:hi]
        assert os.path.exists(pp), "Range 请求没建部分缓存"
        fd = os.open(pp, os.O_RDONLY)
        try:
            assert os.fstat(fd).st_size == 300000, "部分缓存长度不对"
            assert os.pread(fd, blk, lo) == data[lo:lo + blk], "写到了错误偏移"
            assert S.first_hole(fd, 300000) == 0, "开头应该是洞"
            assert S.first_hole(fd, 300000) is not None, "只写了一段不该判成收齐"
        finally:
            os.close(fd)
        # 不足一个块的请求不写缓存（客户端照样收得到）
        before = os.fstat(os.open(pp, os.O_RDONLY)).st_size
        r = await cli.get("/fill/a.bin", headers={"Range": "bytes=200001-200099"})
        assert r.status == 206 and await r.read() == data[200001:200100]
        fd = os.open(pp, os.O_RDONLY)
        try:
            assert S.first_hole(fd, 300000) == 0, "不足一块的边缘不该写进缓存"
            assert os.fstat(fd).st_size == before
        finally:
            os.close(fd)
    finally:
        cache_env(None)
        await stop_dav(cli, up)

async def t_fill_complete(ctx):
    st, client = ctx.st, ctx.client
    cache_env(1)
    wipe_cache()
    cli, s2, up, c, srv = await make_dav()
    old_rate = srv.FILL_RATE
    srv.FILL_RATE = 8 << 20                     # 测试里别真等限速
    try:
        data = rand(300000)
        assert (await cli.put("/fill/b.bin", data=data)).status == 201
        assert await wait_done(
            lambda: (s2.get("/fill/b.bin") or SimpleNamespace(status=1)).status == 0)
        e = s2.get("/fill/b.bin")
        key = S.cache_key(e)
        pp, final = S.partial_path(key), os.path.join(S.CACHE_DIR, "blobs", key)
        r = await cli.get("/fill/b.bin", headers={"Range": "bytes=0-999"})
        assert r.status == 206
        assert await wait_done(lambda: os.path.exists(final) and not os.path.exists(pp)), \
            "后台没把洞补完并转正"
        with open(final, "rb") as f:
            assert f.read() == data, "补出来的内容和原始数据不一致"
        assert S.hash_fd(os.open(final, os.O_RDONLY), 300000) == e.hash, "hash 对不上"
        n = c.downloads
        r = await cli.get("/fill/b.bin", headers={"Range": "bytes=200000-200009"})
        assert await r.read() == data[200000:200010]
        assert c.downloads == n, "补齐后 Range 还去 TG 取"
    finally:
        srv.FILL_RATE = old_rate
        cache_env(None)
        await stop_dav(cli, up)

async def t_fill_punch_zeros(ctx):
    st, client = ctx.st, ctx.client
    cache_env(1)
    wipe_cache()
    cli, s2, up, c, srv = await make_dav()
    old_rate = srv.FILL_RATE
    srv.FILL_RATE = 64 << 20
    try:
        # 中间插 4MB 真正的全 0：打洞后物理占用该明显小于逻辑大小
        data = b"X" * (2 << 20) + b"\0" * (4 << 20) + b"Y" * (2 << 20)
        assert (await cli.put("/fill/z.bin", data=data)).status == 201
        assert await wait_done(
            lambda: (s2.get("/fill/z.bin") or SimpleNamespace(status=1)).status == 0)
        e = s2.get("/fill/z.bin")
        key = S.cache_key(e)
        final = os.path.join(S.CACHE_DIR, "blobs", key)
        r = await cli.get("/fill/z.bin", headers={"Range": "bytes=0-99"})
        assert r.status == 206
        assert await wait_done(lambda: os.path.exists(final)), "没补齐"
        st = os.stat(final)
        assert st.st_size == len(data), "打洞不该改变逻辑大小"
        assert st.st_blocks * 512 <= len(data) // 2, \
            f"全 0 的块没被打洞（物理 {st.st_blocks*512} / 逻辑 {len(data)}）"
        with open(final, "rb") as f:
            assert f.read() == data, "打洞后内容变了"
    finally:
        srv.FILL_RATE = old_rate
        cache_env(None)
        await stop_dav(cli, up)

async def t_fill_no_partial_after_full_get(ctx):
    st, client = ctx.st, ctx.client
    cache_env(1)
    wipe_cache()
    cli, s2, up, c, srv = await make_dav()
    try:
        data = rand(200000)
        assert (await cli.put("/fill/c.bin", data=data)).status == 201
        assert await wait_done(
            lambda: (s2.get("/fill/c.bin") or SimpleNamespace(status=1)).status == 0)
        e = s2.get("/fill/c.bin")
        key = S.cache_key(e)
        assert await (await cli.get("/fill/c.bin")).read() == data     # 全量 → 落缓存
        assert len(blobs()) == 1, f"全量读没落缓存: {blobs()}"
        assert not os.path.exists(S.partial_path(key)), "全量读不该再建部分缓存"
        r = await cli.get("/fill/c.bin", headers={"Range": "bytes=0-9"})
        assert r.status == 206
        assert not os.path.exists(S.partial_path(key)), "命中缓存后 Range 不该建部分缓存"
    finally:
        cache_env(None)
        await stop_dav(cli, up)

async def t_fill_off_when_cache_off(ctx):
    st, client = ctx.st, ctx.client
    cache_env(None)
    wipe_cache()
    cli, s2, up, c, srv = await make_dav()
    try:
        data = rand(100000)
        assert (await cli.put("/fill/d.bin", data=data)).status == 201
        assert await wait_done(
            lambda: (s2.get("/fill/d.bin") or SimpleNamespace(status=1)).status == 0)
        r = await cli.get("/fill/d.bin", headers={"Range": "bytes=0-99"})
        assert r.status == 206 and await r.read() == data[:100]
        left = [n for n in os.listdir(os.path.join(S.CACHE_DIR, "blobs"))
                if n.endswith(".part")] if os.path.isdir(
                    os.path.join(S.CACHE_DIR, "blobs")) else []
        assert left == [], f"缓存关闭还建了部分缓存: {left}"
    finally:
        await stop_dav(cli, up)

async def t_edge_tiny_file(ctx):
    st, client = ctx.st, ctx.client
    # 比一个块还小的文件：整段都不足一块，一个字节都不该写进缓存
    cache_env(1)
    wipe_cache()
    cli, s2, up, c, srv = await make_dav()
    try:
        data = rand(100)
        assert (await cli.put("/edge/tiny.bin", data=data)).status == 201
        assert await wait_done(
            lambda: (s2.get("/edge/tiny.bin") or SimpleNamespace(status=1)).status == 0)
        e = s2.get("/edge/tiny.bin")
        key = S.cache_key(e)
        r = await cli.get("/edge/tiny.bin", headers={"Range": "bytes=0-99"})
        assert r.status == 206 and await r.read() == data
        assert await wait_done(
            lambda: os.path.exists(os.path.join(S.CACHE_DIR, "blobs", key))), "小文件没补齐"
        with open(os.path.join(S.CACHE_DIR, "blobs", key), "rb") as f:
            assert f.read() == data
    finally:
        cache_env(None)
        await stop_dav(cli, up)

async def t_edge_one_block(ctx):
    st, client = ctx.st, ctx.client
    # 正好一个块：Range 覆盖整块，应该直接写进缓存
    cache_env(1)
    wipe_cache()
    cli, s2, up, c, srv = await make_dav(start_fill=False)
    try:
        blk = os.statvfs(S.CACHE_DIR).f_frsize or 4096
        data = rand(blk)
        assert (await cli.put("/edge/one.bin", data=data)).status == 201
        assert await wait_done(
            lambda: (s2.get("/edge/one.bin") or SimpleNamespace(status=1)).status == 0)
        e = s2.get("/edge/one.bin")
        key = S.cache_key(e)
        pp = S.partial_path(key)
        r = await cli.get("/edge/one.bin", headers={"Range": f"bytes=0-{blk - 1}"})
        assert r.status == 206 and await r.read() == data
        fd = os.open(pp, os.O_RDONLY)
        try:
            assert S.first_hole(fd, blk) is None, "整块写完应该没有洞"
            assert os.pread(fd, blk, 0) == data
        finally:
            os.close(fd)
    finally:
        cache_env(None)
        await stop_dav(cli, up)

async def t_edge_tail_block(ctx):
    st, client = ctx.st, ctx.client
    # size 不是块整数倍：尾块不足一块，绝不能只写一半
    cache_env(1)
    wipe_cache()
    cli, s2, up, c, srv = await make_dav(start_fill=False)
    try:
        blk = os.statvfs(S.CACHE_DIR).f_frsize or 4096
        size = blk * 4 + 1234
        data = rand(size)
        assert (await cli.put("/edge/tail.bin", data=data)).status == 201
        assert await wait_done(
            lambda: (s2.get("/edge/tail.bin") or SimpleNamespace(status=1)).status == 0)
        e = s2.get("/edge/tail.bin")
        key = S.cache_key(e)
        pp = S.partial_path(key)
        last_blk = blk * 4
        r = await cli.get("/edge/tail.bin", headers={"Range": f"bytes=0-{size - 1}"})
        assert r.status == 206 and await r.read() == data
        fd = os.open(pp, os.O_RDONLY)
        try:
            assert os.pread(fd, last_blk, 0) == data[:last_blk], "前面的完整块该写进去"
            tail = os.pread(fd, size - last_blk, last_blk)
            assert tail == b"\0" * (size - last_blk), \
                "不足一块的尾块不能写，写半个块会把整块标成已缓存"
            assert S.first_hole(fd, size) == last_blk, "尾块应该还是洞，留给补洞"
        finally:
            os.close(fd)
    finally:
        cache_env(None)
        await stop_dav(cli, up)

async def t_edge_whole_and_suffix(ctx):
    st, client = ctx.st, ctx.client
    cache_env(1)
    wipe_cache()
    cli, s2, up, c, srv = await make_dav()
    try:
        data = rand(50000)
        assert (await cli.put("/edge/ws.bin", data=data)).status == 201
        assert await wait_done(
            lambda: (s2.get("/edge/ws.bin") or SimpleNamespace(status=1)).status == 0)
        r = await cli.get("/edge/ws.bin", headers={"Range": "bytes=0-"})
        assert r.status == 206 and await r.read() == data
        r = await cli.get("/edge/ws.bin", headers={"Range": "bytes=-1000"})
        assert r.status == 206 and await r.read() == data[-1000:]
        r = await cli.get("/edge/ws.bin", headers={"Range": "bytes=49999-"})
        assert r.status == 206 and await r.read() == data[-1:]
    finally:
        cache_env(None)
        await stop_dav(cli, up)

async def t_edge_empty_file(ctx):
    st, client = ctx.st, ctx.client
    cache_env(1)
    wipe_cache()
    cli, s2, up, c, srv = await make_dav()
    try:
        assert (await cli.put("/edge/zero.bin", data=b"")).status == 201
        assert await wait_done(
            lambda: (s2.get("/edge/zero.bin") or SimpleNamespace(status=1)).status == 0)
        r = await cli.get("/edge/zero.bin")
        assert r.status == 200 and await r.read() == b""
        r = await cli.get("/edge/zero.bin", headers={"Range": "bytes=0-0"})
        assert r.status in (200, 416), f"空文件 Range 不该 500，实际 {r.status}"
        assert not parts(), "空文件不该有部分缓存"
    finally:
        cache_env(None)
        await stop_dav(cli, up)

async def t_edge_concurrent_ranges(ctx):
    st, client = ctx.st, ctx.client
    # 多个 Range 同时写同一个稀疏文件的不同块
    cache_env(1)
    wipe_cache()
    cli, s2, up, c, srv = await make_dav(start_fill=False)
    try:
        blk = os.statvfs(S.CACHE_DIR).f_frsize or 4096
        size = blk * 8
        data = rand(size)
        assert (await cli.put("/edge/par.bin", data=data)).status == 201
        assert await wait_done(
            lambda: (s2.get("/edge/par.bin") or SimpleNamespace(status=1)).status == 0)
        e = s2.get("/edge/par.bin")
        pp = S.partial_path(S.cache_key(e))
        spans = [(i * blk, i * blk + blk - 1) for i in (1, 3, 5, 7)]
        rs = await asyncio.gather(*[
            cli.get("/edge/par.bin", headers={"Range": f"bytes={a}-{b}"})
            for a, b in spans])
        bodies = [await r.read() for r in rs]
        for (a, b), body in zip(spans, bodies):
            assert body == data[a:b + 1], "并发 Range 内容错位"
        fd = os.open(pp, os.O_RDONLY)
        try:
            for a, _b in spans:
                assert os.pread(fd, blk, a) == data[a:a + blk], f"块 {a} 没写对"
            assert S.first_hole(fd, size) == 0, "前面没写的块应该还是洞"
        finally:
            os.close(fd)
    finally:
        cache_env(None)
        await stop_dav(cli, up)

async def t_edge_tampered_partial(ctx):
    st, client = ctx.st, ctx.client
    # 部分缓存被人改过：补完洞之后 hash 必须对不上，整份丢掉
    cache_env(1)
    wipe_cache()
    cli, s2, up, c, srv = await make_dav(start_fill=False)
    try:
        blk = os.statvfs(S.CACHE_DIR).f_frsize or 4096
        data = rand(blk * 4)
        assert (await cli.put("/edge/tamper.bin", data=data)).status == 201
        assert await wait_done(
            lambda: (s2.get("/edge/tamper.bin") or SimpleNamespace(status=1)).status == 0)
        e = s2.get("/edge/tamper.bin")
        key = S.cache_key(e)
        pp = S.partial_path(key)
        final = os.path.join(S.CACHE_DIR, "blobs", key)
        r = await cli.get("/edge/tamper.bin", headers={"Range": f"bytes=0-{blk - 1}"})
        assert r.status == 206
        with open(pp, "r+b") as f:          # 写进去的块被改掉
            f.seek(0)
            f.write(b"Z" * blk)
        up.fill.want("/edge/tamper.bin")
        up.fill.start()
        assert await wait_done(lambda: not os.path.exists(pp)), "坏数据没被丢掉"
        assert not os.path.exists(final), "hash 不对还转正了"
    finally:
        cache_env(None)
        await stop_dav(cli, up)

async def t_edge_partial_size_changed(ctx):
    st, client = ctx.st, ctx.client
    # 部分缓存长度和记录对不上（内容换过了）：要整个重建
    cache_env(1)
    wipe_cache()
    cli, s2, up, c, srv = await make_dav(start_fill=False)
    try:
        blk = os.statvfs(S.CACHE_DIR).f_frsize or 4096
        data = rand(blk * 3)
        assert (await cli.put("/edge/resize.bin", data=data)).status == 201
        assert await wait_done(
            lambda: (s2.get("/edge/resize.bin") or SimpleNamespace(status=1)).status == 0)
        e = s2.get("/edge/resize.bin")
        pp = S.partial_path(S.cache_key(e))
        r = await cli.get("/edge/resize.bin", headers={"Range": f"bytes=0-{blk - 1}"})
        assert r.status == 206
        with open(pp, "r+b") as f:          # 模拟长度错乱
            f.truncate(1000)
        r = await cli.get("/edge/resize.bin", headers={"Range": f"bytes=0-{blk - 1}"})
        assert r.status == 206 and await r.read() == data[:blk]
        assert os.path.getsize(pp) == blk * 3, "长度不对没重建"
    finally:
        cache_env(None)
        await stop_dav(cli, up)

async def t_edge_multislice_range(ctx):
    st, client = ctx.st, ctx.client
    # 多个 TG 分片：Range 跨片时不能错位
    cache_env(1)
    wipe_cache()
    cli, s2, up, c, srv = await make_dav(start_fill=False)
    try:
        data = rand(300000)
        await put_bytes(s2, data, "/edge/multi.bin", slice_size=64 * 1024)  # 5 片
        e = s2.get("/edge/multi.bin")
        assert e.status == 0 and len(e.parts) == 5, f"分片数 {len(e.parts)}"
        for lo, hi in ((60000, 70000), (65530, 65550), (130000, 140000)):
            r = await cli.get("/edge/multi.bin",
                              headers={"Range": f"bytes={lo}-{hi}"})
            assert r.status == 206, r.status
            assert await r.read() == data[lo:hi + 1], f"[{lo},{hi}] 跨片读错位"
    finally:
        cache_env(None)
        await stop_dav(cli, up)

async def t_edge_part_not_evicted(ctx):
    st, client = ctx.st, ctx.client
    # 正在填的 .part 算占用但不参与淘汰，腾空间只能删正式条目
    cache_env(0.001)                       # ≈1MB
    wipe_cache()
    pp = os.path.join(BLOBS, "deadbeef-300000" + S.PART_SUFFIX)
    with open(pp, "wb") as f:
        f.write(b"P" * 10000)
    fp = os.path.join(BLOBS, "cafe-300000")
    with open(fp, "wb") as f:
        f.write(b"F" * (2 << 20))
    S.cache_trim(0)
    assert os.path.exists(pp), "正在填的部分缓存被 LRU 删掉了"
    assert not os.path.exists(fp), "正式条目该被淘汰腾地方"
    wipe_cache()
    cache_env(None)

async def t_edge_part_ttl(ctx):
    st, client = ctx.st, ctx.client
    cache_env(1)
    wipe_cache()
    pp = os.path.join(BLOBS, "feedface-1000" + S.PART_SUFFIX)
    with open(pp, "wb") as f:
        f.write(b"Q" * 100)
    old = time.time() - S.PART_TTL - 100
    os.utime(pp, (old, old))
    S._cache_swept_at = 0.0
    S.cache_sweep()
    assert not os.path.exists(pp), "过期部分缓存没被清掉"
    cache_env(None)

async def t_edge_fill_failure_keeps_partial(ctx):
    st, client = ctx.st, ctx.client
    # 补洞时数据拿不到：不能转正，部分缓存留着等下次
    cache_env(1)
    wipe_cache()
    cli, s2, up, c, srv = await make_dav(start_fill=False)
    try:
        blk = os.statvfs(S.CACHE_DIR).f_frsize or 4096
        data = rand(blk * 3)
        assert (await cli.put("/edge/fail.bin", data=data)).status == 201
        assert await wait_done(
            lambda: (s2.get("/edge/fail.bin") or SimpleNamespace(status=1)).status == 0)
        e = s2.get("/edge/fail.bin")
        key = S.cache_key(e)
        pp = S.partial_path(key)
        final = os.path.join(S.CACHE_DIR, "blobs", key)
        r = await cli.get("/edge/fail.bin", headers={"Range": f"bytes=0-{blk - 1}"})
        assert r.status == 206
        mid = S.part_id(next(iter(e.parts.values())))
        c._blobs.pop(mid, None)             # 分片消息取不到数据了
        up.fill.want("/edge/fail.bin")
        up.fill.start()
        await asyncio.sleep(0.4)
        assert not os.path.exists(final), "拿不到数据却转正了"
        assert os.path.exists(pp), "失败后部分缓存该留着，别把已下的也扔了"
    finally:
        cache_env(None)
        await stop_dav(cli, up)

async def t_edge_fill_rate_zero(ctx):
    st, client = ctx.st, ctx.client
    # FILL_RATE_KB=0 关掉边看边缓存：不建 .part，Range 照常可用
    cache_env(1)
    wipe_cache()
    cli, s2, up, c, srv = await make_dav()
    old_rate = srv.FILL_RATE
    srv.FILL_RATE = 0
    try:
        data = rand(50000)
        assert (await cli.put("/edge/off.bin", data=data)).status == 201
        assert await wait_done(
            lambda: (s2.get("/edge/off.bin") or SimpleNamespace(status=1)).status == 0)
        r = await cli.get("/edge/off.bin", headers={"Range": "bytes=0-99"})
        assert r.status == 206 and await r.read() == data[:100]
        assert not parts(), "补洞关了还建部分缓存"
    finally:
        srv.FILL_RATE = old_rate
        cache_env(None)
        await stop_dav(cli, up)

async def t_edge_full_get_and_range_race(ctx):
    st, client = ctx.st, ctx.client
    # 同一个文件，一边全量下、一边拖进度条
    cache_env(1)
    wipe_cache()
    cli, s2, up, c, srv = await make_dav()
    try:
        data = rand(200000)
        assert (await cli.put("/edge/race.bin", data=data)).status == 201
        assert await wait_done(
            lambda: (s2.get("/edge/race.bin") or SimpleNamespace(status=1)).status == 0)
        full, part = await asyncio.gather(
            cli.get("/edge/race.bin"),
            cli.get("/edge/race.bin", headers={"Range": "bytes=100000-100999"}))
        assert await full.read() == data, "全量下载内容被影响"
        assert await part.read() == data[100000:101000], "Range 内容被影响"
    finally:
        cache_env(None)
        await stop_dav(cli, up)

async def main(ctx):
    return [
        ("t_fill_from_range", t_fill_from_range),
        ("t_fill_complete", t_fill_complete),
        ("t_fill_punch_zeros", t_fill_punch_zeros),
        ("t_fill_no_partial_after_full_get", t_fill_no_partial_after_full_get),
        ("t_fill_off_when_cache_off", t_fill_off_when_cache_off),
        ("t_edge_tiny_file", t_edge_tiny_file),
        ("t_edge_one_block", t_edge_one_block),
        ("t_edge_tail_block", t_edge_tail_block),
        ("t_edge_whole_and_suffix", t_edge_whole_and_suffix),
        ("t_edge_empty_file", t_edge_empty_file),
        ("t_edge_concurrent_ranges", t_edge_concurrent_ranges),
        ("t_edge_tampered_partial", t_edge_tampered_partial),
        ("t_edge_partial_size_changed", t_edge_partial_size_changed),
        ("t_edge_multislice_range", t_edge_multislice_range),
        ("t_edge_part_not_evicted", t_edge_part_not_evicted),
        ("t_edge_part_ttl", t_edge_part_ttl),
        ("t_edge_fill_failure_keeps_partial", t_edge_fill_failure_keeps_partial),
        ("t_edge_fill_rate_zero", t_edge_fill_rate_zero),
        ("t_edge_full_get_and_range_race", t_edge_full_get_and_range_race),
    ]
