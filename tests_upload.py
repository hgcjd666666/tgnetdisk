"""PUT 落盘即返回之后，后台上传队列的各种状态迁移。"""

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


async def t_up_accept(ctx):
    st, client = ctx.st, ctx.client
    cli, s2, up, c, srv = await make_dav(start_up=False)
    try:
        data = rand(300000)
        r = await cli.put("/up/a.bin", data=data)
        assert r.status == 201, f"PUT 应 201，实际 {r.status}"
        e = s2.get("/up/a.bin")
        assert e.status == 1, "PUT 返回时不该已经传完（这是落盘即返回）"
        assert os.path.exists(S.staging_path(e.hash)), "staging 里没有本地副本"
        n = c.downloads
        r = await cli.get("/up/a.bin")
        assert r.status == 200, f"上传中 GET 应能读到 staging，实际 {r.status}"
        assert await r.read() == data, "从 staging 读回的内容不对"
        assert c.downloads == n, "上传中的 GET 不该去 TG 取"
    finally:
        await stop_dav(cli, up)

async def t_up_finish(ctx):
    st, client = ctx.st, ctx.client
    cli, s2, up, c, srv = await make_dav()
    try:
        data = rand(300000)
        assert (await cli.put("/up/b.bin", data=data)).status == 201
        e = s2.get("/up/b.bin")
        dig = e.hash
        for _ in range(200):
            await asyncio.sleep(0.05)
            e = s2.get("/up/b.bin")
            if e and e.status == 0:
                break
        assert e.status == 0, "后台一直没传完"
        assert not os.path.exists(S.staging_path(dig)), "传完没清掉 staging"
        r = await cli.get("/up/b.bin")
        assert await r.read() == data, "传完后从 TG 读回的内容不对"
    finally:
        await stop_dav(cli, up)

async def t_up_delete_pending(ctx):
    st, client = ctx.st, ctx.client
    cli, s2, up, c, srv = await make_dav(start_up=False)
    try:
        assert (await cli.put("/up/c.bin", data=rand(50000))).status == 201
        e = s2.get("/up/c.bin")
        dig = e.hash
        assert os.path.exists(S.staging_path(dig))
        r = await cli.delete("/up/c.bin")
        assert r.status == 204
        assert s2.get("/up/c.bin") is None
        assert not os.path.exists(S.staging_path(dig)), "删了文件 staging 还留着"
        assert not any(d == dig for _p, d, *_ in s2.backend.pending()), "pending 没清"
    finally:
        await stop_dav(cli, up)

async def t_up_move_pending(ctx):
    st, client = ctx.st, ctx.client
    cli, s2, up, c, srv = await make_dav()
    try:
        data = rand(120000)
        assert (await cli.put("/up/m1.bin", data=data)).status == 201
        r = await cli.request("MOVE", "/up/m1.bin",
                              headers={"Destination": str(cli.make_url("/up/m2.bin"))})
        # 目标是新路径 → 201 Created（RFC 4918），覆盖已有目标才是 204
        assert r.status in (201, 204), f"MOVE 失败 {r.status}"
        for _ in range(200):
            await asyncio.sleep(0.05)
            e = s2.get("/up/m2.bin")
            if e and e.status == 0:
                break
        assert e is not None and e.status == 0, "改路径后后台没跟上（队列存了旧 path）"
        r = await cli.get("/up/m2.bin")
        assert await r.read() == data
    finally:
        await stop_dav(cli, up)

async def t_up_overwrite_pending(ctx):
    st, client = ctx.st, ctx.client
    cli, s2, up, c, srv = await make_dav(start_up=False)
    try:
        assert (await cli.put("/up/o.bin", data=b"v1" * 20000)).status == 201
        d1 = s2.get("/up/o.bin").hash
        assert (await cli.put("/up/o.bin", data=b"v2" * 20000)).status == 204
        e = s2.get("/up/o.bin")
        assert e.hash != d1, "覆盖后记录还是旧内容"
        assert e.status == 1
        assert os.path.exists(S.staging_path(e.hash)), "新内容的 staging 不在"
    finally:
        await stop_dav(cli, up)

async def t_up_crash_recover(ctx):
    st, client = ctx.st, ctx.client
    # 索引里留着 status=1、staging 有文件 = 崩溃后重启该续传的状态
    cli, s2, up, c, srv = await make_dav(start_up=False)
    try:
        data = rand(200000)
        assert (await cli.put("/up/r.bin", data=data)).status == 201
        e = s2.get("/up/r.bin")
        dig = e.hash
        up2 = srv.Uploader(s2, 1)          # 模拟新进程起来
        up2.start()                        # start 里会从 pending 重建队列
        for _ in range(200):
            await asyncio.sleep(0.05)
            e = s2.get("/up/r.bin")
            if e and e.status == 0:
                break
        assert e.status == 0, "重启后没把 pending 续上"
        assert not os.path.exists(S.staging_path(dig))
        assert await get_bytes(s2, "/up/r.bin") == data
        for t in up2.tasks:
            t.cancel()
    finally:
        await stop_dav(cli, up)

async def t_up_gc_orphan_staging(ctx):
    st, client = ctx.st, ctx.client
    cli, s2, up, c, srv = await make_dav(start_up=False)
    try:
        orphan = S.staging_path("f" * 40)
        with open(orphan, "wb") as f:
            f.write(b"junk")
        up2 = srv.Uploader(s2, 1)
        up2.start()                        # start 会顺手清掉没人认领的 staging
        await asyncio.sleep(0.05)
        assert not os.path.exists(orphan), "无主 staging 没被清掉"
        for t in up2.tasks:
            t.cancel()
    finally:
        await stop_dav(cli, up)

async def main(ctx):
    return [
        ("t_up_accept", t_up_accept),
        ("t_up_finish", t_up_finish),
        ("t_up_delete_pending", t_up_delete_pending),
        ("t_up_move_pending", t_up_move_pending),
        ("t_up_overwrite_pending", t_up_overwrite_pending),
        ("t_up_crash_recover", t_up_crash_recover),
        ("t_up_gc_orphan_staging", t_up_gc_orphan_staging),
    ]
