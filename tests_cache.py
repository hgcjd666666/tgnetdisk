"""本地读缓存：命中、淘汰、失效、回收。"""

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


async def t_cache_default_off(ctx):
    st, client = ctx.st, ctx.client
    cache_env(None)
    wipe_cache()
    data = rand(40000)
    await put_bytes(st, data, f"{D}/cache/off")
    assert await get_bytes(st, f"{D}/cache/off") == data
    assert blobs() == [], f"缓存默认该关闭，却落了盘: {blobs()}"

async def t_cache_hit(ctx):
    st, client = ctx.st, ctx.client
    cache_env(1)
    wipe_cache()
    data = rand(300000)
    await put_bytes(st, data, f"{D}/cache/hit", slice_size=64 * 1024)
    assert await get_bytes(st, f"{D}/cache/hit") == data
    assert len(blobs()) == 1, f"读完没落缓存: {blobs()}"
    n = client.downloads
    assert await get_bytes(st, f"{D}/cache/hit") == data
    assert client.downloads == n, "第二次读仍然走了 TG"
    assert len(blobs()) == 1, f"命中后又多出条目: {blobs()}"

async def t_cache_oversize(ctx):
    st, client = ctx.st, ctx.client
    cache_env(0.000005)                 # ≈5KB 上限
    wipe_cache()
    data = rand(20000)
    await put_bytes(st, data, f"{D}/cache/big")
    assert await get_bytes(st, f"{D}/cache/big") == data
    assert blobs() == [], f"比上限还大的文件不该缓存: {blobs()}"

async def t_cache_lru(ctx):
    st, client = ctx.st, ctx.client
    cache_env(0.0003)                   # ≈314KB 上限
    wipe_cache()
    names = ["a", "b", "c", "d"]
    datas = {n: rand(100000) for n in names}
    for n in names:
        await put_bytes(st, datas[n], f"{D}/cache/lru-{n}")
    for n in names:
        assert await get_bytes(st, f"{D}/cache/lru-{n}") == datas[n]
        await asyncio.sleep(0.02)       # 拉开 mtime，LRU 才有确定的顺序
    # 3×100KB 已缓存，读第 4 个要腾 100KB，只有最旧的 a 会被删
    kept = blobs()
    assert len(kept) == 3, f"淘汰后应剩 3 条，实际 {len(kept)}"
    e = st.get(f"{D}/cache/lru-a")
    assert S.cache_key(e) not in kept, "最旧的 a 没被淘汰"
    n = client.downloads
    assert await get_bytes(st, f"{D}/cache/lru-d") == datas["d"]   # d 还在，直接命中
    assert client.downloads == n
    assert await get_bytes(st, f"{D}/cache/lru-a") == datas["a"]   # a 被淘汰，重新下
    assert client.downloads > n

async def t_cache_invalidate_overwrite(ctx):
    st, client = ctx.st, ctx.client
    cache_env(1)
    wipe_cache()
    await put_bytes(st, b"v1-xxxx", f"{D}/cache/inv")
    assert await get_bytes(st, f"{D}/cache/inv") == b"v1-xxxx"
    assert len(blobs()) == 1
    await put_bytes(st, b"v2-yyyy", f"{D}/cache/inv")      # 重传 → 分片 id 变了
    n = client.downloads
    assert await get_bytes(st, f"{D}/cache/inv") == b"v2-yyyy", "覆盖后命中了旧缓存"
    assert client.downloads > n, "覆盖后应该重新下载，而不是命中旧内容"

async def t_cache_dedup_share(ctx):
    st, client = ctx.st, ctx.client
    cache_env(1)
    wipe_cache()
    data = rand(50000)
    await put_bytes(st, data, f"{D}/cache/d1")
    await put_bytes(st, data, f"{D}/cache/d2")             # 秒传，共用同一批分片
    assert await get_bytes(st, f"{D}/cache/d1") == data
    n = client.downloads
    assert await get_bytes(st, f"{D}/cache/d2") == data
    assert client.downloads == n, "同内容的另一个路径没命中同一份缓存"

async def t_cache_move_hit(ctx):
    st, client = ctx.st, ctx.client
    cache_env(1)
    wipe_cache()
    data = rand(20000)
    await put_bytes(st, data, f"{D}/cache/m1")
    assert await get_bytes(st, f"{D}/cache/m1") == data
    await st.move(f"{D}/cache/m1", f"{D}/cache/m2")        # move 只改路径，不动分片
    n = client.downloads
    assert await get_bytes(st, f"{D}/cache/m2") == data
    assert client.downloads == n, "move 之后没命中缓存"

async def t_cache_corrupt(ctx):
    st, client = ctx.st, ctx.client
    cache_env(1)
    wipe_cache()
    data = rand(80000)
    await put_bytes(st, data, f"{D}/cache/bad")
    assert await get_bytes(st, f"{D}/cache/bad") == data
    b = blobs()
    assert len(b) == 1
    with open(os.path.join(BLOBS, b[0]), "r+b") as f:
        f.truncate(1000)               # 模拟落盘途中被杀 / 盘坏
    n = client.downloads
    assert await get_bytes(st, f"{D}/cache/bad") == data, "残片没被丢弃重下"
    assert client.downloads > n, "残片应该触发重新下载"
    assert len(blobs()) == 1, "重下后应重新落一份"

async def t_cache_abort_no_leftover(ctx):
    st, client = ctx.st, ctx.client
    cache_env(1)
    wipe_cache()
    data = rand(300000)
    await put_bytes(st, data, f"{D}/cache/abort", slice_size=64 * 1024)
    g = st.iter_content(f"{D}/cache/abort")
    await g.__anext__()
    await g.aclose()                   # 客户端中途断开
    assert blobs() == [], f"中断后不该留缓存条目: {blobs()}"
    left = [n for n in os.listdir(BLOBS) if n.startswith(".dl-")] \
        if os.path.isdir(BLOBS) else []
    assert left == [], f"中断后临时文件残留: {left}"

async def t_cache_concurrent(ctx):
    st, client = ctx.st, ctx.client
    cache_env(1)
    wipe_cache()
    data = rand(400000)
    await put_bytes(st, data, f"{D}/cache/par", slice_size=64 * 1024)
    outs = await asyncio.gather(*[get_bytes(st, f"{D}/cache/par") for _ in range(6)])
    assert all(o == data for o in outs), "并发读的内容不一致"
    assert len(blobs()) == 1, f"并发读落下多个条目: {blobs()}"
    await asyncio.sleep(0.01)
    n = client.downloads
    assert await get_bytes(st, f"{D}/cache/par") == data
    assert client.downloads == n

async def t_cache_adopt_after_upload(ctx):
    st, client = ctx.st, ctx.client
    """上传完成后字节直接留成本地缓存：第一次读不该再回 TG 拉一遍。"""
    cache_env(1)
    wipe_cache()
    os.environ["TGND_CACHE_ADOPT"] = "1"
    cli, s2, up, c, srv = await make_dav()
    try:
        data = rand(300000)
        assert (await cli.put("/adopt/a.bin", data=data)).status == 201
        assert await wait_done(
            lambda: (s2.get("/adopt/a.bin") or SimpleNamespace(status=1)).status == 0), \
            "后台没传完"
        e = s2.get("/adopt/a.bin")
        p = os.path.join(S.CACHE_DIR, "blobs", S.cache_key(e))
        assert os.path.exists(p), "传完的字节没留成缓存"
        n = c.downloads
        r = await cli.get("/adopt/a.bin")
        assert await r.read() == data
        assert c.downloads == n, "本地已有缓存却又回 TG 下载了"
    finally:
        os.environ["TGND_CACHE_ADOPT"] = "0"
        cache_env(None)
        await stop_dav(cli, up)

async def t_cache_drop_on_delete(ctx):
    st, client = ctx.st, ctx.client
    """删掉文件后本地缓存要跟着回收，否则 CACHE_MAX_GB 被已经不存在的内容占着。"""
    cache_env(1)
    wipe_cache()
    os.environ["TGND_CACHE_ADOPT"] = "1"
    cli, s2, up, c, srv = await make_dav()
    try:
        data = rand(200000)
        assert (await cli.put("/adopt/b.bin", data=data)).status == 201
        assert await wait_done(
            lambda: (s2.get("/adopt/b.bin") or SimpleNamespace(status=1)).status == 0)
        e = s2.get("/adopt/b.bin")
        p = os.path.join(S.CACHE_DIR, "blobs", S.cache_key(e))
        assert os.path.exists(p), "传完的字节没留成缓存"
        assert (await cli.delete("/adopt/b.bin")).status == 204
        await s2.flush_dead()
        assert not os.path.exists(p), "删了文件缓存还占着"
    finally:
        os.environ["TGND_CACHE_ADOPT"] = "0"
        cache_env(None)
        await stop_dav(cli, up)

async def t_cache_shared_not_dropped(ctx):
    st, client = ctx.st, ctx.client
    """同内容两个路径（秒传共享）：删一个不能把另一个的缓存删了。"""
    cache_env(1)
    wipe_cache()
    os.environ["TGND_CACHE_ADOPT"] = "1"
    cli, s2, up, c, srv = await make_dav()
    try:
        data = rand(200000)
        assert (await cli.put("/adopt/c1", data=data)).status == 201
        assert await wait_done(
            lambda: (s2.get("/adopt/c1") or SimpleNamespace(status=1)).status == 0)
        assert (await cli.put("/adopt/c2", data=data)).status == 201
        e = s2.get("/adopt/c1")
        p = os.path.join(S.CACHE_DIR, "blobs", S.cache_key(e))
        assert (await cli.delete("/adopt/c1")).status == 204
        await s2.flush_dead()
        assert os.path.exists(p), "另一个路径还在引用，缓存不该删"
        n = c.downloads
        r = await cli.get("/adopt/c2")
        assert await r.read() == data
        assert c.downloads == n, "共享缓存没命中"
    finally:
        os.environ["TGND_CACHE_ADOPT"] = "0"
        cache_env(None)
        await stop_dav(cli, up)

async def t_cache_path_independent(ctx):
    st, client = ctx.st, ctx.client
    cache_env(1)
    wipe_cache()
    d1, d2 = rand(30000), rand(30000)
    await put_bytes(st, d1, f"{D}/cache/x1")
    await put_bytes(st, d2, f"{D}/cache/x2")
    assert await get_bytes(st, f"{D}/cache/x1") == d1
    assert await get_bytes(st, f"{D}/cache/x2") == d2
    assert len(blobs()) == 2, f"两个不同内容应各占一条: {blobs()}"
    assert await get_bytes(st, f"{D}/cache/x1") == d1     # 不会串到 x2 的缓存
    cache_env(None)                   # 收尾还原，别影响后面的用例

async def main(ctx):
    return [
        ("t_cache_default_off", t_cache_default_off),
        ("t_cache_hit", t_cache_hit),
        ("t_cache_oversize", t_cache_oversize),
        ("t_cache_lru", t_cache_lru),
        ("t_cache_invalidate_overwrite", t_cache_invalidate_overwrite),
        ("t_cache_dedup_share", t_cache_dedup_share),
        ("t_cache_move_hit", t_cache_move_hit),
        ("t_cache_corrupt", t_cache_corrupt),
        ("t_cache_abort_no_leftover", t_cache_abort_no_leftover),
        ("t_cache_concurrent", t_cache_concurrent),
        ("t_cache_adopt_after_upload", t_cache_adopt_after_upload),
        ("t_cache_drop_on_delete", t_cache_drop_on_delete),
        ("t_cache_shared_not_dropped", t_cache_shared_not_dropped),
        ("t_cache_path_independent", t_cache_path_independent),
    ]
