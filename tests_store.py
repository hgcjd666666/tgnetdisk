"""基础读写、路径规范化、秒传、删除、move/copy、lost+found。"""

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


async def t_empty(ctx):
    st, client = ctx.st, ctx.client
    await put_bytes(st, b"", f"{D}/empty")
    assert await get_bytes(st, f"{D}/empty") == b"", "空文件读取不为空"

async def t_one_byte(ctx):
    st, client = ctx.st, ctx.client
    await put_bytes(st, b"x", f"{D}/one")
    assert await get_bytes(st, f"{D}/one") == b"x"

async def t_unicode(ctx):
    st, client = ctx.st, ctx.client
    data = rand(3000)
    await put_bytes(st, data, f"{D}/中文 目录/音频 测试.mp3")
    assert await get_bytes(st, f"{D}/中文 目录/音频 测试.mp3") == data

async def t_special(ctx):
    st, client = ctx.st, ctx.client
    data = rand(3000)
    await put_bytes(st, data, f"{D}/a b(1)#?&.txt")
    assert await get_bytes(st, f"{D}/a b(1)#?&.txt") == data

async def t_deep(ctx):
    st, client = ctx.st, ctx.client
    await put_bytes(st, b"x", f"{D}/d1/d2/d3/d4/deep")
    assert await get_bytes(st, f"{D}/d1/d2/d3/d4/deep") == b"x"

async def t_multislice(ctx):
    st, client = ctx.st, ctx.client
    data = rand(400000)
    await put_bytes(st, data, f"{D}/multi", slice_size=64 * 1024)
    e = st.get(f"{D}/multi")
    assert len(e.parts) == 7, f"分片数 {len(e.parts)} != 7"
    assert await get_bytes(st, f"{D}/multi") == data, "内容不一致"

async def t_dedup(ctx):
    st, client = ctx.st, ctx.client
    data = rand(50000)
    await put_bytes(st, data, f"{D}/dup1")
    await put_bytes(st, data, f"{D}/dup2")
    a, b = st.get(f"{D}/dup1"), st.get(f"{D}/dup2")
    assert a.parts == b.parts, f"没共享分片 {a.parts} vs {b.parts}"
    assert await get_bytes(st, f"{D}/dup2") == data

async def t_overwrite(ctx):
    st, client = ctx.st, ctx.client
    await put_bytes(st, b"v1", f"{D}/over")
    await put_bytes(st, b"v2", f"{D}/over")
    assert await get_bytes(st, f"{D}/over") == b"v2", "覆盖后不是新内容"

async def t_copy_move(ctx):
    st, client = ctx.st, ctx.client
    await put_bytes(st, rand(1000), f"{D}/src")
    await st.copy(f"{D}/src", f"{D}/cp")
    await st.move(f"{D}/cp", f"{D}/mv")
    assert st.get(f"{D}/mv") is not None and st.get(f"{D}/cp") is None
    assert await get_bytes(st, f"{D}/mv") == await get_bytes(st, f"{D}/src")

async def t_delete(ctx):
    st, client = ctx.st, ctx.client
    await put_bytes(st, rand(1000), f"{D}/del")
    await st.delete(f"{D}/del")
    assert st.get(f"{D}/del") is None
    try:
        await get_bytes(st, f"{D}/del")
        raise AssertionError("删了还能读")
    except FileNotFoundError:
        pass

async def t_rmdir(ctx):
    st, client = ctx.st, ctx.client
    await put_bytes(st, b"x", f"{D}/dir/a")
    await put_bytes(st, b"y", f"{D}/dir/sub/b")
    await st.delete(f"{D}/dir")
    assert st.get(f"{D}/dir/sub/b") is None, "子文件没删掉"

async def t_mkdir(ctx):
    st, client = ctx.st, ctx.client
    await st.mkdir(f"{D}/mdir")
    assert st.get(f"{D}/mdir") is not None and st.get(f"{D}/mdir").is_dir
    assert st.children(f"{D}/mdir") == []

async def t_reindex(ctx):
    st, client = ctx.st, ctx.client
    before = st.get(f"{D}/one")
    await st.sync(full=True)
    after = st.get(f"{D}/one")
    assert (before is None) == (after is None)

async def t_emoji_path(ctx):
    st, client = ctx.st, ctx.client
    data = rand(2000)
    p = f"{D}/🎵 emoji/😀 笑脸.bin"
    await put_bytes(st, data, p)
    assert st.get(p) is not None, "emoji 路径没登记进索引"
    kids = [e.path for e in st.children(f"{D}/🎵 emoji")]
    assert p in kids, f"children 漏掉 emoji 文件: {kids}"
    assert await get_bytes(st, p) == data, "emoji 文件内容不一致"
    await st.delete(f"{D}/🎵 emoji")
    assert st.get(p) is None, "递归删目录没删掉 emoji 文件"

async def t_move_guard(ctx):
    st, client = ctx.st, ctx.client
    await put_bytes(st, b"v", f"{D}/mvsrc/f")
    await st.move(f"{D}/mvsrc", f"{D}/mvsrc")        # 移到自身：应当是空操作
    assert st.get(f"{D}/mvsrc/f") is not None, "move 到自身把数据删光了"
    try:
        await st.move(f"{D}/mvsrc", f"{D}/mvsrc/sub")
        raise AssertionError("move 到自己的子目录应被拒绝")
    except OSError:
        pass
    assert st.get(f"{D}/mvsrc/f") is not None, "被拒绝的 move 改了数据"
    await st.delete(f"{D}/mvsrc")

async def t_manual_to_lostfound(ctx):
    st, client = ctx.st, ctx.client
    if "--real" in sys.argv:
        print("SKIP  手动上传用例只跑内存版（会往真频道发消息）")
        return
    m = await client.send_file(1, b"manual-bytes",
                               attributes=[SimpleNamespace(file_name="manual-photo.jpg")])
    await st.sync(full=True)
    p = "/lost+found/manual-photo.jpg"
    assert st.get(p) is not None, "手动上传没归入 /lost+found"
    assert m.id in client._msgs, "归入 lost+found 后消息不见了"
    await st.delete(p)

async def t_part_list_format(ctx):
    st, client = ctx.st, ctx.client
    if "--real" in sys.argv:
        print("SKIP  历史分片格式用例只跑内存版（会往索引里写伪造记录）")
        return
    body = b"legacy-bytes"
    m = await client.send_file(
        1, body, attributes=[SimpleNamespace(file_name="a" * 32 + ".0")])
    p = f"{D}/legacy"
    # 历史数据里 parts 的值是 [msg_id, size] 而不是 msg_id
    st.backend.set_file(p, "a" * 32, len(body), 1, {0: [m.id, len(body)]})
    out = b""
    async for piece in st.iter_content(p, verify=False):
        out += piece
    assert out == body, f"list 格式分片读不出来: {out!r}"
    await st.delete(p)
    await st.flush_dead()
    assert m.id not in client._msgs, "list 格式删除没删掉 TG 消息"

async def t_missing_slice(ctx):
    st, client = ctx.st, ctx.client
    await put_bytes(st, b"0123456789", f"{D}/broken")
    e = st.get(f"{D}/broken")
    st.backend.set_file(f"{D}/broken", e.hash, e.size, e.mtime, {0: 999999}, 0, 1)
    try:
        await get_bytes(st, f"{D}/broken")
        raise AssertionError("缺分片时下载没有报错（会静默交付截断文件）")
    except IOError:
        pass
    st.backend.remove(f"{D}/broken")

async def t_norm_dotdot(ctx):
    st, client = ctx.st, ctx.client
    assert norm("/a/../b") == "/b", "norm 没消解 .."
    assert norm("/a/./b/") == "/a/b"
    assert norm("/../x") == "/x"
    await put_bytes(st, b"shadow", f"{D}/normdir/../normfile")
    assert st.get(f"{D}/normfile") is not None, ".. 路径没被规范化，产生影子条目"
    kids = [e.path for e in st.children(D)]
    assert f"{D}/normfile" in kids, f"规范化后的文件不在 children 里: {kids}"
    await st.delete(f"{D}/normfile")

async def t_overwrite_fail_keeps_old(ctx):
    st, client = ctx.st, ctx.client
    await put_bytes(st, b"old-content", f"{D}/keep")
    orig = st._send_slices

    async def boom(*a, **k):
        raise ConnectionError("模拟断网")

    st._send_slices = boom
    try:
        await put_bytes(st, b"new-content", f"{D}/keep")
        raise AssertionError("断网时覆盖上传居然成功了")
    except ConnectionError:
        pass
    finally:
        st._send_slices = orig
    e = st.get(f"{D}/keep")
    assert e is not None and e.status == 0, f"覆盖失败后旧记录没恢复: {e}"
    assert await get_bytes(st, f"{D}/keep") == b"old-content", "覆盖失败把旧内容弄丢了"
    await st.delete(f"{D}/keep")

async def t_parent_is_file(ctx):
    st, client = ctx.st, ctx.client
    await put_bytes(st, b"f", f"{D}/pfile")
    try:
        await put_bytes(st, b"c", f"{D}/pfile/child")
        raise AssertionError("父路径是文件时不该能登记子项")
    except FileExistsError:
        pass
    p = st.get(f"{D}/pfile")
    assert p is not None and not p.is_dir, "父文件被改成目录了"
    assert st.get(f"{D}/pfile/child") is None, "仍然在文件下面造出了子条目"
    await st.delete(f"{D}/pfile")

async def t_append_total_size(ctx):
    st, client = ctx.st, ctx.client
    await put_bytes(st, b"0123456789", f"{D}/ap")
    fd, tmp = tempfile.mkstemp()
    with os.fdopen(fd, "wb") as f:
        f.write(b"XY")
    try:
        e = await st.put_file(f"{D}/ap", tmp, append=True, start_index=1, total_size=12)
    finally:
        os.unlink(tmp)
    assert e.size == 12, f"append 后记录大小应为 12，实际 {e.size}"
    out = b""
    async for piece in st.iter_content(f"{D}/ap", verify=False):
        out += piece
    assert out == b"0123456789XY", f"append 内容不对: {out!r}"
    await st.delete(f"{D}/ap")

async def t_many_slices(ctx):
    st, client = ctx.st, ctx.client
    data = rand(12 * 1024 + 7)          # 13 片，跨过 "10" 的字符串排序坑
    await put_bytes(st, data, f"{D}/many", slice_size=1024)
    e = st.get(f"{D}/many")
    assert len(e.parts) == 13, f"分片数 {len(e.parts)} != 13"
    assert sorted(e.parts) == list(range(13)), f"parts 的 key 不是 int: {sorted(e.parts)[:5]}"
    assert await get_bytes(st, f"{D}/many") == data, "多分片下载内容顺序错乱"
    await st.delete(f"{D}/many")

async def t_lostfound_not_dedup_source(ctx):
    st, client = ctx.st, ctx.client
    body = rand(3000)
    fd, tmp = tempfile.mkstemp()
    with os.fdopen(fd, "wb") as f:
        f.write(body)
    dig = digest_of(tmp)
    os.unlink(tmp)
    # 模拟「上传中断的残片被 reindex 收进 /lost+found」
    m = await client.send_file(1, body,
                               attributes=[SimpleNamespace(file_name=dig + ".0")])
    st.backend.set_dir("/lost+found", 1)
    p_lf = "/lost+found/" + dig
    st.backend.set_file(p_lf, dig, len(body), 1, {0: m.id}, 0, 1)
    await put_bytes(st, body, f"{D}/after-rescue")
    e = st.get(f"{D}/after-rescue")
    # 分片级复用允许用 lost+found 的片，但记录大小必须按这次的文件算，
    # 不能继承残片记录的 size（那是分片字节和，缺片时会偏小）
    assert e.size == len(body), f"记录 size 错了: {e.size}"
    assert await get_bytes(st, f"{D}/after-rescue") == body
    await st.delete(p_lf)
    await st.flush_dead()
    # 分片被 after-rescue 复用了，删 lost+found 不能把消息删掉
    assert m.id in client._msgs, "被真实文件引用的分片被删了"
    assert await get_bytes(st, f"{D}/after-rescue") == body, "删 lost+found 影响了真实文件"
    await st.delete(f"{D}/after-rescue")
    await st.flush_dead()
    assert m.id not in client._msgs, "最后一个引用删了消息还留着"

async def t_refcount_dedup(ctx):
    st, client = ctx.st, ctx.client
    data = rand(4000)
    await put_bytes(st, data, f"{D}/rc1")
    await put_bytes(st, data, f"{D}/rc2")
    a, b = st.get(f"{D}/rc1"), st.get(f"{D}/rc2")
    assert a.parts == b.parts, "秒传没共享分片"
    mids = set(a.parts.values())
    await st.delete(f"{D}/rc2")
    assert all(mid in client._msgs for mid in mids), "删一个引用就把共享消息删了"
    await st.delete(f"{D}/rc1")
    await st.flush_dead()
    assert all(mid not in client._msgs for mid in mids), "最后一个引用删了消息还留着"

async def t_move_ancestor(ctx):
    st, client = ctx.st, ctx.client
    await put_bytes(st, b"v", f"{D}/anc/sub/f")
    for dst in (f"{D}/anc", f"{D}/anc/sub/..", "/"):
        try:
            await st.move(f"{D}/anc/sub", dst)
            raise AssertionError(f"move 到祖先 {dst} 应被拒绝")
        except OSError:
            pass
    assert st.get(f"{D}/anc/sub/f") is not None, "move 被拒绝但数据没了"
    await st.delete(f"{D}/anc")

async def t_manual_named_snapshot(ctx):
    st, client = ctx.st, ctx.client
    if "--real" in sys.argv:
        print("SKIP  同名快照用例只跑内存版")
        return
    await client.send_file(1, b"not-a-db",
                           attributes=[SimpleNamespace(file_name="tgnd.db.xz")])
    await st.sync(full=True)
    p = "/lost+found/tgnd.db.xz"
    assert st.get(p) is not None, "手动上传的同名文件被当成快照丢掉了"
    await st.delete(p)

async def t_foreign_part_msg(ctx):
    st, client = ctx.st, ctx.client
    if "--real" in sys.argv:
        print("SKIP  外来分片用例只跑内存版")
        return
    body = rand(2000)
    await put_bytes(st, body, f"{D}/real")
    e = st.get(f"{D}/real")
    await client.send_file(1, b"junk",
                           attributes=[SimpleNamespace(file_name=e.hash + ".0")])
    await st.sync(full=True)
    assert await get_bytes(st, f"{D}/real") == body, "同名外来消息覆盖了真实记录"
    await st.delete(f"{D}/real")

async def t_ring_long_line(ctx):
    st, client = ctx.st, ctx.client
    import logger as lg
    d = tempfile.mkdtemp()
    p = os.path.join(d, "ring.log")
    r = lg.RingLog(p, 256)
    r.write("Z" * 400)                      # 单行比整个正文还长
    txt = lg.read_log(p)
    assert "Z" * 200 in txt, f"超长行被整条切掉了: {txt!r}"

async def main(ctx):
    return [
        ("t_empty", t_empty),
        ("t_one_byte", t_one_byte),
        ("t_unicode", t_unicode),
        ("t_special", t_special),
        ("t_deep", t_deep),
        ("t_multislice", t_multislice),
        ("t_dedup", t_dedup),
        ("t_overwrite", t_overwrite),
        ("t_copy_move", t_copy_move),
        ("t_delete", t_delete),
        ("t_rmdir", t_rmdir),
        ("t_mkdir", t_mkdir),
        ("t_reindex", t_reindex),
        ("t_emoji_path", t_emoji_path),
        ("t_move_guard", t_move_guard),
        ("t_manual_to_lostfound", t_manual_to_lostfound),
        ("t_part_list_format", t_part_list_format),
        ("t_missing_slice", t_missing_slice),
        ("t_norm_dotdot", t_norm_dotdot),
        ("t_overwrite_fail_keeps_old", t_overwrite_fail_keeps_old),
        ("t_parent_is_file", t_parent_is_file),
        ("t_append_total_size", t_append_total_size),
        ("t_many_slices", t_many_slices),
        ("t_lostfound_not_dedup_source", t_lostfound_not_dedup_source),
        ("t_refcount_dedup", t_refcount_dedup),
        ("t_move_ancestor", t_move_ancestor),
        ("t_manual_named_snapshot", t_manual_named_snapshot),
        ("t_foreign_part_msg", t_foreign_part_msg),
        ("t_ring_long_line", t_ring_long_line),
    ]
