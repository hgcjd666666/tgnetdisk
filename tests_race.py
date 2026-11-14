"""锁、快照、多线程/多进程并发写。"""

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


async def t_lock_bootstrap(ctx):
    st, client = ctx.st, ctx.client
    if st.fast:
        print("SKIP  锁用例需要 --full（默认为 fast 模式，跳过分布式锁）")
        return
    # 空频道：没有快照消息时 acquire 要自己建一条空快照再占锁，
    # 且不能因 acquire → snapshot → acquire 递归而卡死。
    c2 = FakeClient()
    st2 = Store(c2, 1, backend=SqliteBackend(tempfile.mktemp(suffix=".db")))
    await st2.acquire(timeout=10)
    assert st2._holding, "acquire 返回了但没标记持有"
    snap = await st2._snap_msg()
    assert snap is not None, "抢锁后读不到快照消息"
    assert st2._cap(snap) == LOCK_PREFIX + st2.token, \
        f"caption 不是自己的 token: {st2._cap(snap)!r}"
    db_msgs = [m for m in c2._msgs.values() if is_db_msg(m)]
    assert len(db_msgs) == 1, f"建了 {len(db_msgs)} 条快照消息，应只建 1 条"
    await st2.release()
    assert st2._cap(await st2._snap_msg()) == LOCK_IDLE, "release 后 caption 不是 lock:null"

async def t_lock_reacquire(ctx):
    st, client = ctx.st, ctx.client
    if st.fast:
        return
    await st.acquire(timeout=10)
    assert st._holding
    await st.acquire(timeout=10)         # 已持有：幂等，不能再抢一次
    assert st._holding
    await st.release()
    assert not st._holding

async def t_lock_busy(ctx):
    st, client = ctx.st, ctx.client
    if st.fast:
        return
    # 另一个节点已经持锁时，nonblock 应立即抛 LockBusy 而不是傻等
    other = Store(client, st.chat, backend=SqliteBackend(tempfile.mktemp(suffix=".db")))
    other.token = "0" * 32               # 模拟另一台机器的 token
    await st.acquire(timeout=10)
    try:
        await other.acquire(timeout=2, nonblock=True)
        raise AssertionError("锁被别人持有时 nonblock 没抛 LockBusy")
    except LockBusy:
        pass
    finally:
        await st.release()
    await other.acquire(timeout=10)      # 放锁后应能抢到
    assert other._holding
    await other.release()                # 别把锁留给后面的用例

async def t_snapshot_dirty_clean(ctx):
    st, client = ctx.st, ctx.client
    # 写入后本地 dirty=1，但 dump 出来的快照副本里必须是 dirty=0
    await put_bytes(st, rand(8), "/snap/a.bin")
    assert st.dirty, "写入后本地 dirty 应为 1"
    fd, xz = tempfile.mkstemp(suffix=".xz")
    os.close(fd)
    fd, db = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        st.backend.dump(xz)
        with lzma.open(xz) as f:
            with open(db, "wb") as o:
                o.write(f.read())
        c = sqlite3.connect(db)
        row = c.execute("select v from meta where k='dirty'").fetchone()
        c.close()
        assert row == ("0",), f"快照副本里 dirty 应为 0，实际 {row}"
        assert st.dirty, "dump 不应改动本地 dirty"
    finally:
        os.unlink(xz)
        os.unlink(db)

async def t_race_backend_writers(ctx):
    st, client = ctx.st, ctx.client
    import threading
    b = SqliteBackend(tempfile.mktemp(suffix=".db"))
    errs = []
    def w(i):
        try:
            for j in range(50):
                b.set_file(f"/race/{i}/{j}", f"h{i}-{j}", 100, 0, {0: i * 100 + j}, 0, 1)
        except Exception as e:
            errs.append(repr(e))
    ts = [threading.Thread(target=w, args=(i,)) for i in range(8)]
    for t in ts: t.start()
    for t in ts: t.join()
    n = len(b.all_entries())
    assert not errs, f"并发写抛异常: {errs[:3]}"
    assert n == 8 * 50 + 1, f"记录数 {n} != 401"

async def t_race_backend_same_path(ctx):
    st, client = ctx.st, ctx.client
    import threading
    b = SqliteBackend(tempfile.mktemp(suffix=".db"))
    errs = []
    def w(i):
        try:
            for j in range(100):
                b.set_file("/race/same", f"v{i}-{j}", 1000 + j, 0, {0: i}, 0, 1)
        except Exception as e:
            errs.append(repr(e))
    ts = [threading.Thread(target=w, args=(i,)) for i in range(8)]
    for t in ts: t.start()
    for t in ts: t.join()
    e = b.get("/race/same")
    assert not errs, f"并发写抛异常: {errs[:3]}"
    assert e is not None and e.hash.startswith("v") and e.size >= 1000, f"最终记录不完整: {e}"

async def t_race_restore_storm(ctx):
    st, client = ctx.st, ctx.client
    import threading, time
    b = SqliteBackend(tempfile.mktemp(suffix=".db"))
    b.set_file("/storm/base", "h", 1, 0, {}, 0, 0)
    snap = tempfile.mktemp(suffix=".xz")
    b.dump(snap)
    stop = threading.Event()
    errs = []
    def restore_loop():
        try:
            while not stop.is_set():
                b.restore(snap)
        except Exception as e:
            errs.append(("restore", repr(e)))
    def rw_loop():
        try:
            while not stop.is_set():
                b.get("/storm/base")
                b.set_file("/storm/live", "x", 1, 0, {}, 0, 0)
                b.all_entries()
        except Exception as e:
            errs.append(("rw", repr(e)))
    t1 = threading.Thread(target=restore_loop)
    t2 = threading.Thread(target=rw_loop)
    t1.start(); t2.start()
    time.sleep(1.2)
    stop.set(); t1.join(); t2.join()
    os.unlink(snap)
    assert not errs, f"restore 风暴下出错: {errs[:3]}"

async def t_race_dump_while_write(ctx):
    st, client = ctx.st, ctx.client
    import threading, time
    b = SqliteBackend(tempfile.mktemp(suffix=".db"))
    stop = threading.Event()
    errs = []
    def writer():
        i = 0
        while not stop.is_set():
            try:
                b.set_file(f"/dumprace/{i % 10}", f"h{i}", i, 0, {}, 0, 0)
            except Exception as e:
                errs.append(repr(e)); return
            i += 1
    t = threading.Thread(target=writer)
    t.start()
    try:
        for _ in range(8):
            dst = tempfile.mktemp(suffix=".xz")
            tmp = tempfile.mktemp(suffix=".db")
            b.dump(dst)
            with lzma.open(dst) as f, open(tmp, "wb") as o:
                o.write(f.read())
            c = sqlite3.connect(tmp)
            assert c.execute("PRAGMA quick_check").fetchone()[0] == "ok", "dump 出来的库校验失败"
            c.close()
            os.unlink(dst); os.unlink(tmp)
    finally:
        stop.set(); t.join()
    assert not errs, f"并发写 dump 报错: {errs[:3]}"

async def t_race_lock_contend(ctx):
    st, client = ctx.st, ctx.client
    if st._holding:                  # 前面的用例可能还在租约里，先把锁让出来
        await st.release()
    stores = []
    for i in range(3):
        o = Store(client, st.chat, backend=SqliteBackend(tempfile.mktemp(suffix=".db")))
        o.token = f"{i + 1:032x}"
        stores.append(o)
    got = []
    async def try_acq(o):
        try:
            await o.acquire(timeout=2, nonblock=True)
            got.append(o)
        except LockBusy:
            pass
    await asyncio.gather(*[try_acq(o) for o in stores])
    try:
        assert len(got) == 1, f"3 个节点抢锁，成功数应为 1，实际 {len(got)}"
    finally:
        for o in got:
            try:
                await o.release()
            except Exception:
                pass

async def t_race_put_dedup(ctx):
    st, client = ctx.st, ctx.client
    data = rand(200000)
    async def one(i):
        await put_bytes(st, data, f"{D}/race/dedup{i}")
    await asyncio.gather(*[one(i) for i in range(8)])
    groups = set()
    for i in range(8):
        e = st.get(f"{D}/race/dedup{i}")
        assert e and e.status == 0, f"第 {i} 个没完成: {e}"
        groups.add(tuple(sorted(e.parts.items())))
    assert len(groups) == 1, f"同一内容并发上传应只产生一组消息，实际 {len(groups)} 组"

async def t_race_delete_read(ctx):
    st, client = ctx.st, ctx.client
    await put_bytes(st, rand(1000), f"{D}/race/delread")
    async def deleter():
        for _ in range(20):
            await st.delete(f"{D}/race/delread")
            await put_bytes(st, rand(1000), f"{D}/race/delread")
    async def reader():
        for _ in range(50):
            try:
                async for _ in st.iter_content(f"{D}/race/delread"):
                    break
            except Exception:
                pass
    await asyncio.gather(deleter(), reader())

async def t_race_process_coexist(ctx):
    st, client = ctx.st, ctx.client
    import subprocess
    db = tempfile.mktemp(suffix=".db")
    here = os.path.dirname(os.path.abspath(__file__))
    code = (
        "import sys; sys.path.insert(0, %r)\n"
        "from store import SqliteBackend\n"
        "b = SqliteBackend(%r)\n"
        "for i in range(200):\n"
        "    b.set_file('/proc/%%d' %% i, 'h%%d' %% i, i, 0, {}, 0, 0)\n"
        "print('child done')\n"
    ) % (here, db)
    p = subprocess.Popen([sys.executable, "-c", code],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    b = SqliteBackend(db)
    for i in range(200):
        b.set_file(f"/main/{i}", f"m{i}", i, 0, {}, 0, 0)
    out, err = p.communicate(timeout=120)
    assert p.returncode == 0, f"子进程失败: {err.decode()[-300:]}"
    n = len(b.all_entries())
    assert n == 401, f"两个进程各写 200 条，应有 401 条（含根），实际 {n}"

async def t_race_put_many_different(ctx):
    st, client = ctx.st, ctx.client
    async def one(i):
        await put_bytes(st, rand(50000), f"{D}/race/many{i}")
    await asyncio.gather(*[one(i) for i in range(10)])
    for i in range(10):
        e = st.get(f"{D}/race/many{i}")
        assert e and e.status == 0, f"第 {i} 个未完成"
    assert await get_bytes(st, f"{D}/race/many9") is not None

async def t_race_put_same_path(ctx):
    st, client = ctx.st, ctx.client
    data = [rand(30000) for _ in range(6)]
    async def one(i):
        await put_bytes(st, data[i], f"{D}/race/samepath")
    await asyncio.gather(*[one(i) for i in range(6)])
    e = st.get(f"{D}/race/samepath")
    assert e and e.status == 0, f"最终记录异常: {e}"
    got = await get_bytes(st, f"{D}/race/samepath")
    assert len(got) == 30000, f"内容长度 {len(got)}"

async def t_race_write_read_content(ctx):
    st, client = ctx.st, ctx.client
    p = f"{D}/race/wr"
    await put_bytes(st, rand(100000), p)
    errs = []
    async def writer():
        for _ in range(10):
            try:
                await put_bytes(st, rand(100000), p)
            except Exception as e:
                errs.append(("w", repr(e)))
    async def reader():
        for _ in range(30):
            try:
                async for _ in st.iter_content(p):
                    break
            except Exception:
                pass
    await asyncio.gather(writer(), reader())
    assert not errs, f"边写边读出错: {errs[:3]}"

async def t_race_snapshot_while_write(ctx):
    st, client = ctx.st, ctx.client
    errs = []
    async def writer():
        for i in range(20):
            try:
                await put_bytes(st, rand(1000), f"{D}/race/snap{i}")
            except Exception as e:
                errs.append(("w", repr(e)))
            await asyncio.sleep(0)
    async def snapper():
        for _ in range(10):
            try:
                await st.snapshot()
            except Exception as e:
                errs.append(("s", repr(e)))
            await asyncio.sleep(0)
    await asyncio.gather(writer(), snapper())
    assert not errs, f"推送中写入出错: {errs[:3]}"

async def t_race_mv_cp(ctx):
    st, client = ctx.st, ctx.client
    a = f"{D}/race/mv/a"
    b = f"{D}/race/mv/b"
    await put_bytes(st, rand(1000), a)
    async def mover():
        for _ in range(10):
            try:
                await st.move(a, b)
                await st.move(b, a)
            except Exception:
                pass
    async def copier():
        for _ in range(10):
            try:
                await st.copy(a, b)
            except Exception:
                pass
            try:
                await st.delete(b)
            except Exception:
                pass
    await asyncio.gather(mover(), copier())

async def t_race_kill_recovery(ctx):
    st, client = ctx.st, ctx.client
    import subprocess, time
    db = tempfile.mktemp(suffix=".db")
    here = os.path.dirname(os.path.abspath(__file__))
    code = (
        "import sys; sys.path.insert(0, %r)\n"
        "from store import SqliteBackend\n"
        "b = SqliteBackend(%r)\n"
        "b.set_file('/kill/first', 'h', 1, 0, {}, 0, 0)\n"
        "print('ready', flush=True)\n"
        "i = 0\n"
        "while True:\n"
        "    b.set_file('/kill/%%d' %% i, 'h', 1, 0, {}, 0, 0)\n"
        "    i += 1\n"
    ) % (here, db)
    p = subprocess.Popen([sys.executable, "-c", code],
                         stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    p.stdout.readline()          # 等它至少写完一条再杀，避免机器慢时杀在启动阶段
    time.sleep(0.1)
    p.kill()
    p.wait()
    b = SqliteBackend(db)
    n = len(b.all_entries())
    c = sqlite3.connect(db)
    ok = c.execute("PRAGMA quick_check").fetchone()[0]
    c.close()
    assert ok == "ok", f"被 kill 后库损坏: {ok}"
    assert n >= 2, f"少量写入应保留，实际 {n}"

async def main(ctx):
    return [
        ("t_lock_bootstrap", t_lock_bootstrap),
        ("t_lock_reacquire", t_lock_reacquire),
        ("t_lock_busy", t_lock_busy),
        ("t_snapshot_dirty_clean", t_snapshot_dirty_clean),
        ("t_race_backend_writers", t_race_backend_writers),
        ("t_race_backend_same_path", t_race_backend_same_path),
        ("t_race_restore_storm", t_race_restore_storm),
        ("t_race_dump_while_write", t_race_dump_while_write),
        ("t_race_lock_contend", t_race_lock_contend),
        ("t_race_put_dedup", t_race_put_dedup),
        ("t_race_delete_read", t_race_delete_read),
        ("t_race_process_coexist", t_race_process_coexist),
        ("t_race_put_many_different", t_race_put_many_different),
        ("t_race_put_same_path", t_race_put_same_path),
        ("t_race_write_read_content", t_race_write_read_content),
        ("t_race_snapshot_while_write", t_race_snapshot_while_write),
        ("t_race_mv_cp", t_race_mv_cp),
        ("t_race_kill_recovery", t_race_kill_recovery),
    ]
