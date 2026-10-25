#!/usr/bin/env python3
"""tgnd —— 直接调 Telegram 的网盘命令行（不走 WebDAV，只依赖 store.py）。

用法:
  tgnd ls [路径]
  tgnd ll [路径]            带大小/时间
  tgnd mkdir <路径>
  tgnd pushdir [--no-skip] [--lease 3] [-x 排除模式] <本地目录> <远端根>   批量上传，全程持锁
  tgnd put [--slice 1900M] [--offset N] [--length N] <本地文件...> <远端路径>
  tgnd get <远端路径> [本地文件]
  tgnd rm <路径...>
  tgnd mv <源> <目标>
  tgnd cp <源> <目标>
  tgnd stat <路径>
  tgnd gc [--dry-run]       清理 TG 上没人引用的消息 + 索引里指向已删消息的记录
  tgnd export-sql [文件.sql]     导出索引为 SQL 文本
  tgnd import-sql <文件.sql>     从 SQL 文本重建索引
  tgnd reindex              全量扫聊天记录重建索引（正常不需要）

环境变量（默认读 /opt/tgnd/env）:
  TG_API_ID, TG_API_HASH, TG_SESSION, TG_CHAT
"""
import asyncio
import logging
import os
import shutil
import sqlite3
import sys
import tempfile
import time
from logger import log, progress, done

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from store import (Store, LockBusy, norm, base_of, make_session,
                   is_path_session, digest_of, is_online, DB_PATH)  # noqa: E402


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")


def load_env():
    if os.environ.get("TG_API_ID"):
        return
    here = os.path.dirname(os.path.abspath(__file__))
    for path in [os.environ.get("TGND_ENV", ""), os.path.join(here, "env"),
                 os.path.join(here, ".env"), "/opt/tgnd/env"]:
        if path and os.path.exists(path):
            for line in open(path):
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())
            os.environ.setdefault("TG_SESSION", os.path.join(here, "session.session"))
            return


def build_proxy():
    """TG_PROXY=http://127.0.0.1:2080 或 socks5://host:port"""
    p = os.environ.get("TG_PROXY", "").strip()
    if not p:
        return None
    try:
        from python_socks import ProxyType
    except ImportError:
        sys.exit("要代理请先安装 python-socks: uv pip install python-socks")
    scheme, rest = (p.split("://", 1) if "://" in p else ("http", p))
    host, _, port = rest.rpartition(":")
    host = host or "127.0.0.1"
    t = {"http": ProxyType.HTTP, "socks5": ProxyType.SOCKS5,
         "socks4": ProxyType.SOCKS4}.get(scheme.lower(), ProxyType.HTTP)
    return (t, host, int(port))


def human(n):
    for u in ("B", "K", "M", "G", "T"):
        if n < 1024:
            return f"{n:.0f}{u}" if u == "B" else f"{n:.1f}{u}"
        n /= 1024
    return f"{n:.1f}P"


def copy_session(src):
    """服务在跑时 session 有写锁，复制一份给 CLI 用。StringSession 无需复制。"""
    if not is_path_session(src):
        return None
    dst = tempfile.mktemp(prefix="tgnd_cli_", suffix=".session")
    s = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
    d = sqlite3.connect(dst)
    s.backup(d)
    d.close()
    s.close()
    return dst


async def get_store():
    from telethon import TelegramClient
    load_env()
    src = os.environ["TG_SESSION"]
    tmp = copy_session(src)
    client = TelegramClient(tmp or make_session(src), int(os.environ["TG_API_ID"]), os.environ["TG_API_HASH"],
                            proxy=build_proxy(), device_model=os.environ.get("TG_DEVICE", "tgnd"),
                            system_version="Linux", app_version="tgnd")
    await client.connect()
    if not await client.is_user_authorized():
        sys.exit("session 未授权")
    chat = os.environ["TG_CHAT"]
    chat = int(chat) if chat.lstrip("-").isdigit() else chat
    st = Store(client, chat)
    await st.load()
    return client, st, tmp


def cleanup(tmp):
    if not tmp:
        return
    try:
        os.unlink(tmp)
    except OSError:
        pass


def cmd_ls(st, args, long=False):
    path = norm(args[0] if args else "/")
    e = st.get(path)
    if e is None:
        sys.exit(f"不存在: {path}")
    if not e.is_dir:
        log(f"{e.name}\t{human(e.size)}")
        return
    for c in st.children(path):
        if long:
            name = c.name + ("/" if c.is_dir else "")
            log(f"{human(c.size) if not c.is_dir else '-':>8}\t{name}")
        else:
            log(c.name + ("/" if c.is_dir else ""))


def parse_size(v: str) -> int:
    v = v.strip().upper()
    mult = 1
    if v.endswith("K"):
        mult, v = 1024, v[:-1]
    elif v.endswith("M"):
        mult, v = 1024 ** 2, v[:-1]
    elif v.endswith("G"):
        mult, v = 1024 ** 3, v[:-1]
    elif v.endswith("T"):
        mult, v = 1024 ** 4, v[:-1]
    return int(float(v) * mult)


def _collect(srcs, remote, into=False):
    """把命令行给的文件/目录展开成 [(本地文件, 远端路径)]，目录保留相对结构。"""
    jobs = []
    multi = len(srcs) > 1 or remote.endswith("/")
    for s in srcs:
        if os.path.isdir(s):
            sabs = os.path.abspath(s)
            base = sabs if into else os.path.dirname(sabs)
            for dp, _, ns in os.walk(sabs):
                for n in sorted(ns):
                    full = os.path.join(dp, n)
                    rel = os.path.relpath(full, base).replace(os.sep, "/")
                    jobs.append((full, remote.rstrip("/") + "/" + rel))
        else:
            if not os.path.isfile(s):
                sys.exit(f"本地文件不存在: {s}")
            dst = (remote.rstrip("/") + "/" + os.path.basename(s)) if multi else remote
            jobs.append((s, dst))
    return jobs


async def cmd_put(st, args):
    slice_size = offset = length = None
    skip, dry, rest = True, False, []
    i = 0
    while i < len(args):
        a = args[i]
        if a in ("--slice", "-s"):
            slice_size = parse_size(args[i + 1]); i += 2
        elif a in ("--offset", "-o"):
            offset = parse_size(args[i + 1]); i += 2
        elif a in ("--length", "-l"):
            length = parse_size(args[i + 1]); i += 2
        elif a == "--no-skip":
            skip = False; i += 1
        elif a in ("--dry-run", "-n"):
            dry = True; i += 1
        else:
            rest.append(a); i += 1
    if len(rest) < 2:
        sys.exit("用法: tgnd put [--slice 1900M] [--no-skip] [--dry-run] <本地文件或目录...> <远端路径>")
    remote = rest[-1]
    if not remote.startswith("/"):
        sys.exit("远端路径要以 / 开头")
    jobs = _collect(rest[:-1], remote)
    if not jobs:
        sys.exit("没有可上传的文件")
    total = len(jobs)
    if total > 1 or rest[:-1] != [jobs[0][0]]:
        log(f"共 {total} 个文件待处理")

    ok = skipped = failed = 0
    for idx, (f, dst) in enumerate(jobs, 1):
        tag = f"[{idx}/{total}]"
        try:
            if offset is not None or length is not None:
                off = offset or 0
                fd, tmp = tempfile.mkstemp(prefix="tgnd_rng_")
                with open(fd, "wb") as out, open(f, "rb") as src:
                    src.seek(off)
                    left = length if length is not None else os.path.getsize(f) - off
                    while left > 0:
                        b = src.read(min(1 << 20, left))
                        if not b:
                            break
                        out.write(b)
                        left -= len(b)
                try:
                    si = (off // slice_size) if slice_size else None
                    e = await st.put_file(dst, tmp, slice_size=slice_size, append=True, start_index=si)
                finally:
                    try:
                        os.unlink(tmp)
                    except OSError:
                        pass
                ok += 1
                log(f"{tag} OK   {dst}  ({human(e.size)}, {len(e.slices)} 片)")
            elif dry:
                await st.ensure_fresh()
                old = st.backend.get(dst)
                same = (old and not old.is_dir and old.status == 0
                        and old.size == os.path.getsize(f) and old.hash == digest_of(f))
                log(f"{tag} {'SKIP' if same else 'NEW '} {dst}  ({human(os.path.getsize(f))})")
                skipped += 1 if same else 0
                ok += 0 if same else 1
            else:
                e = await st.put_file(dst, f, slice_size=slice_size, skip_same=skip)
                if e.skipped:
                    skipped += 1
                    log(f"{tag} SKIP {dst}  (已存在)")
                else:
                    ok += 1
                    log(f"{tag} OK   {dst}  ({human(e.size)}, {len(e.slices)} 片)")
        except Exception as ex:
            failed += 1
            log(f"{tag} FAIL {dst}: {type(ex).__name__}: {ex}")
    if total > 1:
        log(f"\n完成：上传 {ok}，跳过 {skipped}，失败 {failed}，共 {total}")


async def _snapshot_loop(st, interval: float = 60.0):
    """后台每分钟把索引推到 TG 一次：本地写是实时的，TG 只做低频落盘。"""
    while True:
        await asyncio.sleep(interval)
        if not st.dirty:
            continue
        try:
            await st.snapshot_locked()
        except LockBusy:
            pass                                   # 别的节点正在写，下一轮再试
        except Exception as ex:
            log(f"  周期落库失败: {type(ex).__name__}: {ex}", flush=True)


async def cmd_pushdir(st, args):
    """批量把本地目录/文件推到远端，全程持锁（空闲 3 秒才落库放锁）。

    用法: tgnd pushdir [--no-skip] [--lease 3] <本地目录或文件> <远端根目录>
    """
    lease = 3.0
    skip = True
    excludes = []
    rest = []
    i = 0
    while i < len(args):
        a = args[i]
        if a == "--no-skip":
            skip = False; i += 1
        elif a == "--lease":
            lease = float(args[i + 1]); i += 2
        elif a in ("--exclude", "-x"):
            excludes.append(args[i + 1]); i += 2
        else:
            rest.append(a); i += 1
    if len(rest) < 2:
        sys.exit("用法: tgnd pushdir [--no-skip] [--lease 3] <本地目录或文件> <远端根目录>")
    src, dst = rest[0], rest[1]
    if not dst.startswith("/"):
        sys.exit("远端路径要以 / 开头")
    jobs = _collect([src], dst.rstrip("/") + "/", into=True)
    if excludes:
        import fnmatch
        before = len(jobs)
        jobs = [(f, d) for f, d in jobs
                if not any(fnmatch.fnmatch(d, pat) or pat.strip("/") in d for pat in excludes)]
        log(f"排除规则 {excludes}：{before} → {len(jobs)} 个文件", flush=True)
    if not jobs:
        sys.exit("没有可上传的文件")
    total = sum(os.path.getsize(p) for p, _ in jobs)
    log(f"共 {len(jobs)} 个文件，{human(total)}，租约 {lease}s", flush=True)

    ok = skipped = failed = 0
    t0 = time.time()
    snap_task = asyncio.create_task(_snapshot_loop(st, float(os.environ.get("SNAPSHOT_SEC", "60"))))

    # 阶段一：先把哈希算完（纯本地，不碰 TG），再一次性持锁写入索引
    import os as _os
    HASH_THREADS = int(_os.environ.get("HASH_THREADS", "2"))
    log(f"预计算哈希（{HASH_THREADS} 线程）…", flush=True)
    import concurrent.futures as _cf

    def _prep(item):
        f, d = item
        stt = os.stat(f)
        return (f, d, stt.st_size, digest_of(f), int(stt.st_mtime))

    prepared = []
    done_n = 0
    with _cf.ThreadPoolExecutor(max_workers=HASH_THREADS) as ex:
        for res in ex.map(_prep, jobs):
            prepared.append(res)
            done_n += 1
            progress(f"哈希 {done_n}/{len(jobs)}")

    log("写入本地索引（不持锁）…", flush=True)
    for f, d, size, digest, mt in prepared:
        try:
            e = await st.register(d, f, digest=digest, size=size, mtime=mt)
            if e.skipped:
                skipped += 1
        except Exception as ex:
            failed += 1
            log(f"登记失败 {d}: {type(ex).__name__}: {ex}", flush=True)

    # 只有「把索引推到 TG」这一步需要锁，而且一次 edit 同时落库 + 放锁
    if st.dirty:
        # 哈希算完、上传之前，先把清单落一次库：崩溃也不至于重算
        log("上传索引快照…", flush=True)
        await st.snapshot_locked()
    log(f"登记完成（新增 {len(jobs)-skipped-failed}，已存在 {skipped}，失败 {failed}），锁已释放", flush=True)

    # 阶段二：不持锁传字节；每 10 个文件短暂拿一次锁把结果落库
    def _is_pending(d):
        try:
            e = st.get(d)
        except Exception:
            return False
        return e is not None and not e.is_dir and e.status == 1

    todo = [(f, d) for f, d in jobs if _is_pending(d)]
    log(f"阶段二：待上传 {len(todo)} 个，开始传字节…", flush=True)
    for i, (f, d) in enumerate(todo, 1):
        try:
            e = await st.upload_pending(d, f)
            ok += 1
            done = time.time() - t0
            done()
            log(f"[{i}/{len(todo)}] OK {d} ({human(e.size)}) 已用 {done/60:.1f} 分钟", flush=True)
        except Exception as ex:
            failed += 1
            done()
            log(f"[{i}/{len(todo)}] FAIL {d}: {type(ex).__name__}: {ex}", flush=True)
        if is_online() and i % 100 == 0 and st.dirty:
            try:
                await st.snapshot_locked()      # 拿不到锁会在 acquire 里抛 LockBusy
            except LockBusy:
                log("  (索引锁被占用，跳过本次落库)", flush=True)
    try:
        async with st.guard(timeout=30):
            pass
    except Exception as ex:
        log("最终落库失败:", ex, flush=True)

    snap_task.cancel()
    await st.wait_released()
    dt = max(1e-6, time.time() - t0)
    log(f"完成：上传 {ok}，跳过 {skipped}，失败 {failed}，耗时 {dt/60:.1f} 分钟", flush=True)


async def cmd_get(st, args):
    if not args:
        sys.exit("用法: tgnd get <远端路径> [本地文件]")
    remote = norm(args[0])
    local = args[1] if len(args) > 1 else base_of(remote)
    e = st.get(remote)
    if e is None or e.is_dir:
        sys.exit(f"不存在或不是文件: {remote}")
    with open(local, "wb") as f:
        async for c in st.iter_content(remote):
            f.write(c)
    log(f"OK {remote} -> {local} ({human(e.size)})")


async def cmd_rm(st, args):
    for p in args:
        await st.delete(norm(p))
        log(f"removed {p}")


async def cmd_mv(st, args):
    await st.move(norm(args[0]), norm(args[1]))
    log(f"{args[0]} -> {args[1]}")


async def cmd_cp(st, args):
    await st.copy(norm(args[0]), norm(args[1]))
    log(f"copied {args[0]} -> {args[1]}")


def cmd_stat(st, args):
    e = st.get(norm(args[0]))
    if not e:
        sys.exit("不存在")
    log(f"path={e.path}")
    log(f"type={'dir' if e.is_dir else 'file'}")
    log(f"size={e.size} ({human(e.size)})")
    log(f"slices={len(e.slices)} ids={e.slices[:5]}{'...' if len(e.slices) > 5 else ''}")


USAGE = __doc__


async def cmd_export_sql(st, args):
    """把本地索引导出成 SQL 文本。"""
    import sqlite3
    out = args[0] if args else None
    con = sqlite3.connect(DB_PATH)
    sql = "\n".join(con.iterdump()) + "\n"
    con.close()
    if out:
        open(out, "w", encoding="utf-8").write(sql)
        log(f"已导出 {out}（{len(sql)} 字节）")
    else:
        sys.stdout.write(sql)


async def cmd_import_sql(st, args):
    """从 SQL 文本重建本地索引（会先清空现有库）。"""
    import sqlite3
    if not args:
        sys.exit("用法: tgnd import-sql <文件.sql>")
    sql = open(args[0], encoding="utf-8").read()
    con = sqlite3.connect(DB_PATH)
    con.executescript(sql)
    con.commit()
    n = con.execute("select count(*) from files").fetchone()[0]
    d = con.execute("select count(*) from dirs").fetchone()[0]
    con.close()
    log(f"已导入：{n} 个文件、{d} 个目录")


async def main():
    argv = sys.argv[1:]
    if not argv or argv[0] in ("-h", "--help", "help"):
        log(USAGE)
        return
    cmd, args = argv[0], argv[1:]
    client, st, tmp = await get_store()
    try:
        if cmd in ("ls", "ll"):
            cmd_ls(st, args, long=(cmd == "ll"))
        elif cmd == "mkdir":
            for p in args:
                await est_mkdir(st, p)
        elif cmd == "put":
            await cmd_put(st, args)
        elif cmd in ("pushdir", "push"):
            await cmd_pushdir(st, args)
        elif cmd == "get":
            await cmd_get(st, args)
        elif cmd == "rm":
            await cmd_rm(st, args)
        elif cmd == "mv":
            await cmd_mv(st, args)
        elif cmd == "cp":
            await cmd_cp(st, args)
        elif cmd == "export-sql":
            await cmd_export_sql(st, args)
        elif cmd == "import-sql":
            await cmd_import_sql(st, args)
        elif cmd == "gc":
            dry = "--dry-run" in args or "-n" in args
            orphans, missing = await st.gc(dry_run=dry)
            log(f"{'[预览] ' if dry else ''}孤儿消息 {len(orphans)} 条，无效记录 {len(missing)} 条")
            for p in missing[:20]:
                log("  无效记录:", p)
        elif cmd == "reindex":
            n = await st.sync(full=True)
            log(f"已扫描 {n} 条消息重建索引")
        elif cmd in ("stat", "info"):
            cmd_stat(st, args)
        else:
            log(USAGE)
    finally:
        try:
            if st.dirty:
                await st.snapshot()
        except Exception as e:
            log("快照上传失败:", e)
        await client.disconnect()
        cleanup(tmp)


async def est_mkdir(st, p):
    await st.mkdir(norm(p))
    log(f"mkdir {p}")


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    asyncio.run(main())
