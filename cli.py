#!/usr/bin/env python3
"""tgnd —— 直接调 Telegram 的网盘命令行（不走 WebDAV，只依赖 store.py）。

用法:
  tgnd ls [路径]
  tgnd ll [路径]            带大小/时间
  tgnd mkdir <路径>
  tgnd put [--slice 1900M] [--offset N] [--length N] <本地文件...> <远端路径>
  tgnd get <远端路径> [本地文件]
  tgnd rm <路径...>
  tgnd mv <源> <目标>
  tgnd cp <源> <目标>
  tgnd stat <路径>

环境变量（默认读 /app/tgnd/env）:
  TG_API_ID, TG_API_HASH, TG_SESSION, TG_CHAT
"""
import asyncio
import os
import shutil
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from store import Store, norm, base_of, make_session, is_path_session  # noqa: E402


def load_env():
    if os.environ.get("TG_API_ID"):
        return
    here = os.path.dirname(os.path.abspath(__file__))
    for path in [os.environ.get("TGND_ENV", ""), os.path.join(here, "env"),
                 os.path.join(here, ".env"), "/app/tgnd/env"]:
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
                            proxy=build_proxy())
    await client.connect()
    if not await client.is_user_authorized():
        sys.exit("session 未授权")
    chat = os.environ["TG_CHAT"]
    chat = int(chat) if chat.lstrip("-").isdigit() else chat
    st = Store(client, chat)
    await st.sync(full=True)
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
        print(f"{e.name}\t{human(e.size)}")
        return
    for c in st.children(path):
        if long:
            name = c.name + ("/" if c.is_dir else "")
            print(f"{human(c.size) if not c.is_dir else '-':>8}\t{name}")
        else:
            print(c.name + ("/" if c.is_dir else ""))


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


async def cmd_put(st, args):
    slice_size, offset, length, rest = None, None, None, []
    i = 0
    while i < len(args):
        a = args[i]
        if a in ("--slice", "-s"):
            slice_size = parse_size(args[i + 1]); i += 2
        elif a in ("--offset", "-o"):
            offset = parse_size(args[i + 1]); i += 2
        elif a in ("--length", "-l"):
            length = parse_size(args[i + 1]); i += 2
        else:
            rest.append(a); i += 1
    if len(rest) < 2:
        sys.exit("用法: tgnd put [--slice 1900M] [--offset N] [--length N] <本地文件...> <远端路径>")
    remote = rest[-1]
    if not remote.startswith("/"):
        sys.exit("远端路径要以 / 开头")
    files = rest[:-1]
    for f in files:
        if not os.path.isfile(f):
            sys.exit(f"本地文件不存在: {f}")
        dst = remote
        if len(files) > 1 or remote.endswith("/"):
            dst = remote.rstrip("/") + "/" + os.path.basename(f)
        if offset is not None or length is not None:
            # 只取源文件的 [offset, offset+length) 这一段，作为新分片追加
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
            print(f"OK {f}[{off}:+{(length if length is not None else 'EOF')}] -> {dst} 共 {len(e.slices)} 片 ({human(e.size)})")
        else:
            e = await st.put_file(dst, f, slice_size=slice_size)
            print(f"OK {f} -> {dst} ({human(e.size)}, {len(e.slices)} 片)")


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
    print(f"OK {remote} -> {local} ({human(e.size)})")


async def cmd_rm(st, args):
    for p in args:
        await st.delete(norm(p))
        print(f"removed {p}")


async def cmd_mv(st, args):
    await st.move(norm(args[0]), norm(args[1]))
    print(f"{args[0]} -> {args[1]}")


async def cmd_cp(st, args):
    await st.copy(norm(args[0]), norm(args[1]))
    print(f"copied {args[0]} -> {args[1]}")


def cmd_stat(st, args):
    e = st.get(norm(args[0]))
    if not e:
        sys.exit("不存在")
    print(f"path={e.path}")
    print(f"type={'dir' if e.is_dir else 'file'}")
    print(f"size={e.size} ({human(e.size)})")
    print(f"slices={len(e.slices)} ids={e.slices[:5]}{'...' if len(e.slices) > 5 else ''}")


USAGE = __doc__


async def main():
    argv = sys.argv[1:]
    if not argv or argv[0] in ("-h", "--help", "help"):
        print(USAGE)
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
        elif cmd == "get":
            await cmd_get(st, args)
        elif cmd == "rm":
            await cmd_rm(st, args)
        elif cmd == "mv":
            await cmd_mv(st, args)
        elif cmd == "cp":
            await cmd_cp(st, args)
        elif cmd in ("stat", "info"):
            cmd_stat(st, args)
        else:
            print(USAGE)
    finally:
        await client.disconnect()
        cleanup(tmp)


async def est_mkdir(st, p):
    await st.mkdir(norm(p))
    print(f"mkdir {p}")


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    asyncio.run(main())
