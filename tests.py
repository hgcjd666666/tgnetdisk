#!/usr/bin/env python3
"""tgnd 自测：单进程直接调 Store，不反复启 CLI、不扫聊天记录。

用法:
    python tests.py            # 全量用例，用内存版 Telegram（秒级，零网络）
    python tests.py -k put     # 只跑名字含 put 的
    python tests.py --real     # 连真 Telegram（会产生真实消息）
    python tests.py --full     # 连锁与快照一起测

对环境的要求同 cli.py（读同目录 env 或 TGND_ENV 指向的文件）。
"""
import asyncio
import datetime
import hashlib
import io
import lzma
import os
import sqlite3
import sys
import tempfile
import traceback
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 缓存目录必须是独立的临时目录：CACHE_DIR 在 import 时就固定了，先设再导
os.environ["TGND_CACHE"] = tempfile.mkdtemp(prefix="tgnd_test_cache_")

import store as S  # noqa: E402
from store import (Store, SqliteBackend, make_session, is_db_msg, norm, digest_of,
                   LOCK_IDLE, LOCK_PREFIX, LockBusy)  # noqa: E402

RESULTS = []


class FakeClient:
    """内存版 Telegram 服务端：send_file / get_messages / edit_message / delete_messages / iter_download。

    默认测试走它，零网络、零垃圾消息；加 --real 才连真 Telegram。
    """

    def __init__(self):
        self._id = 0
        self._msgs = {}
        self._blobs = {}
        self.downloads = 0          # iter_download 被调用的次数，用来判断缓存有没有命中
        self._uploads = {}          # file_id -> {part: bytes}  模拟 upload.saveBigFilePart
        self._pinned_ids = []       # 模拟置顶：只有置顶过的消息才会出现在 pinned 过滤里

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
        mid = msg if isinstance(msg, int) else msg.id
        m = self._msgs[mid]
        if file is not None:
            data, name = self._read(file, attributes)
            m.document.size = len(data)
            m.document.attributes = [SimpleNamespace(file_name=name)]
            self._blobs[mid] = data
        # 真实 telethon：改文字用 text=，caption= 只在发文件的调用里出现。
        # 锁状态就是靠 edit_message(text=...) 写的，这里必须认 text，否则永远改不动 caption。
        cap = text if text is not None else caption
        if cap is not None:
            m.message = cap
        return m

    async def get_messages(self, chat, ids=None, limit=None, filter=None, **kw):
        if ids is not None:
            if isinstance(ids, (list, tuple)):
                return [self._msgs[i] for i in ids if i in self._msgs]
            return self._msgs.get(ids)
        if filter is not None and "Pinned" in type(filter).__name__:
            msgs = [self._msgs[i] for i in self._pinned_ids if i in self._msgs]
            return msgs[:limit] if limit else msgs
        return [self._msgs[i] for i in sorted(self._msgs, reverse=True)[:limit or 1]]

    async def iter_messages(self, chat, limit=None, min_id=0, reverse=False, **kw):
        ids = sorted(self._msgs)
        if min_id:
            ids = [i for i in ids if i > min_id]
        if not reverse:
            ids = ids[::-1]
        if limit:
            ids = ids[:limit]
        for i in ids:
            yield self._msgs[i]

    async def delete_messages(self, chat, ids):
        for i in ids:
            self._msgs.pop(i, None)
            self._blobs.pop(i, None)
            if i in self._pinned_ids:
                self._pinned_ids.remove(i)

    async def connect(self):
        pass

    async def disconnect(self):
        pass

    async def get_entity(self, x):
        return x

    async def pin_message(self, chat, msg, notify=False, **kw):
        mid = msg if isinstance(msg, int) else msg.id
        self._pinned_ids = [mid]

    async def is_user_authorized(self):
        return True

    async def __call__(self, req):
        if type(req).__name__ == "SaveBigFilePartRequest":
            self._uploads.setdefault(req.file_id, {})[req.file_part] = req.bytes
            return True
        return SimpleNamespace(full_chat=SimpleNamespace(
            pinned_msg_id=self._pinned_ids[-1] if self._pinned_ids else None))

    async def iter_download(self, m, *, offset=0, limit=None, **kw):
        self.downloads += 1
        data = self._blobs.get(m.id, b"")
        if offset:
            data = data[offset:]
        if limit is not None:
            data = data[:limit]
        for i in range(0, len(data), 512 * 1024):
            yield data[i:i + 512 * 1024]

    async def download_media(self, m, file=None, **kw):
        data = self._blobs.get(m if isinstance(m, int) else m.id, b"")
        if hasattr(file, "write"):
            file.write(data)
            return file
        with open(file, "wb") as f:
            f.write(data)
        return file


def rec(name, ok, detail=""):
    RESULTS.append((name, ok, detail))
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"   <<< {detail}" if not ok else ""),
          flush=True)


def rand(n):
    return os.urandom(n)


async def put_bytes(st, data, path, **kw):
    fd, tmp = tempfile.mkstemp()
    with os.fdopen(fd, "wb") as f:
        f.write(data)
    try:
        await st.put_file(path, tmp, **kw)
    finally:
        os.unlink(tmp)


async def get_bytes(st, path):
    out = b""
    async for chunk in st.iter_content(path):
        out += chunk
    return out


async def get_slice(st, path, off, n=None):
    out = b""
    async for chunk in st.iter_content(path, offset=off, length=n):
        out += chunk
    return out


async def main():
    only = None
    if "-k" in sys.argv:
        only = sys.argv[sys.argv.index("-k") + 1]

    if "--real" in sys.argv:                    # 连真实 Telegram
        from telethon import TelegramClient
        api_id = int(os.environ["TG_API_ID"])
        api_hash = os.environ["TG_API_HASH"]
        chat = os.environ["TG_CHAT"]
        chat = int(chat) if str(chat).lstrip("-").isdigit() else chat
        client = TelegramClient(make_session(os.environ["TG_SESSION"]), api_id, api_hash,
                                device_model=os.environ.get("TG_DEVICE", "tgnd-test"),
                                system_version="Linux", app_version="tgnd")
        await client.connect()
        st = Store(client, chat)
        st.fast = "--full" not in sys.argv
        await st.load()
    else:                                       # 默认：内存版 TG
        client = FakeClient()
        chat = 1
        st = Store(client, chat, backend=SqliteBackend(tempfile.mktemp(suffix=".db")))
        st.fast = "--full" not in sys.argv

    async def case(name, fn):
        if only and only not in name:
            return
        try:
            await fn()
            rec(name, True)
        except AssertionError as e:
            rec(name, False, str(e) or "断言失败")
        except Exception as e:
            rec(name, False, f"{type(e).__name__}: {e}")
            traceback.print_exc()

    D = "/_test"

    async def t_empty():
        await put_bytes(st, b"", f"{D}/empty")
        assert await get_bytes(st, f"{D}/empty") == b"", "空文件读取不为空"

    async def t_one_byte():
        await put_bytes(st, b"x", f"{D}/one")
        assert await get_bytes(st, f"{D}/one") == b"x"

    async def t_unicode():
        data = rand(3000)
        await put_bytes(st, data, f"{D}/中文 目录/音频 测试.mp3")
        assert await get_bytes(st, f"{D}/中文 目录/音频 测试.mp3") == data

    async def t_special():
        data = rand(3000)
        await put_bytes(st, data, f"{D}/a b(1)#?&.txt")
        assert await get_bytes(st, f"{D}/a b(1)#?&.txt") == data

    async def t_deep():
        await put_bytes(st, b"x", f"{D}/d1/d2/d3/d4/deep")
        assert await get_bytes(st, f"{D}/d1/d2/d3/d4/deep") == b"x"

    async def t_multislice():
        data = rand(400000)
        await put_bytes(st, data, f"{D}/multi", slice_size=64 * 1024)
        e = st.get(f"{D}/multi")
        assert len(e.parts) == 7, f"分片数 {len(e.parts)} != 7"
        assert await get_bytes(st, f"{D}/multi") == data, "内容不一致"

    async def t_dedup():
        data = rand(50000)
        await put_bytes(st, data, f"{D}/dup1")
        await put_bytes(st, data, f"{D}/dup2")
        a, b = st.get(f"{D}/dup1"), st.get(f"{D}/dup2")
        assert a.parts == b.parts, f"没共享分片 {a.parts} vs {b.parts}"
        assert await get_bytes(st, f"{D}/dup2") == data

    async def t_overwrite():
        await put_bytes(st, b"v1", f"{D}/over")
        await put_bytes(st, b"v2", f"{D}/over")
        assert await get_bytes(st, f"{D}/over") == b"v2", "覆盖后不是新内容"

    async def t_copy_move():
        await put_bytes(st, rand(1000), f"{D}/src")
        await st.copy(f"{D}/src", f"{D}/cp")
        await st.move(f"{D}/cp", f"{D}/mv")
        assert st.get(f"{D}/mv") is not None and st.get(f"{D}/cp") is None
        assert await get_bytes(st, f"{D}/mv") == await get_bytes(st, f"{D}/src")

    async def t_delete():
        await put_bytes(st, rand(1000), f"{D}/del")
        await st.delete(f"{D}/del")
        assert st.get(f"{D}/del") is None
        try:
            await get_bytes(st, f"{D}/del")
            raise AssertionError("删了还能读")
        except FileNotFoundError:
            pass

    async def t_rmdir():
        await put_bytes(st, b"x", f"{D}/dir/a")
        await put_bytes(st, b"y", f"{D}/dir/sub/b")
        await st.delete(f"{D}/dir")
        assert st.get(f"{D}/dir/sub/b") is None, "子文件没删掉"

    async def t_mkdir():
        await st.mkdir(f"{D}/mdir")
        assert st.get(f"{D}/mdir") is not None and st.get(f"{D}/mdir").is_dir
        assert st.children(f"{D}/mdir") == []

    async def t_reindex():
        before = st.get(f"{D}/one")
        await st.sync(full=True)
        after = st.get(f"{D}/one")
        assert (before is None) == (after is None)

    # ---- 数据路径的边角：emoji、路径自覆盖、手动上传、历史分片格式、缺分片 ----

    async def t_emoji_path():
        data = rand(2000)
        p = f"{D}/🎵 emoji/😀 笑脸.bin"
        await put_bytes(st, data, p)
        assert st.get(p) is not None, "emoji 路径没登记进索引"
        kids = [e.path for e in st.children(f"{D}/🎵 emoji")]
        assert p in kids, f"children 漏掉 emoji 文件: {kids}"
        assert await get_bytes(st, p) == data, "emoji 文件内容不一致"
        await st.delete(f"{D}/🎵 emoji")
        assert st.get(p) is None, "递归删目录没删掉 emoji 文件"

    async def t_move_guard():
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

    async def t_manual_to_lostfound():
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

    async def t_part_list_format():
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
        assert m.id not in client._msgs, "list 格式删除没删掉 TG 消息"

    async def t_missing_slice():
        await put_bytes(st, b"0123456789", f"{D}/broken")
        e = st.get(f"{D}/broken")
        st.backend.set_file(f"{D}/broken", e.hash, e.size, e.mtime, {0: 999999}, 0, 1)
        try:
            await get_bytes(st, f"{D}/broken")
            raise AssertionError("缺分片时下载没有报错（会静默交付截断文件）")
        except IOError:
            pass
        st.backend.remove(f"{D}/broken")

    async def t_norm_dotdot():
        assert norm("/a/../b") == "/b", "norm 没消解 .."
        assert norm("/a/./b/") == "/a/b"
        assert norm("/../x") == "/x"
        await put_bytes(st, b"shadow", f"{D}/normdir/../normfile")
        assert st.get(f"{D}/normfile") is not None, ".. 路径没被规范化，产生影子条目"
        kids = [e.path for e in st.children(D)]
        assert f"{D}/normfile" in kids, f"规范化后的文件不在 children 里: {kids}"
        await st.delete(f"{D}/normfile")

    async def t_overwrite_fail_keeps_old():
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

    async def t_parent_is_file():
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

    async def t_append_total_size():
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

    async def t_many_slices():
        data = rand(12 * 1024 + 7)          # 13 片，跨过 "10" 的字符串排序坑
        await put_bytes(st, data, f"{D}/many", slice_size=1024)
        e = st.get(f"{D}/many")
        assert len(e.parts) == 13, f"分片数 {len(e.parts)} != 13"
        assert sorted(e.parts) == list(range(13)), f"parts 的 key 不是 int: {sorted(e.parts)[:5]}"
        assert await get_bytes(st, f"{D}/many") == data, "多分片下载内容顺序错乱"
        await st.delete(f"{D}/many")

    async def t_lostfound_not_dedup_source():
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
        assert e.size == len(body), f"秒传用了 lost+found 残片，记录 size={e.size}"
        assert e.parts != {0: m.id}, "还是秒传命中了 lost+found 的残片"
        assert await get_bytes(st, f"{D}/after-rescue") == body
        await st.delete(p_lf)
        assert m.id not in client._msgs, "残片消息没被清掉（引用计数按 hash 太粗）"
        assert await get_bytes(st, f"{D}/after-rescue") == body, "删 lost+found 影响了真实文件"
        await st.delete(f"{D}/after-rescue")

    async def t_refcount_dedup():
        data = rand(4000)
        await put_bytes(st, data, f"{D}/rc1")
        await put_bytes(st, data, f"{D}/rc2")
        a, b = st.get(f"{D}/rc1"), st.get(f"{D}/rc2")
        assert a.parts == b.parts, "秒传没共享分片"
        mids = set(a.parts.values())
        await st.delete(f"{D}/rc2")
        assert all(mid in client._msgs for mid in mids), "删一个引用就把共享消息删了"
        await st.delete(f"{D}/rc1")
        assert all(mid not in client._msgs for mid in mids), "最后一个引用删了消息还留着"

    async def t_dav_basic():
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

    async def t_move_ancestor():
        await put_bytes(st, b"v", f"{D}/anc/sub/f")
        for dst in (f"{D}/anc", f"{D}/anc/sub/..", "/"):
            try:
                await st.move(f"{D}/anc/sub", dst)
                raise AssertionError(f"move 到祖先 {dst} 应被拒绝")
            except OSError:
                pass
        assert st.get(f"{D}/anc/sub/f") is not None, "move 被拒绝但数据没了"
        await st.delete(f"{D}/anc")

    async def t_manual_named_snapshot():
        if "--real" in sys.argv:
            print("SKIP  同名快照用例只跑内存版")
            return
        await client.send_file(1, b"not-a-db",
                               attributes=[SimpleNamespace(file_name="tgnd.db.xz")])
        await st.sync(full=True)
        p = "/lost+found/tgnd.db.xz"
        assert st.get(p) is not None, "手动上传的同名文件被当成快照丢掉了"
        await st.delete(p)

    async def t_foreign_part_msg():
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

    async def t_ring_long_line():
        import logger as lg
        d = tempfile.mkdtemp()
        p = os.path.join(d, "ring.log")
        r = lg.RingLog(p, 256)
        r.write("Z" * 400)                      # 单行比整个正文还长
        txt = lg.read_log(p)
        assert "Z" * 200 in txt, f"超长行被整条切掉了: {txt!r}"

    # ---- 分布式锁（快照 caption 即锁）：只有 --full 才走真实锁路径 ----

    async def t_lock_bootstrap():
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

    async def t_lock_reacquire():
        if st.fast:
            return
        await st.acquire(timeout=10)
        assert st._holding
        await st.acquire(timeout=10)         # 已持有：幂等，不能再抢一次
        assert st._holding
        await st.release()
        assert not st._holding

    async def t_lock_busy():
        if st.fast:
            return
        # 另一个节点已经持锁时，nonblock 应立即抛 LockBusy 而不是傻等
        other = Store(client, chat, backend=SqliteBackend(tempfile.mktemp(suffix=".db")))
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
        await other.release()

    async def t_snapshot_dirty_clean():
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

    # ---------- 竞态 ----------
    async def t_race_backend_writers():
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

    async def t_race_backend_same_path():
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

    async def t_race_restore_storm():
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

    async def t_race_dump_while_write():
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

    async def t_race_lock_contend():
        stores = []
        for i in range(3):
            o = Store(client, chat, backend=SqliteBackend(tempfile.mktemp(suffix=".db")))
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

    async def t_race_put_dedup():
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

    async def t_race_delete_read():
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

    async def t_race_process_coexist():
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

    async def t_race_put_many_different():
        async def one(i):
            await put_bytes(st, rand(50000), f"{D}/race/many{i}")
        await asyncio.gather(*[one(i) for i in range(10)])
        for i in range(10):
            e = st.get(f"{D}/race/many{i}")
            assert e and e.status == 0, f"第 {i} 个未完成"
        assert await get_bytes(st, f"{D}/race/many9") is not None

    async def t_race_put_same_path():
        data = [rand(30000) for _ in range(6)]
        async def one(i):
            await put_bytes(st, data[i], f"{D}/race/samepath")
        await asyncio.gather(*[one(i) for i in range(6)])
        e = st.get(f"{D}/race/samepath")
        assert e and e.status == 0, f"最终记录异常: {e}"
        got = await get_bytes(st, f"{D}/race/samepath")
        assert len(got) == 30000, f"内容长度 {len(got)}"

    async def t_race_write_read_content():
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

    async def t_race_snapshot_while_write():
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

    async def t_race_mv_cp():
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

    async def t_race_kill_recovery():
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

    # ---------- 本地读缓存 ----------
    BLOBS = os.path.join(S.CACHE_DIR, "blobs")

    def blobs():
        try:
            return sorted(n for n in os.listdir(BLOBS) if not n.startswith("."))
        except OSError:
            return []

    def wipe_cache():
        for n in blobs():
            try:
                os.unlink(os.path.join(BLOBS, n))
            except OSError:
                pass
        try:
            for n in os.listdir(BLOBS):
                if n.startswith(".dl-"):
                    os.unlink(os.path.join(BLOBS, n))
        except OSError:
            pass

    def cache_env(gb):
        if gb is None:
            os.environ.pop("CACHE_MAX_GB", None)
        else:
            os.environ["CACHE_MAX_GB"] = repr(gb)

    async def t_cache_default_off():
        cache_env(None)
        wipe_cache()
        data = rand(40000)
        await put_bytes(st, data, f"{D}/cache/off")
        assert await get_bytes(st, f"{D}/cache/off") == data
        assert blobs() == [], f"缓存默认该关闭，却落了盘: {blobs()}"

    async def t_cache_hit():
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

    async def t_cache_oversize():
        cache_env(0.000005)                 # ≈5KB 上限
        wipe_cache()
        data = rand(20000)
        await put_bytes(st, data, f"{D}/cache/big")
        assert await get_bytes(st, f"{D}/cache/big") == data
        assert blobs() == [], f"比上限还大的文件不该缓存: {blobs()}"

    async def t_cache_lru():
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

    async def t_cache_invalidate_overwrite():
        cache_env(1)
        wipe_cache()
        await put_bytes(st, b"v1-xxxx", f"{D}/cache/inv")
        assert await get_bytes(st, f"{D}/cache/inv") == b"v1-xxxx"
        assert len(blobs()) == 1
        await put_bytes(st, b"v2-yyyy", f"{D}/cache/inv")      # 重传 → 分片 id 变了
        n = client.downloads
        assert await get_bytes(st, f"{D}/cache/inv") == b"v2-yyyy", "覆盖后命中了旧缓存"
        assert client.downloads > n, "覆盖后应该重新下载，而不是命中旧内容"

    async def t_cache_dedup_share():
        cache_env(1)
        wipe_cache()
        data = rand(50000)
        await put_bytes(st, data, f"{D}/cache/d1")
        await put_bytes(st, data, f"{D}/cache/d2")             # 秒传，共用同一批分片
        assert await get_bytes(st, f"{D}/cache/d1") == data
        n = client.downloads
        assert await get_bytes(st, f"{D}/cache/d2") == data
        assert client.downloads == n, "同内容的另一个路径没命中同一份缓存"

    async def t_cache_move_hit():
        cache_env(1)
        wipe_cache()
        data = rand(20000)
        await put_bytes(st, data, f"{D}/cache/m1")
        assert await get_bytes(st, f"{D}/cache/m1") == data
        await st.move(f"{D}/cache/m1", f"{D}/cache/m2")        # move 只改路径，不动分片
        n = client.downloads
        assert await get_bytes(st, f"{D}/cache/m2") == data
        assert client.downloads == n, "move 之后没命中缓存"

    async def t_cache_corrupt():
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

    async def t_cache_abort_no_leftover():
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

    async def t_cache_concurrent():
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

    async def t_cache_path_independent():
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

    # ---------- 落盘即返回 + 后台上传 ----------
    async def make_dav(start_up=True):
        from aiohttp import web
        from aiohttp.test_utils import TestClient, TestServer
        import server as srv
        c = FakeClient()
        s2 = Store(c, 1, backend=SqliteBackend(tempfile.mktemp(suffix=".db")))
        s2.fast = True
        up = srv.Uploader(s2, 1)
        app = web.Application(client_max_size=0)
        app.router.add_route("*", "/{tail:.*}", lambda r: srv.handle(r, s2, up))
        cli = TestClient(TestServer(app))
        await cli.start_server()
        if start_up:
            up.start()
        return cli, s2, up, c, srv

    async def stop_dav(cli, up):
        for t in up.tasks:
            t.cancel()
        await cli.close()

    async def t_up_accept():
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

    async def t_up_finish():
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

    async def t_up_delete_pending():
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

    async def t_up_move_pending():
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

    async def t_up_overwrite_pending():
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

    async def t_up_crash_recover():
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

    async def t_up_gc_orphan_staging():
        cli, s2, up, c, srv = await make_dav(start_up=False)
        try:
            orphan = os.path.join(S.staging_dir(), "f" * 40)
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

    # ---------- HTTP Range ----------
    async def t_range_store():
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

    async def t_range_http():
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

    async def t_range_after_upload():
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

    async def t_range_cache_hit():
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

    for name, fn in [
        ("空文件", t_empty), ("1字节", t_one_byte), ("中文+空格", t_unicode),
        ("特殊字符", t_special), ("深目录", t_deep), ("多分片7片", t_multislice),
        ("秒传共享分片", t_dedup), ("同名覆盖", t_overwrite), ("cp+mv", t_copy_move),
        ("删除文件", t_delete), ("递归删目录", t_rmdir), ("mkdir+空目录", t_mkdir),
        ("reindex 全量", t_reindex),
        ("emoji 路径", t_emoji_path), ("move 自覆盖保护", t_move_guard),
        ("手动上传归入 lost+found", t_manual_to_lostfound), ("历史分片格式", t_part_list_format),
        ("缺分片必须报错", t_missing_slice), ("WebDAV 语义", t_dav_basic),
        (".. 路径规范化", t_norm_dotdot), ("覆盖失败保留旧文件", t_overwrite_fail_keeps_old),
        ("父路径是文件", t_parent_is_file), ("append 记录总大小", t_append_total_size),
        ("13 片顺序", t_many_slices), ("lost+found 不参与秒传", t_lostfound_not_dedup_source),
        ("引用计数按消息 id", t_refcount_dedup), ("move 到祖先被拒", t_move_ancestor),
        ("同名 tgnd.db.xz 不丢", t_manual_named_snapshot),
        ("同名 hash.0 不覆盖", t_foreign_part_msg), ("环形超长行", t_ring_long_line),
        ("锁-空频道自建快照", t_lock_bootstrap), ("锁-重复acquire幂等", t_lock_reacquire),
        ("锁-双节点互斥", t_lock_busy),
        ("快照里的 dirty 恒为 0", t_snapshot_dirty_clean),
        ("竞态-多线程写不同路径", t_race_backend_writers),
        ("竞态-多线程写同一路径", t_race_backend_same_path),
        ("竞态-restore 风暴", t_race_restore_storm),
        ("竞态-dump 与写并发", t_race_dump_while_write),
        ("竞态-三节点抢锁", t_race_lock_contend),
        ("竞态-并发秒传", t_race_put_dedup),
        ("竞态-边删边读", t_race_delete_read),
        ("竞态-双进程共存", t_race_process_coexist),
        ("竞态-并发传不同文件", t_race_put_many_different),
        ("竞态-并发覆盖同路径", t_race_put_same_path),
        ("竞态-边写边读内容", t_race_write_read_content),
        ("竞态-推送中写入", t_race_snapshot_while_write),
        ("竞态-mv 与 cp 并发", t_race_mv_cp),
        ("竞态-kill 后恢复", t_race_kill_recovery),
        ("缓存-默认关闭", t_cache_default_off),
        ("缓存-命中不碰TG", t_cache_hit),
        ("缓存-超上限不落盘", t_cache_oversize),
        ("缓存-LRU淘汰最旧", t_cache_lru),
        ("缓存-覆盖重传失效", t_cache_invalidate_overwrite),
        ("缓存-秒传共享同一份", t_cache_dedup_share),
        ("缓存-move后仍命中", t_cache_move_hit),
        ("缓存-残片丢弃重下", t_cache_corrupt),
        ("缓存-断连不留残迹", t_cache_abort_no_leftover),
        ("缓存-并发读只落一份", t_cache_concurrent),
        ("缓存-不同内容互不串", t_cache_path_independent),
        ("上传-PUT立刻返回", t_up_accept),
        ("上传-后台传完清staging", t_up_finish),
        ("上传-删掉未传完的", t_up_delete_pending),
        ("上传-移动后仍能传完", t_up_move_pending),
        ("上传-覆盖未传完的", t_up_overwrite_pending),
        ("上传-崩溃后重启续传", t_up_crash_recover),
        ("上传-清无主staging", t_up_gc_orphan_staging),
        ("范围-Store分段读", t_range_store),
        ("范围-HTTP 206与416", t_range_http),
        ("范围-传完后从TG读", t_range_after_upload),
        ("范围-命中缓存seek", t_range_cache_hit),
    ]:
        await case(name, fn)

    # 收尾：删测试目录并提交索引
    try:
        if st.get(D):
            await st.delete(D)
    except Exception as e:
        print("清理失败:", e)

    if not st.fast:
        try:
            await st.wait_released(10)
        except Exception:
            pass

    await client.disconnect()
    ok = sum(1 for _, o, _ in RESULTS if o)
    print(f"\n===== PASS={ok} FAIL={len(RESULTS) - ok} =====")
    return 0 if ok == len(RESULTS) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
