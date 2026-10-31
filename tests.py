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
import os
import sys
import tempfile
import traceback
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

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

    async def iter_download(self, m, **kw):
        data = self._blobs.get(m.id, b"")
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

    async def t_gc():
        # gc 是只读体检：无论怎么调都不许删消息/删记录
        before_msgs = dict(client._msgs)
        before = {e.path for e in st.backend.all_entries().values()}
        orphans, missing = await st.gc()
        assert isinstance(orphans, list) and isinstance(missing, list)
        assert set(client._msgs) == set(before_msgs), "gc 删掉了 TG 消息"
        assert {e.path for e in st.backend.all_entries().values()} == before, "gc 改了索引"

    async def t_reindex():
        before = st.get(f"{D}/one")
        await st.sync(full=True)
        after = st.get(f"{D}/one")
        assert (before is None) == (after is None)

    # ---- 数据路径的边角：emoji、路径自覆盖、gc/manual、历史分片格式、缺分片 ----

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

    async def t_gc_manual():
        if "--real" in sys.argv:
            print("SKIP  手动上传用例只跑内存版（会往真频道发消息）")
            return
        m = await client.send_file(1, b"manual-bytes",
                                   attributes=[SimpleNamespace(file_name="manual-photo.jpg")])
        await st.sync(full=True)
        p = "/lost+found/manual-photo.jpg"
        assert st.get(p) is not None, "手动上传没归入 /lost+found"
        _, missing = await st.gc()
        assert st.get(p) is not None, f"gc 把手动上传的记录当 missing 删了: {missing}"
        assert m.id in client._msgs, "gc 删掉了手动上传的 TG 消息"
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
        _, missing = await st.gc(dry_run=True)
        assert p not in missing, "list 格式被 gc 误判成 missing"
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
        app = web.Application(client_max_size=0)
        app.router.add_route("*", "/{tail:.*}", lambda r: srv.handle(r, st3))
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
        finally:
            if old is None:
                os.environ.pop("ONLINE", None)
            else:
                os.environ["ONLINE"] = old

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

    for name, fn in [
        ("空文件", t_empty), ("1字节", t_one_byte), ("中文+空格", t_unicode),
        ("特殊字符", t_special), ("深目录", t_deep), ("多分片7片", t_multislice),
        ("秒传共享分片", t_dedup), ("同名覆盖", t_overwrite), ("cp+mv", t_copy_move),
        ("删除文件", t_delete), ("递归删目录", t_rmdir), ("mkdir+空目录", t_mkdir),
        ("gc 只读体检", t_gc), ("reindex 全量", t_reindex),
        ("emoji 路径", t_emoji_path), ("move 自覆盖保护", t_move_guard),
        ("gc 不删手动上传", t_gc_manual), ("历史分片格式", t_part_list_format),
        ("缺分片必须报错", t_missing_slice), ("WebDAV 语义", t_dav_basic),
        (".. 路径规范化", t_norm_dotdot), ("覆盖失败保留旧文件", t_overwrite_fail_keeps_old),
        ("父路径是文件", t_parent_is_file), ("append 记录总大小", t_append_total_size),
        ("13 片顺序", t_many_slices), ("lost+found 不参与秒传", t_lostfound_not_dedup_source),
        ("引用计数按消息 id", t_refcount_dedup),
        ("锁-空频道自建快照", t_lock_bootstrap), ("锁-重复acquire幂等", t_lock_reacquire),
        ("锁-双节点互斥", t_lock_busy),
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
