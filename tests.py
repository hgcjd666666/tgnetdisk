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

from store import Store, SqliteBackend, make_session  # noqa: E402

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

    async def edit_message(self, chat, msg, file=None, caption=None, attributes=None, **kw):
        mid = msg if isinstance(msg, int) else msg.id
        m = self._msgs[mid]
        if file is not None:
            data, name = self._read(file, attributes)
            m.document.size = len(data)
            m.document.attributes = [SimpleNamespace(file_name=name)]
            self._blobs[mid] = data
        if caption is not None:
            m.message = caption
        return m

    async def get_messages(self, chat, ids=None, limit=None, **kw):
        if ids is not None:
            if isinstance(ids, (list, tuple)):
                return [self._msgs[i] for i in ids if i in self._msgs]
            return self._msgs.get(ids)
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

    async def connect(self):
        pass

    async def disconnect(self):
        pass

    async def get_entity(self, x):
        return x

    async def pin_message(self, *a, **kw):
        pass

    async def is_user_authorized(self):
        return True

    async def __call__(self, req):
        if type(req).__name__ == "SaveBigFilePartRequest":
            self._uploads.setdefault(req.file_id, {})[req.file_part] = req.bytes
            return True
        return SimpleNamespace(full_chat=SimpleNamespace(pinned_msg_id=None))

    async def iter_download(self, m, **kw):
        data = self._blobs.get(m.id, b"")
        for i in range(0, len(data), 512 * 1024):
            yield data[i:i + 512 * 1024]


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
        orphans, missing = await st.gc(dry_run=True)
        assert missing == [] or all(isinstance(x, str) for x in missing)

    async def t_reindex():
        before = st.get(f"{D}/one")
        await st.sync(full=True)
        after = st.get(f"{D}/one")
        assert (before is None) == (after is None)

    for name, fn in [
        ("空文件", t_empty), ("1字节", t_one_byte), ("中文+空格", t_unicode),
        ("特殊字符", t_special), ("深目录", t_deep), ("多分片7片", t_multislice),
        ("秒传共享分片", t_dedup), ("同名覆盖", t_overwrite), ("cp+mv", t_copy_move),
        ("删除文件", t_delete), ("递归删目录", t_rmdir), ("mkdir+空目录", t_mkdir),
        ("gc 预览", t_gc), ("reindex 全量", t_reindex),
    ]:
        await case(name, fn)

    # 收尾：删测试目录并提交索引
    try:
        if st.get(D):
            await st.delete(D)
    except Exception as e:
        print("清理失败:", e)

    await client.disconnect()
    ok = sum(1 for _, o, _ in RESULTS if o)
    print(f"\n===== PASS={ok} FAIL={len(RESULTS) - ok} =====")
    return 0 if ok == len(RESULTS) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
