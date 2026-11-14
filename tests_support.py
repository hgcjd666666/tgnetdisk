#!/usr/bin/env python3
"""测试公共设施：内存版 Telegram、缓存目录、WebDAV 夹具、小工具。

用例按功能拆在 tests_store / tests_race / tests_dav / tests_upload /
tests_cache / tests_edge 里，tests.py 只负责挑组跑。

用法（都在仓库根执行）:
    python tests.py              # 全量
    python tests.py -k 缓存      # 只跑名字含「缓存」的
    python tests.py --real       # 连真 Telegram
"""
import asyncio
import datetime
import os
import sys
import tempfile
import traceback
from types import SimpleNamespace

# 缓存目录必须是独立的临时目录：CACHE_DIR 在 import 时就固定了，先设再导
os.environ["TGND_CACHE"] = tempfile.mkdtemp(prefix="tgnd_test_cache_")
# 「上传完成即转本地缓存」单独用一个用例测，别的前提是「本地没缓存」
os.environ["TGND_CACHE_ADOPT"] = "0"

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import store as S  # noqa: E402
from store import (Store, SqliteBackend, make_session, is_db_msg, norm,  # noqa: E402
                   digest_of, LOCK_IDLE, LOCK_PREFIX, LockBusy)

RESULTS = []
D = "/_test"
BLOBS = os.path.join(S.CACHE_DIR, "blobs")

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
        self.send_fail_times = 0    # 接下来几次 send_file 直接失败（测建消息重试）
        self.rpc = 0                # 累计网络调用次数：用来断言「单设备模式的写操作不碰 TG」

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
        self.rpc += 1
        if self.send_fail_times > 0:
            self.send_fail_times -= 1
            raise RuntimeError("模拟 sendMedia 失败")
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
        self.rpc += 1
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
        self.rpc += 1
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
        self.rpc += 1
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
        self.rpc += 1
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
        self.rpc += 1
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




def blobs():
    try:
        return sorted(n for n in os.listdir(BLOBS)
                      if not n.startswith(".") and not n.endswith(S.PART_SUFFIX))
    except OSError:
        return []

def parts():
    try:
        return sorted(n for n in os.listdir(BLOBS) if n.endswith(S.PART_SUFFIX))
    except OSError:
        return []

def wipe_cache():
    for n in blobs() + parts():
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

async def make_dav(start_up=True, start_fill=True):
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer
    import server as srv
    c = FakeClient()
    s2 = Store(c, 1, backend=SqliteBackend(tempfile.mktemp(suffix=".db")))
    s2.fast = True
    up = srv.Uploader(s2, 1)
    fill = srv.Filler(s2)
    up.fill = fill
    app = web.Application(client_max_size=0)
    app.router.add_route("*", "/{tail:.*}", lambda r: srv.handle(r, s2, up, fill))
    cli = TestClient(TestServer(app))
    await cli.start_server()
    if start_up:
        up.start()
    if start_fill:
        fill.start()
    return cli, s2, up, c, srv

async def stop_dav(cli, up):
    for t in up.tasks:
        t.cancel()
    fill = getattr(up, "fill", None)
    if fill is not None:
        for t in fill.tasks:
            t.cancel()
    await cli.close()

async def wait_done(pred, tries=400):
    for _ in range(tries):
        await asyncio.sleep(0.05)
        if pred():
            return True
    return False
