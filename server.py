"""用 Telegram 当存储的 WebDAV 服务（aiohttp 实现）。

用法:
  export TG_API_ID=... TG_API_HASH=... TG_PHONE=+86... TG_CHAT=-100...
  export DAV_USER=admin DAV_PASS=xxx PORT=8087
  python server.py            # 首次会要求输入验证码
"""
import asyncio
import base64
import hashlib
import json
import logging
import os
import sys
import time
import uuid
from email.utils import formatdate
from urllib.parse import quote, unquote, urlsplit

from aiohttp import web
from telethon import TelegramClient

import logger
from store import (cache_file, cache_key, cache_limit, CACHE_DIR, first_hole, LockBusy,
                   make_session, is_online, partial_commit, partial_open,
                   Store, norm, base_of, staging_dir, staging_drop, staging_ok,
                   staging_path)

log = logging.getLogger("dav")

API_ID = int(os.environ.get("TG_API_ID", "0"))
API_HASH = os.environ.get("TG_API_HASH", "")
PHONE = os.environ.get("TG_PHONE", "")
CHAT = os.environ.get("TG_CHAT", "")
SESSION = os.environ.get("TG_SESSION", "/opt/tgnd/session")
DEVICE = os.environ.get("TG_DEVICE", "tgnd")
PORT = int(os.environ.get("PORT", "8087"))
DAV_USER = os.environ.get("DAV_USER", "admin")
DAV_PASS = os.environ.get("DAV_PASS", "")
READONLY = os.environ.get("DAV_READONLY", "0") == "1"

# 正在给客户端吐字节的响应数。补洞靠它判断「现在忙不忙」，忙就降速让路。
ACTIVE_STREAMS = 0
FILL_CHUNK = 4 << 20                                        # 补洞一次拉多少
FILL_RATE = int(os.environ.get("FILL_RATE_KB", "2048") or 0) * 1024
FILL_YIELD = 8                                              # 有人在读时降到 1/8

ALLOW = "OPTIONS, HEAD, GET, PUT, DELETE, PROPFIND, PROPPATCH, MKCOL, MOVE, COPY"
DAV_CLASS = "1"            # 只宣告 Class 1：锁（Class 2）不实现，别假装支持


# ---------------- 工具 ----------------
STREAM_CHUNK = 1 << 20


def http_date(ts: float) -> str:
    return formatdate(ts or time.time(), usegmt=True)


def fs_block_size(fd=None) -> int:
    """文件系统块大小 = SEEK_HOLE 的判定粒度。别写死 4096。"""
    try:
        blk = os.statvfs(CACHE_DIR).f_frsize
    except OSError:
        blk = 0
    if not blk and fd is not None:
        try:
            blk = os.fstat(fd).st_blksize
        except OSError:
            blk = 0
    return blk or 4096


def parse_range(value, size: int):
    """解析单区间 Range，返回 (start, end) / None（当没给）/ "bad"（要回 416）。

    多区间不支持：RFC 9110 允许服务端忽略 Range 直接回整份，这比自己做 multipart
    简单也安全得多。播放器拖进度条用的都是单区间，够。
    """
    if not value or size <= 0 or not value.startswith("bytes="):
        return None
    spec = value[6:].strip()
    if "," in spec:
        return None
    a, sep, b = spec.partition("-")
    if not sep:
        return None
    try:
        if not a:                       # bytes=-N：最后 N 字节
            n = int(b)
            if n <= 0:
                return "bad"
            return max(0, size - n), size - 1
        start = int(a)
        end = int(b) if b else size - 1
    except ValueError:
        return "bad"
    if start < 0 or start > end or start >= size:
        return "bad"
    return start, min(end, size - 1)


def xesc(s: str) -> str:
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace('"', "&quot;"))


def href_for(path: str, is_dir: bool) -> str:
    p = quote(norm(path), safe="/")
    if is_dir and not p.endswith("/"):
        p += "/"
    return p


def is_authorized(request) -> bool:
    if not DAV_PASS:
        return True
    h = request.headers.get("Authorization", "")
    if not h.lower().startswith("basic "):
        return False
    try:
        raw = base64.b64decode(h[6:]).decode("utf-8", "replace")
        u, _, p = raw.partition(":")
        return u == DAV_USER and p == DAV_PASS
    except Exception:
        return False


def unauthorized() -> web.Response:
    return web.Response(
        status=401,
        headers={"WWW-Authenticate": 'Basic realm="WebDAV"'},
        text="auth required",
    )


def prop_xml(entry, path, is_dir) -> str:
    mtime = entry.mtime if entry else time.time()
    size = "" if is_dir else str(entry.size if entry else 0)
    ctype = "httpd/unix-directory" if is_dir else "application/octet-stream"
    rt = "<D:resourcetype><D:collection/></D:resourcetype>" if is_dir else "<D:resourcetype/>"
    name = "/" if norm(path) == "/" else base_of(path)
    return f"""<D:response>
<D:href>{xesc(href_for(path, is_dir))}</D:href>
<D:propstat><D:prop>
<D:displayname>{xesc(name)}</D:displayname>
{rt}
<D:getcontentlength>{size}</D:getcontentlength>
<D:getcontenttype>{ctype}</D:getcontenttype>
<D:getlastmodified>{http_date(mtime)}</D:getlastmodified>
<D:creationdate>{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(mtime))}</D:creationdate>
<D:supportedlock><D:lockentry><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockentry></D:supportedlock>
</D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat>
</D:response>"""


# ---------------- WebDAV 方法 ----------------
async def dav_options(request):
    return web.Response(status=200, headers={
        "DAV": DAV_CLASS,
        "Allow": ALLOW,
        "MS-Author-Via": "DAV",
    })


async def dav_propfind(request, store):
    path = norm(request.path)
    e = store.get(path)
    if e is None:
        return web.Response(status=404, text="not found")
    depth = request.headers.get("Depth", "1")
    parts = [prop_xml(e, path, e.is_dir)]
    if e.is_dir and depth in ("1", "infinity"):
        if depth == "infinity":
            stack = [path]
            while stack:
                cur = stack.pop()
                for c in store.children(cur):
                    parts.append(prop_xml(c, c.path, c.is_dir))
                    if c.is_dir:
                        stack.append(c.path)
        else:
            for c in store.children(path):
                parts.append(prop_xml(c, c.path, c.is_dir))
    body = ('<?xml version="1.0" encoding="utf-8"?>\n<D:multistatus xmlns:D="DAV:">'
            + "".join(parts) + "</D:multistatus>")
    return web.Response(status=207, body=body.encode("utf-8"),
                        headers={"Content-Type": "application/xml; charset=utf-8"})


async def dav_head(request, store):
    path = norm(request.path)
    e = store.get(path)
    if e is None:
        return web.Response(status=404)
    if e.is_dir:
        return web.Response(status=200, content_type="httpd/unix-directory")
    return web.Response(status=200, headers={
        "Content-Length": str(e.size),
        "Content-Type": "application/octet-stream",
        "Last-Modified": http_date(e.mtime),
        "Accept-Ranges": "bytes",
    })


async def dav_get(request, store, fill=None):
    path = norm(request.path)
    e = store.get(path)
    if e is None:
        return web.Response(status=404, text="not found")
    if e.is_dir:
        rows = []
        for c in store.children(path):
            icon = "🗀" if c.is_dir else "🗎"
            rows.append(f'<li><a href="{xesc(href_for(c.path, c.is_dir))}">{icon} {xesc(c.name)}</a></li>')
        html = f"<html><body><h3>{xesc(path)}</h3><ul>{''.join(rows)}</ul></body></html>"
        return web.Response(text=html, content_type="text/html", charset="utf-8")

    # 能走本地文件的两种情形：还没传完（staging）或命中读缓存。
    # 这两条路径支持 seek，Range 直接落到底层 fd 上，不用重下前面的字节。
    uploading = e.status == 1 or bool(e.expect and len(e.parts) < e.expect)
    fh = None
    if uploading:
        try:
            fh = open(staging_path(e.hash), "rb")
        except OSError:
            # Content-Length 一旦发出去客户端会等满超时，所以这里先拒绝
            return web.Response(status=503, text="file is being uploaded or incomplete",
                                headers={"Retry-After": "3"})
    else:
        hit = store.cached_path(path)
        if hit:
            try:
                fh = open(hit, "rb")
            except OSError:
                fh = None
    total = os.fstat(fh.fileno()).st_size if fh is not None else e.size

    rng = parse_range(request.headers.get("Range"), total)
    if rng == "bad":
        if fh is not None:
            fh.close()
        return web.Response(status=416, headers={"Content-Range": f"bytes */{total}"})
    start, end = rng if rng else (0, total - 1)
    n = end - start + 1 if total else 0

    headers = {
        "Content-Type": "application/octet-stream",
        "Content-Disposition": f"attachment; filename*=UTF-8''{quote(e.name)}",
        "Content-Length": str(n),
        "Last-Modified": http_date(e.mtime),
        "Accept-Ranges": "bytes",
    }
    if rng:
        headers["Content-Range"] = f"bytes {start}-{end}/{total}"
    resp = web.StreamResponse(status=206 if rng else 200, headers=headers)
    await resp.prepare(request)
    global ACTIVE_STREAMS
    ACTIVE_STREAMS += 1
    pfd = None
    try:
        if fh is not None:
            if start:
                fh.seek(start)
            left = n
            while left > 0:
                b = await asyncio.to_thread(fh.read, min(STREAM_CHUNK, left))
                if not b:
                    break
                left -= len(b)
                await resp.write(b)
        elif rng:
            # 边发给客户端边落到部分缓存：稀疏文件，写过的区间就是已缓存，
            # 「填到哪了」交给 SEEK_HOLE，不用另外记账。
            key = cache_key(e)
            # FILL_RATE=0 就是关掉「边看边缓存」：没人补洞的话，留下一个永远
            # 填不满的 .part 只是占地方
            pfd = partial_open(key, total) if (key and FILL_RATE > 0) else None
            if pfd is not None:
                # SEEK_HOLE 的粒度是文件系统块，不是字节。只写半个块的话整块都会
                # 被当成已缓存，块内没写的部分是 0，补洞会跳过它，最后 hash 对不上。
                # 所以只把「完全落在请求范围内的完整块」写进缓存，不足一块的边缘
                # 直接丢掉——反正客户端已经收到了，下次补齐会连着那个块一起拉。
                blk = fs_block_size(pfd)
                ws = (start + blk - 1) // blk * blk          # 向上对齐
                we = (end + 1) // blk * blk                  # 向下对齐
                left = n
                pos = start
                async for chunk in store.iter_content(path, offset=start, length=n):
                    if we > ws:
                        cs, ce = pos, pos + len(chunk)
                        a, b = max(cs, ws), min(ce, we)
                        if b > a:
                            try:
                                os.pwrite(pfd, chunk[a - cs:b - cs], a)
                            except OSError as ex:
                                log.warning("部分缓存写入失败 %s: %s", path, ex)
                                try:
                                    os.close(pfd)
                                except OSError:
                                    pass
                                pfd = None
                    pos += len(chunk)
                    if left > 0:
                        if len(chunk) > left:
                            chunk = chunk[:left]
                        left -= len(chunk)
                        await resp.write(chunk)
            else:
                async for chunk in store.iter_content(path, offset=start, length=n):
                    await resp.write(chunk)
        else:
            # 没 Range 就别传 offset/length：传了 iter_content 会当成部分读，
            # 于是不写缓存、不校验全文件 hash。
            async for chunk in store.iter_content(path):
                await resp.write(chunk)
    except Exception as ex:
        log.warning("下载中断 %s: %s", path, ex)
        # 状态行和 Content-Length 已经发出去，只能硬断连接让客户端报错；
        # 调 write_eof 会因为长度不足再抛一次，客户端反而挂到超时。
        try:
            resp.force_close()
        except Exception:
            pass
        return resp
    finally:
        ACTIVE_STREAMS -= 1
        if pfd is not None:
            try:
                os.close(pfd)
            except OSError:
                pass
        if fh is not None:
            fh.close()
    await resp.write_eof()
    if rng and fh is None and fill is not None:
        # 这一段给完了，剩下的洞交给后台按空闲带宽慢慢补
        fill.want(path)
    return resp


async def dav_put(request, store, up):
    if READONLY:
        return web.Response(status=403, text="readonly")
    path = norm(request.path)
    e = store.get(path)
    if e and e.is_dir:
        return web.Response(status=409, text="is a collection")
    # 客户端先报了大小就提前判断，装不下别白传一遍
    cl = request.content_length
    if cl and cl > 0 and not staging_ok(cl):
        return web.Response(status=507, text="insufficient storage, raise CACHE_MAX_GB")
    # 落到磁盘，而不是 /tmp（tmpfs，等于内存）。
    # 名字必须带唯一后缀：只按路径生成的话，同路径两个并发 PUT 会写同一个文件。
    tmp = cache_file(f"put-{os.getpid()}-{uuid.uuid4().hex[:12]}.part")
    fd = os.open(tmp, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
    h = hashlib.shake_128()          # 落盘的同时把 hash 算出来，省一次全量读
    size = 0
    try:
        with os.fdopen(fd, "wb") as f:
            async for chunk in request.content.iter_chunked(1 << 20):
                h.update(chunk)
                size += len(chunk)
                f.write(chunk)
        digest = h.hexdigest(16)
        # 客户端若带了 X-OC-Mtime（ownCloud/Nextcloud 约定）就用它，否则交给 store 取文件 mtime
        raw = request.headers.get("X-OC-Mtime") or request.headers.get("X-Update-Mtime")
        try:
            mtime = int(float(raw)) if raw and raw != "false" else None
        except ValueError:
            mtime = None
        if mtime is not None and mtime <= 0:
            mtime = None
        existed = e is not None
        if size == 0:
            # 空文件没有字节要传，直接登记
            await store.put_file(path, tmp, mtime)
            return web.Response(status=204 if existed else 201)
        if not staging_ok(size):
            return web.Response(status=507, text="insufficient storage, raise CACHE_MAX_GB")
        src = staging_path(digest)
        os.replace(tmp, src)             # 原子改名：staging 里要么没有、要么是完整的一份
        ent = await store.register(path, src, digest, size, mtime)
        if ent is not None and ent.status == 0:
            staging_drop(digest)         # 秒传命中，字节根本不用传
        else:
            up.nudge()                   # 剩下的交给后台，请求立刻返回
        return web.Response(status=204 if existed else 201)
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass


async def dav_delete(request, store):
    if READONLY:
        return web.Response(status=403)
    path = norm(request.path)
    if path == "/":
        return web.Response(status=403, text="cannot delete root")
    if store.get(path) is None:
        return web.Response(status=404)
    doomed = [x.hash for x in (store.get(q) for q in store.backend.paths_under(path))
              if x and not x.is_dir and x.status == 1 and x.hash]
    await store.delete(path)
    # 队列不存 path，靠索引现扫；这里只要把没人认领的 staging 收掉
    alive = {d for _p, d, *_ in store.backend.pending()}
    for d in set(doomed):
        if d not in alive:
            staging_drop(d)
    return web.Response(status=204)


async def dav_mkcol(request, store):
    if READONLY:
        return web.Response(status=403)
    path = norm(request.path)
    if store.get(path) is not None:
        return web.Response(status=405, text="exists")
    await store.mkdir(path)
    return web.Response(status=201)


def _dest_path(request) -> str:
    d = request.headers.get("Destination", "")
    if not d:
        return ""
    return norm(unquote(urlsplit(d).path))


async def dav_move(request, store):
    if READONLY:
        return web.Response(status=403)
    src = norm(request.path)
    dst = _dest_path(request)
    if not dst:
        return web.Response(status=400, text="no destination")
    if dst == src:
        return web.Response(status=204)          # 移到自己：什么都不用做
    # 目标是自己祖先（含根）时，下面会先 delete(dst)——那会把源子树一起删掉。
    # 必须在删除之前拦下来。
    if dst == "/" or src.startswith(dst.rstrip("/") + "/"):
        return web.Response(status=409, text="destination is an ancestor of source")
    if store.get(src) is None:
        return web.Response(status=404)
    ow = request.headers.get("Overwrite", "T").strip().upper()
    exists = store.get(dst) is not None
    if exists and ow == "F":
        return web.Response(status=412, text="destination exists")
    if exists:
        await store.delete(dst)
        await store.move(src, dst)
        return web.Response(status=204)
    await store.move(src, dst)
    return web.Response(status=201)


async def dav_copy(request, store):
    if READONLY:
        return web.Response(status=403)
    src = norm(request.path)
    dst = _dest_path(request)
    if not dst:
        return web.Response(status=400)
    if dst == src:
        return web.Response(status=403, text="copy onto itself")
    if dst == "/" or src.startswith(dst.rstrip("/") + "/"):
        return web.Response(status=409, text="destination is an ancestor of source")
    if store.get(src) is None:
        return web.Response(status=404)
    ow = request.headers.get("Overwrite", "T").strip().upper()
    exists = store.get(dst) is not None
    if exists and ow == "F":
        return web.Response(status=412, text="destination exists")
    if exists:
        await store.delete(dst)
        await store.copy(src, dst)
        return web.Response(status=204)
    await store.copy(src, dst)
    return web.Response(status=201)


async def dav_proppatch(request, store):
    return web.Response(status=207, body=b'<?xml version="1.0"?><D:multistatus xmlns:D="DAV:"/>',
                        content_type="application/xml")


class Filler:
    """Range 请求把文件「开个头」之后，空下来把剩下的洞补齐。

    进度不用另外存：partial 文件是稀疏的，SEEK_HOLE 指到哪里还没写就是进度。
    补完 → 校验 hash → 打洞 → rename 成正式缓存条目。有客户端在读的时候降速，
    没人的时候放开跑，这样缓存吃的是空闲带宽。
    """

    def __init__(self, store):
        self.store = store
        self.q = asyncio.Queue()
        self.seen = set()
        self.tasks = []

    def start(self):
        self.tasks.append(asyncio.create_task(self._loop(), name="tgnd-fill"))

    def want(self, path: str):
        if cache_limit() <= 0 or FILL_RATE <= 0 or path in self.seen:
            return
        self.seen.add(path)
        self.q.put_nowait(path)

    async def _loop(self):
        while True:
            path = await self.q.get()
            try:
                await self._one(path)
            except Exception as ex:
                log.warning("补洞失败 %s: %s: %s", path, type(ex).__name__, ex)
            finally:
                self.seen.discard(path)

    async def _one(self, path: str):
        e = self.store.get(path)
        if not e or e.is_dir or e.status == 1 or not e.size:
            return
        key = cache_key(e)
        if not key or e.size > cache_limit():
            return
        if self.store.cached_path(path):
            return                                  # 已经收齐了
        fd = partial_open(key, e.size)
        if fd is None:
            return
        sent = 0
        t0 = time.monotonic()
        try:
            while True:
                hole = first_hole(fd, e.size)
                if hole is None:
                    break
                n = min(FILL_CHUNK, e.size - hole)
                pos = 0
                async for chunk in self.store.iter_content(path, offset=hole, length=n):
                    os.pwrite(fd, chunk, hole + pos)
                    pos += len(chunk)
                    sent += len(chunk)
                if pos == 0:
                    break                           # 拉不到数据，别在这儿转圈
                rate = FILL_RATE // (FILL_YIELD if ACTIVE_STREAMS else 1)
                due = sent / rate
                spent = time.monotonic() - t0
                while due > spent + 0.05:
                    await asyncio.sleep(min(due - spent, 1.0))
                    spent = time.monotonic() - t0
        finally:
            os.close(fd)
        if partial_commit(key, e.hash, e.size):
            log.info("缓存补齐 %s (%.1f MB，补了 %.1f MB)",
                     path, e.size / 1048576, sent / 1048576)


class Uploader:
    """落盘即返回之后的「送上传」队列。

    不维护队列本身：每次都从索引里现挑 status=1 且有 staging 文件的记录。
    这样 move 改了路径、delete 删了条目、同名覆盖顶掉了旧内容，队列自动跟上，
    不需要额外写一套「队列里那条还有没有效」的同步逻辑。
    """

    def __init__(self, store, workers: int = 1):
        self.store = store
        self.workers = max(1, workers)
        self.wake = asyncio.Event()
        self.tasks = []
        self.busy = set()              # 正在传的 path，防同一条被两个 worker 抢
        self.dead = set()              # 重试到上限的 digest，不再自动重试
        self.retry_at = {}             # digest -> 下次允许重试的 monotonic
        self.fails = {}
        self.ok = 0

    def _gc_staging(self):
        """清掉没人认领的 staging：比如落盘之后、登记之前被 kill 的那一份。"""
        pend = {d for _p, d, *_ in self.store.backend.pending()}
        try:
            names = os.listdir(staging_dir())
        except OSError:
            return
        for n in names:
            if n.startswith(".") or n in pend:
                continue
            try:
                os.unlink(os.path.join(staging_dir(), n))
                log.info("清掉无主的 staging: %s", n[:12])
            except OSError:
                pass

    def start(self):
        self._gc_staging()
        for i in range(self.workers):
            self.tasks.append(asyncio.create_task(self._loop(), name=f"tgnd-up-{i}"))
        self.tasks.append(asyncio.create_task(self._tick(), name="tgnd-up-tick"))
        pend = self.store.backend.pending()
        log.info("后台上传已启动（%d 路），待上传 %d 个", self.workers, len(pend))
        if pend:
            self.nudge()

    def nudge(self):
        self.wake.set()

    async def _tick(self):
        """兜底：失败退避之后、以及重启恢复出来的任务，都要有人踢一脚。"""
        while True:
            await asyncio.sleep(30)
            self.wake.set()

    def _pick(self):
        now = time.monotonic()
        for path, digest, _expect, _size, _mtime in self.store.backend.pending():
            if not digest or path in self.busy or digest in self.dead:
                continue
            if self.retry_at.get(digest, 0) > now:
                continue
            try:
                if not os.path.exists(staging_path(digest)):
                    continue
            except OSError:
                continue
            self.busy.add(path)
            return path, digest
        return None

    async def _loop(self):
        while True:
            await self.wake.wait()
            self.wake.clear()
            while True:
                job = self._pick()
                if not job:
                    break
                path, digest = job
                try:
                    await self._settle(path, digest)
                    self.fails.pop(digest, None)
                    self.retry_at.pop(digest, None)
                except Exception as ex:
                    n = self.fails.get(digest, 0) + 1
                    self.fails[digest] = n
                    delay = min(300, 2 ** n)
                    self.retry_at[digest] = time.monotonic() + delay
                    log.warning("后台上传 %s 第 %d 次失败（%ds 后重试）: %s: %s",
                                path, n, delay, type(ex).__name__, ex)
                    if n >= 6:
                        self.dead.add(digest)
                        log.error("放弃自动重试 %s：本地副本留在 %s，索引里仍是「上传中」",
                                  path, staging_path(digest))
                finally:
                    self.busy.discard(path)

    def _needed(self, digest: str) -> bool:
        """还有没有别的记录在等这份 staging。"""
        for _path, d, *_ in self.store.backend.pending():
            if d == digest:
                return True
        return False

    async def _settle(self, path: str, digest: str):
        e = self.store.get(path)
        if not e or e.is_dir or e.status != 1 or e.hash != digest:
            # 传之前就被删掉/被覆盖了，这份字节已经没人要
            if not self._needed(digest):
                staging_drop(digest)
            return
        # 同一份内容已经被别的路径传完了（并发上传同内容），直接秒传落地
        for opath, osize, oparts in self.store.backend.find_by_hash(digest):
            if opath != path and self.store._dedup_source_ok(opath, osize, oparts):
                self.store.backend.set_file(path, digest, e.size, e.mtime,
                                            dict(oparts), 0, 0)
                self.store.dirty = True
                log.info("后台秒传落地: %s（复用 %s）", path, opath)
                if not self._needed(digest):
                    staging_drop(digest)
                return
        src = staging_path(digest)
        await self.store.upload_pending(path, src)
        self.ok += 1
        log.info("后台上传完成: %s", path)
        if not self._needed(digest):
            staging_drop(digest)


async def handle(request, store, up, fill=None):
    if not is_authorized(request):
        return unauthorized()
    m = request.method.upper()
    try:
        if m == "OPTIONS":
            return await dav_options(request)
        if m == "PROPFIND":
            return await dav_propfind(request, store)
        if m == "HEAD":
            return await dav_head(request, store)
        if m == "GET":
            return await dav_get(request, store, fill)
        if m == "PUT":
            return await dav_put(request, store, up)
        if m == "DELETE":
            return await dav_delete(request, store)
        if m == "MKCOL":
            return await dav_mkcol(request, store)
        if m == "MOVE":
            return await dav_move(request, store)
        if m == "COPY":
            return await dav_copy(request, store)
        if m == "PROPPATCH":
            return await dav_proppatch(request, store)
    except LockBusy:
        return web.Response(status=503, text="index lock busy, retry later",
                            headers={"Retry-After": "3"})
    except FileNotFoundError:
        return web.Response(status=404, text="not found")
    except FileExistsError as ex:
        # 父路径已经是文件，或同名资源类型冲突：RFC 4918 要 409
        return web.Response(status=409, text=str(ex) or "conflict")
    except Exception as ex:
        log.exception("处理 %s %s 失败: %s", m, request.path, ex)
        return web.Response(status=500, text=str(ex))
    finally:
        # 写操作后把索引推上去，缩短两个节点互相覆盖的窗口。
        # 有租约任务在跑时交给它（空闲 3 秒统一落库），避免每个请求都 edit 两次；
        # 没有租约时才自己推，且必须走 snapshot_locked（抢锁）：
        # 裸调 snapshot() 会在不持锁的情况下改快照 caption，把别的节点的锁清掉。
        if m in ("PUT", "DELETE", "MKCOL", "MOVE", "COPY") and store.dirty:
            t = store._lease_task
            if t is None or t.done():
                try:
                    if is_online():
                        await store.snapshot_locked(nonblock=True)   # 别在锁上等满 60 秒
                    else:
                        await store.snapshot()
                except LockBusy:
                    pass
                except Exception as ex:
                    log.warning("写后快照失败: %s", ex)
    return web.Response(status=405, text="method not allowed")


async def ensure_chat(client, cfg_path="/opt/tgnd/chat.json", title="tgnd") -> int:
    """复用/创建私有超级群作为存储空间。"""
    if os.path.exists(cfg_path):
        try:
            return int(json.load(open(cfg_path))["chat"])
        except Exception:
            pass
    async for d in client.iter_dialogs():
        if d.is_channel and (d.title or "") == title:
            cid = d.id
            with open(cfg_path, "w") as f:
                json.dump({"chat": cid}, f)
            return cid
    from telethon.tl.functions.channels import CreateChannelRequest
    r = await client(CreateChannelRequest(title=title, about="tgnd storage", megagroup=True))
    cid = int(f"-100{r.chats[0].id}")
    with open(cfg_path, "w") as f:
        json.dump({"chat": cid}, f)
    log.info("已创建存储群 %s (%s)", title, cid)
    return cid


async def main():
    os.environ.setdefault("TGND_NODE", "server")   # 和 cli 用不同的锁身份
    logger.configure(log_file=os.environ.get("TGND_LOG", ""),
                     log_max=int(os.environ.get("TGND_LOG_MAX") or 0),
)
    # WEBDAV=0：只跑 CLI，不起 HTTP 服务（连同 Telegram 连接都不必建立）
    if os.environ.get("WEBDAV", "1").strip().lower() in ("0", "false", "no", "off"):
        log.info("WEBDAV=0，跳过 WebDAV 启动（只用命令行）")
        return
    if not (API_ID and API_HASH):
        sys.exit("缺 TG_API_ID / TG_API_HASH")
    sess = make_session(SESSION)
    if isinstance(sess, str):
        d = os.path.dirname(sess)            # TG_SESSION=session.session 时是空串，makedirs("") 会崩
        if d:
            os.makedirs(d, exist_ok=True)
    client = TelegramClient(sess, API_ID, API_HASH, device_model=DEVICE,
                            system_version="Linux", app_version="tgnd")
    await client.connect()
    if not await client.is_user_authorized():
        sys.exit("session 未授权")
    me = await client.get_me()
    log.info("登录身份: %s (%s)", getattr(me, "username", None) or me.id, me.phone)
    if CHAT:
        chat = int(CHAT) if str(CHAT).lstrip("-").isdigit() else CHAT
    else:
        chat = await ensure_chat(client)
    store = Store(client, chat)
    await store.load()
    up = Uploader(store, int(os.environ.get("UPLOAD_QUEUE_WORKERS", "1") or 1))
    up.start()
    fill = Filler(store)
    fill.start()

    app = web.Application(client_max_size=0)
    app.router.add_route("*", "/{tail:.*}", lambda r: handle(r, store, up, fill))
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info("WebDAV 已启动: http://0.0.0.0:%d/", PORT)
    while True:
        await asyncio.sleep(30)
        try:
            if store.dirty:                 # 兜底：有未落盘的改动就补一次快照
                try:
                    # 同样必须抢锁：裸 snapshot() 会改掉别的节点持有的 caption
                    if is_online():
                        await store.snapshot_locked(nonblock=True)
                    else:
                        await store.snapshot()
                except LockBusy:
                    pass
        except Exception as e:
            log.warning("后台快照失败: %s", e)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
