"""用 Telegram 当存储的 WebDAV 服务（aiohttp 实现）。

用法:
  export TG_API_ID=... TG_API_HASH=... TG_PHONE=+86... TG_CHAT=-100...
  export DAV_USER=admin DAV_PASS=xxx PORT=8087
  python server.py            # 首次会要求输入验证码
"""
import asyncio
import base64
import hashlib
import hmac
import json
import logging
import mimetypes
import os
import sys
import time
import uuid
from contextlib import ExitStack
from email.utils import formatdate, parsedate_to_datetime
from urllib.parse import quote, unquote, urlsplit

from aiohttp import web
from telethon import TelegramClient

import logger
from store import (cache_file, cache_key, cache_limit, CACHE_DIR, first_hole, LockBusy,
                   make_session, need_tg_lock, partial_commit, partial_open,
                   Store, norm, base_of, staging_dir, staging_hold,
                   staging_ok, staging_path)

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
MAX_STREAMS = int(os.environ.get("MAX_STREAMS", "64") or 64)   # 同时在吐字节的响应上限
FILL_CHUNK = 4 << 20                                        # 补洞一次拉多少
FILL_RATE = int(os.environ.get("FILL_RATE_KB", "2048") or 0) * 1024
FILL_YIELD = 8                                              # 有人在读时降到 1/8

PROPFIND_MAX = int(os.environ.get("PROPFIND_MAX_ENTRIES", "50000") or 50000)
GHOST_TTL = 24 * 3600      # 「上传中」放这么久还没动静，就当对方早没了，允许接管
ALLOW = "OPTIONS, HEAD, GET, PUT, DELETE, PROPFIND, PROPPATCH, MKCOL, MOVE, COPY"
DAV_CLASS = "1"            # 只宣告 Class 1：锁（Class 2）不实现，别假装支持


# ---------------- 工具 ----------------
STREAM_CHUNK = 1 << 20


def mime_of(name: str) -> str:
    """按扩展名猜类型：对客户端来说比一律 octet-stream 有用（预览/内联播放）。"""
    t, _ = mimetypes.guess_type(name or "")
    return t or "application/octet-stream"


def http_date(ts: float) -> str:
    try:
        return formatdate(ts or time.time(), usegmt=True)
    except (OverflowError, OSError, ValueError):
        return formatdate(time.time(), usegmt=True)


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
    """解析单区间 Range，返回 (start, end) / None（没给或语法错）/ "unsat"（要 416）。

    RFC 9110：语法错的 Range 必须忽略、回整份；只有「语法正确但取不到任何字节」
    才回 416。多区间也不支持，按允许的方式忽略掉。
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
                return None             # bytes=-0 之类：语法不成立，忽略
            return max(0, size - n), size - 1
        start = int(a)
        end = int(b) if b else size - 1
    except ValueError:
        return None                     # bytes=abc-def 之类：忽略
    if start < 0:
        return None
    if start >= size:
        return "unsat"                  # 起点超出文件：真取不到
    if start > end:
        return None                     # 显式倒序区间：语法不成立，忽略
    return start, min(end, size - 1)


def etag_of(e, total=None) -> str:
    """内容身份：hash + 大小。秒传/缓存天然一致，不用另算。"""
    if not e or not e.hash:
        return ""
    return '"%s-%d"' % (e.hash, e.size if total is None else total)


def _http_ts(value: str):
    try:
        return parsedate_to_datetime(value).timestamp()
    except Exception:
        return None


def is_fresh(request, etag: str, mtime: float) -> bool:
    """条件请求命中：客户端手上的副本就是当前内容。"""
    inm = request.headers.get("If-None-Match")
    if inm:
        tags = [t.strip().removeprefix("W/") for t in inm.split(",")]
        return "*" in tags or (bool(etag) and etag in tags)
    ims = request.headers.get("If-Modified-Since")
    if ims:
        ts = _http_ts(ims)
        if ts is not None:
            return int(mtime) <= int(ts)
    return False


def if_range_ok(request, etag: str, mtime: float) -> bool:
    """If-Range：跟当前内容对不上就让调用方忽略 Range、回整份。"""
    v = request.headers.get("If-Range")
    if not v:
        return True
    v = v.strip()
    if v.startswith('"') or v.startswith("W/"):
        return v == etag
    ts = _http_ts(v)
    return ts is not None and int(mtime) <= int(ts)


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
        # 恒定时间比较：别让响应时间把用户名/密码一位位漏出去
        return (hmac.compare_digest(u.encode("utf-8"), DAV_USER.encode("utf-8"))
                and hmac.compare_digest(p.encode("utf-8"), DAV_PASS.encode("utf-8")))
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
    try:
        created = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(mtime))
    except (OverflowError, OSError, ValueError):
        created = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    size = "" if is_dir else str(entry.size if entry else 0)
    ctype = "httpd/unix-directory" if is_dir else mime_of(entry.name if entry else path)
    rt = "<D:resourcetype><D:collection/></D:resourcetype>" if is_dir else "<D:resourcetype/>"
    name = "/" if norm(path) == "/" else base_of(path)
    tag = "" if is_dir else etag_of(entry)
    etag = f"<D:getetag>{xesc(tag)}</D:getetag>" if tag else ""
    return f"""<D:response>
<D:href>{xesc(href_for(path, is_dir))}</D:href>
<D:propstat><D:prop>
<D:displayname>{xesc(name)}</D:displayname>
{rt}
<D:getcontentlength>{size}</D:getcontentlength>
<D:getcontenttype>{ctype}</D:getcontenttype>
<D:getlastmodified>{http_date(mtime)}</D:getlastmodified>
{etag}
<D:creationdate>{created}</D:creationdate>
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
    e = store.visible(path)
    if e is None:
        return web.Response(status=404, text="not found")
    depth = (request.headers.get("Depth") or "1").strip().lower()
    if depth not in ("0", "1"):
        # 只支持 Depth 0/1。infinity 要把整库拼成一份 XML（几万条就是几十 MB），
        # 客户端也基本不等它；直接拒掉，让它逐层走。
        body = (b'<?xml version="1.0" encoding="utf-8"?>'
                b'<D:error xmlns:D="DAV:"><D:propfind-finite-depth/></D:error>')
        return web.Response(status=403, body=body, content_type="application/xml")
    parts = [prop_xml(e, path, e.is_dir)]
    if e.is_dir and depth == "1":
        kids = store.children(path)
        if len(kids) > PROPFIND_MAX:
            log.warning("PROPFIND %s 子项 %d 个，超过上限 %d", path, len(kids), PROPFIND_MAX)
            return web.Response(status=507, text="too many entries in one collection")
        for c in kids:
            parts.append(prop_xml(c, c.path, c.is_dir))
    body = ('<?xml version="1.0" encoding="utf-8"?>\n<D:multistatus xmlns:D="DAV:">'
            + "".join(parts) + "</D:multistatus>")
    return web.Response(status=207, body=body.encode("utf-8"),
                        headers={"Content-Type": "application/xml; charset=utf-8"})


async def dav_head(request, store):
    path = norm(request.path)
    e = store.visible(path)
    if e is None:
        return web.Response(status=404)
    if e.is_dir:
        return web.Response(status=200, content_type="httpd/unix-directory",
                            headers={"Last-Modified": http_date(e.mtime)})
    # HEAD 和 GET 必须给出一致的结论：CLI 切片上传中的文件 staging 没有字节，
    # GET 会 503，HEAD 就不能先报 200 + 一个根本拿不到的长度。
    uploading = e.status == 1 or bool(e.expect and len(e.parts) < e.expect)
    total = e.size
    if uploading:
        try:
            total = os.path.getsize(staging_path(e.hash))
        except OSError:
            return web.Response(status=404)
    etag = etag_of(e, total)
    last = http_date(e.mtime)
    if is_fresh(request, etag, e.mtime):
        h = {"Last-Modified": last}
        if etag:
            h["ETag"] = etag
        return web.Response(status=304, headers=h)
    headers = {
        "Content-Length": str(total),
        "Content-Type": mime_of(e.name),
        "Last-Modified": last,
        "Accept-Ranges": "bytes",
    }
    if etag:
        headers["ETag"] = etag
    return web.Response(status=200, headers=headers)


async def dav_get(request, store, fill=None):
    global ACTIVE_STREAMS
    path = norm(request.path)
    e = store.visible(path)          # 上传中且本机没有那份缓冲 → 当作不存在
    if e is None:
        return web.Response(status=404, text="not found")
    if e.is_dir:
        kids = store.children(path)
        if len(kids) > PROPFIND_MAX:
            return web.Response(status=507, text="too many entries in one collection")
        rows = []
        for c in kids:
            icon = "🗀" if c.is_dir else "🗎"
            rows.append(f'<li><a href="{xesc(href_for(c.path, c.is_dir))}">{icon} {xesc(c.name)}</a></li>')
        html = f"<html><body><h3>{xesc(path)}</h3><ul>{''.join(rows)}</ul></body></html>"
        return web.Response(text=html, content_type="text/html", charset="utf-8")

    if ACTIVE_STREAMS >= MAX_STREAMS:
        # 每个流都在占 TG 带宽/本地 fd；不限的话几十个并发 Range 就能把
        # 上传队列和补洞全饿死
        return web.Response(status=503, text="too many concurrent streams",
                            headers={"Retry-After": "5"})
    # 能走本地文件的两种情形：还没传完（staging）或命中读缓存。
    # 这两条路径支持 seek，Range 直接落到底层 fd 上，不用重下前面的字节。
    uploading = e.status == 1 or bool(e.expect and len(e.parts) < e.expect)
    etag = etag_of(e)
    if not uploading and is_fresh(request, etag, e.mtime):
        h = {"Last-Modified": http_date(e.mtime)}
        if etag:
            h["ETag"] = etag
        return web.Response(status=304, headers=h)
    fh = None
    if uploading:
        try:
            fh = open(staging_path(e.hash), "rb")
        except OSError:
            # 这台设备没有那份缓冲：对它来说就是没有这个文件
            return web.Response(status=404, text="not found")
    else:
        hit = store.cached_path(path)
        if hit:
            try:
                fh = open(hit, "rb")
            except OSError:
                fh = None
    total = os.fstat(fh.fileno()).st_size if fh is not None else e.size

    rng = parse_range(request.headers.get("Range"), total)
    if rng == "unsat":
        if fh is not None:
            fh.close()
        return web.Response(status=416, headers={"Content-Range": f"bytes */{total}"})
    if rng and not if_range_ok(request, etag, e.mtime):
        rng = None                      # If-Range 不匹配：按整份回，别拼出坏文件
    start, end = rng if rng else (0, total - 1)
    n = end - start + 1 if total else 0

    headers = {
        "Content-Type": mime_of(e.name),
        "Content-Disposition": f"attachment; filename*=UTF-8''{quote(e.name)}",
        "Content-Length": str(n),
        "Last-Modified": http_date(e.mtime),
        "Accept-Ranges": "bytes",
    }
    if etag:
        headers["ETag"] = etag
    if rng:
        headers["Content-Range"] = f"bytes {start}-{end}/{total}"
    resp = web.StreamResponse(status=206 if rng else 200, headers=headers)
    await resp.prepare(request)
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
    if e is not None and e.status == 1 and not os.path.exists(staging_path(e.hash)):
        # 本机没有这份缓冲 = 上传是别的设备发起的：那个路径对它来说是「存在但
        # 不可读」，不该被这边顶掉。本机有缓冲（自己上一次没传完）时允许覆盖，
        # 那是同一台设备在改自己的东西。
        # 超过 GHOST_TTL 还没动静的当作幽灵（对方早就不在了），允许接管。
        if time.time() - (e.mtime or 0) < GHOST_TTL:
            return web.Response(status=409, text="file is being uploaded by another node",
                                headers={"Retry-After": "5"})
        log.warning("接管陈旧的上传中记录: %s（%d 秒没动静）", path, time.time() - (e.mtime or 0))
    # 预检查时就把这一份的大小记进预留：两个 PUT 同时进来，谁都不该以为
    # 预算还是空的。chunked 没有 Content-Length，只能边写边看。
    cl = request.content_length
    # 预留只需要覆盖「字节还在临时文件里」这段：一旦 os.replace 进 staging，
    # 它就已经计入 _cache_usage 了，再留着预留会让后面的 cache_adopt 误判超限。
    with ExitStack() as budget:
        ok = budget.enter_context(staging_hold(cl or 0))
        if not ok:
            return web.Response(status=507, text="insufficient storage, raise CACHE_MAX_GB")
        # 落到磁盘，而不是 /tmp（tmpfs，等于内存）。
        # 名字必须带唯一后缀：只按路径生成的话，同路径两个并发 PUT 会写同一个文件。
        tmp = cache_file(f"put-{os.getpid()}-{uuid.uuid4().hex[:12]}.part")
        fd = os.open(tmp, os.O_CREAT | os.O_TRUNC | os.O_WRONLY
                     | getattr(os, "O_NOFOLLOW", 0), 0o600)
        h = hashlib.shake_128()      # 落盘的同时把 hash 算出来，省一次全量读
        size = 0
        checked = 0
        try:
            with os.fdopen(fd, "wb") as f:
                async for chunk in request.content.iter_chunked(1 << 20):
                    h.update(chunk)
                    size += len(chunk)
                    await asyncio.to_thread(f.write, chunk)
                    # 没有 Content-Length 时按 64MB 粒度回头看一眼预算，
                    # 别让一次误操作先写满盘再报错
                    if not cl and size - checked >= (64 << 20):
                        checked = size
                        if not staging_ok(size):
                            return web.Response(status=507,
                                                text="insufficient storage, raise CACHE_MAX_GB")
            digest = h.hexdigest(16)
            # 客户端若带了 X-OC-Mtime（ownCloud/Nextcloud 约定）就用它，否则交给 store 取文件 mtime
            raw = request.headers.get("X-OC-Mtime") or request.headers.get("X-Update-Mtime")
            try:
                mtime = int(float(raw)) if raw and raw != "false" else None
            except (ValueError, OverflowError):
                mtime = None
            # 只收 1970..2038 之间的秒数：inf/nan/超大整数会一路炸到 SQLite
            # 和 http_date()，留一个脏值进索引后面每个 PROPFIND 都会 500
            if mtime is not None and not (0 < mtime <= 2 ** 31 - 1):
                mtime = None
            existed = e is not None
            if size == 0:
                # 空文件没有字节要传，直接登记
                await store.put_file(path, tmp, mtime)
                return web.Response(status=204 if existed else 201)
            # 实际字节比声明的多：把差额也过一遍预算（预留只覆盖了 cl）
            if cl and size > cl and not staging_ok(size - cl):
                return web.Response(status=507, text="insufficient storage, raise CACHE_MAX_GB")
            src = staging_path(digest)
            os.replace(tmp, src)             # 原子改名：staging 里要么没有、要么是完整的一份
            budget.pop_all().close()         # 字节已落盘，预留还回去
            ent = await store.register(path, src, digest, size, mtime)
            if ent is not None and ent.status == 0:
                store.staging_finish(path, digest)  # 秒传命中，字节根本不用传
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
    await store.delete(path)          # 索引一次摘掉，staging 由 store 内部收
    return web.Response(status=204)


async def dav_mkcol(request, store):
    if READONLY:
        return web.Response(status=403)
    path = norm(request.path)
    if store.get(path) is not None:
        return web.Response(status=405, text="exists")
    await store.mkdir(path)
    return web.Response(status=201)


def bad_path(p: str) -> bool:
    """控制字符（含 NUL）不能进索引：SQLite 收得下，但查询、日志、HTTP 头全会被它搞坏。"""
    return any(ord(c) < 0x20 or ord(c) == 0x7f for c in (p or ""))


def _dest_path(request) -> str:
    d = request.headers.get("Destination", "")
    if not d:
        return ""
    p = unquote(urlsplit(d).path)
    if bad_path(p):
        return ""
    return norm(p)


async def dav_move(request, store):
    if READONLY:
        return web.Response(status=403)
    src = norm(request.path)
    dst = _dest_path(request)
    if not dst:
        return web.Response(status=400, text="no destination")
    if dst == src:
        return web.Response(status=204)          # 移到自己：什么都不用做
    se = store.visible(src)
    if se is None:
        return web.Response(status=404)
    # 目标是自己的祖先（含根）时，下面会先 delete(dst)——那会把源子树一起删掉。
    # 必须在删除之前拦下来。
    if dst == "/" or src.startswith(dst.rstrip("/") + "/"):
        return web.Response(status=409, text="destination is an ancestor of source")
    if dst.startswith(src.rstrip("/") + "/"):
        return web.Response(status=409, text="cannot move into itself")
    de = store.get(dst)
    if de is not None and store.visible(dst) is None:
        # 目标是个本机看不见的「上传中」：别的设备正占着这个路径
        return web.Response(status=409, text="destination is being uploaded")
    if de is not None:
        if de.is_dir:
            # RFC 4918：集合不能被资源覆盖。以前是先递归删目标再移，等于
            # 一个请求头就把整个目录抹了，还回 204。
            return web.Response(status=409, text="destination is a collection")
        if se.is_dir != de.is_dir:
            return web.Response(status=409, text="resource type mismatch")
        if request.headers.get("Overwrite", "T").strip().upper() == "F":
            return web.Response(status=412, text="destination exists")
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
    se = store.visible(src)
    if se is None:
        return web.Response(status=404)
    if dst == "/" or src.startswith(dst.rstrip("/") + "/"):
        return web.Response(status=409, text="destination is an ancestor of source")
    if dst.startswith(src.rstrip("/") + "/"):
        return web.Response(status=409, text="cannot copy into itself")
    de = store.get(dst)
    if de is not None and store.visible(dst) is None:
        return web.Response(status=409, text="destination is being uploaded")
    if de is not None:
        if de.is_dir:
            return web.Response(status=409, text="destination is a collection")
        if se.is_dir != de.is_dir:
            return web.Response(status=409, text="resource type mismatch")
        if request.headers.get("Overwrite", "T").strip().upper() == "F":
            return web.Response(status=412, text="destination exists")
        await store.copy(src, dst)
        return web.Response(status=204)
    await store.copy(src, dst)
    return web.Response(status=201)


async def dav_proppatch(request, store):
    if READONLY:
        return web.Response(status=403, text="readonly")
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
        self.fails = {}
        self.tasks = []
        self.retries = set()

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
                n = self.fails.get(path, 0) + 1
                if n <= 3:
                    self.fails[path] = n
                    t = asyncio.create_task(self._retry(path, n))
                    self.retries.add(t)
                    t.add_done_callback(self.retries.discard)
                else:
                    self.fails.pop(path, None)
                    log.error("补洞连续失败 %d 次，放弃 %s", n, path)
            else:
                self.fails.pop(path, None)
            finally:
                self.seen.discard(path)

    async def _retry(self, path: str, n: int):
        """退避一会儿再排一次：网络抖一下就放弃的话，缓存永远补不齐。"""
        try:
            await asyncio.sleep(30 * n)
            self.want(path)
        except asyncio.CancelledError:
            raise

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
                    await asyncio.to_thread(os.pwrite, fd, chunk, hole + pos)
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
        if await asyncio.to_thread(partial_commit, key, e.hash, e.size):
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

    def _gc_staging(self, age: float = 0):
        """清掉没人认领的 staging：比如落盘之后、登记之前被 kill 的那一份。

        age>0 时只清「够老」的。运行期扫必须带 age：PUT 把文件 os.replace
        进 staging 之后、register() 之前有一个短窗口，此刻索引里还查不到它，
        抢在这个窗口里删掉就等于把刚传完的字节扔了。
        """
        pend = {d for _p, d, *_ in self.store.backend.pending()}
        try:
            names = os.listdir(staging_dir())
        except OSError:
            return
        now = time.time()
        for n in names:
            if n.startswith(".") or n in pend:
                continue
            p = os.path.join(staging_dir(), n)
            try:
                if age and now - os.path.getmtime(p) < age:
                    continue
                os.unlink(p)
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
            try:
                self._gc_staging(age=600)     # 运行期也收孤儿，10 分钟内的一律当在写
            except Exception as ex:
                log.warning("staging 巡检失败: %s", ex)

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
                    n = min(self.fails.get(digest, 0) + 1, 20)
                    self.fails[digest] = n
                    delay = min(300, 2 ** min(n, 12))
                    self.retry_at[digest] = time.monotonic() + delay
                    if n <= 6:
                        log.warning("后台上传 %s 第 %d 次失败（%ds 后重试）: %s: %s",
                                    path, n, delay, type(ex).__name__, ex)
                    else:
                        # 不放弃：网络恢复了就该自己继续，别逼人重启进程
                        log.error("后台上传 %s 连续失败 %d 次，每 %ds 重试一次；"
                                  "本地副本留在 %s", path, n, delay, staging_path(digest))
                finally:
                    self.busy.discard(path)

    async def _settle(self, path: str, digest: str):
        e = self.store.get(path)
        if not e or e.is_dir or e.status != 1 or e.hash != digest:
            # 传之前就被删掉/被覆盖了，这份字节已经没人要
            self.store.staging_release(digest)
            return
        # 同一份内容已经被别的路径传完了（并发上传同内容），直接秒传落地
        for opath, osize, oparts in self.store.backend.find_by_hash(digest):
            if opath != path and self.store._dedup_source_ok(opath, osize, oparts):
                self.store.backend.set_file(path, digest, e.size, e.mtime,
                                            dict(oparts), 0, 0)
                self.store.dirty = True
                log.info("后台秒传落地: %s（复用 %s）", path, opath)
                self.store.staging_finish(path, digest)
                return
        src = staging_path(digest)
        await self.store.upload_pending(path, src)
        self.ok += 1
        log.info("后台上传完成: %s", path)
        self.store.staging_finish(path, digest)   # 字节就在手边：留成本地缓存，或收掉


async def handle(request, store, up, fill=None):
    if not is_authorized(request):
        return unauthorized()
    if bad_path(request.path):
        return web.Response(status=400, text="bad path")
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
        # 内部异常只进日志：把 str(ex) 回给客户端等于把路径、SQLite 错误、
        # 甚至文件系统结构泄给任何人
        log.exception("处理 %s %s 失败: %s", m, request.path, ex)
        return web.Response(status=500, text="internal error")
    finally:
        # 写操作只改本地索引：推送快照是后台的事（server 主循环里 backup_if_idle）。
        # 在请求路径上压缩整库 + 上传要几秒，MOVE 这种小操作会被它拖住。
        if m in ("PUT", "DELETE", "MKCOL", "MOVE", "COPY"):
            store.kick_backup()
    return web.Response(status=405, text="method not allowed",
                        headers={"Allow": ALLOW})


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
    # 单设备模式本地库就是权威，重启时再从 TG 拉一份旧快照覆盖它纯属多余；
    # 跨设备模式或第一次运行（本地库是空的）才需要先同步下来。
    if need_tg_lock() or store.is_empty():
        await store.load()
    up = Uploader(store, int(os.environ.get("UPLOAD_QUEUE_WORKERS", "1") or 1))
    up.start()
    fill = Filler(store)
    fill.start()

    app = web.Application(client_max_size=0)
    app.router.add_route("*", "/{tail:.*}", lambda r: handle(r, store, up, fill))
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    bind = os.environ.get("DAV_BIND", "0.0.0.0")
    if not DAV_PASS:
        log.warning("DAV_PASS 未设置：WebDAV 不需要认证，任何能连到 %s:%d 的人都能读写删。"
                    "绑公网前务必设置 DAV_PASS，或把 DAV_BIND 改成 127.0.0.1", bind, PORT)
    site = web.TCPSite(runner, bind, PORT)
    await site.start()
    log.info("WebDAV 已启动: http://%s:%d/", bind, PORT)
    # 后台备份：写操作只改本地索引，这里在「空闲 SNAPSHOT_SEC 秒」后统一推一次。
    # 连续上传/改名的时候不会反复压缩上传整库，安静下来才落一次盘。
    try:
        while True:
            try:
                await asyncio.wait_for(store._backup_wake.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass
            store._backup_wake.clear()
            try:
                await store.backup_if_idle()
            except Exception as e:
                log.warning("后台备份失败: %s", e)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        # 退出前把没备份的改动推上去：等空闲 10 秒就没人做了
        try:
            if store.dirty:
                await asyncio.shield(store.backup_if_idle(0))
        except Exception as e:
            log.warning("退出前备份失败: %s", e)
        try:
            await store.wait_released()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
