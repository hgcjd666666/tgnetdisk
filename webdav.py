"""WebDAV 协议层：只做 HTTP ↔ VFS 的翻译。

支持的方法：OPTIONS / PROPFIND(Depth 0,1) / GET / HEAD / PUT / DELETE /
MKCOL / MOVE / COPY / PROPPATCH。不实现 LOCK（回 405）。
PUT 支持 Content-Range 断点续传（非标准扩展，curl / 部分客户端在用）。
"""
import asyncio
import base64
import hmac
import logging
import mimetypes
import os
import re
import time
from email.utils import formatdate, parsedate_to_datetime
from urllib.parse import quote, unquote, urlsplit

import cache as cachemod
from paths import bad, base, norm
import tg

log = logging.getLogger("dav")

DAV_CLASS = "1"
ALLOW = "OPTIONS, PROPFIND, HEAD, GET, PUT, DELETE, MKCOL, MOVE, COPY, PROPPATCH"
MAX_STREAMS = int(os.environ.get("MAX_STREAMS", "64"))
PROPFIND_MAX = int(os.environ.get("PROPFIND_MAX_ENTRIES", "50000"))
CHUNK = 1 << 20

_active_streams = 0


class Response:
    """aiohttp 之外的最小响应对象，方便离线测试。"""

    def __init__(self, status=200, body=b"", headers=None, text=None):
        self.status = status
        self.headers = dict(headers or {})
        if text is not None:
            body = text.encode("utf-8")
            self.headers.setdefault("Content-Type", "text/plain; charset=utf-8")
        self.body = body if isinstance(body, bytes) else bytes(body)


def http_date(ts) -> str:
    try:
        return formatdate(float(ts), usegmt=True)
    except (ValueError, OverflowError, OSError):
        return formatdate(usegmt=True)


def _ts(v):
    try:
        dt = parsedate_to_datetime(v)
        return None if dt is None else dt.timestamp()
    except (TypeError, ValueError, IndexError):
        return None


def etag_of(e) -> str:
    return '"%s-%d"' % (e.hash, e.size) if e and not e.is_dir else ""


def etag_match(header: str, etag: str) -> bool:
    """If-None-Match 的比较规则：弱比较，认 `*`，认逗号分隔的列表。"""
    if not header or not etag:
        return False
    for part in header.split(","):
        t = part.strip()
        if t == "*":
            return True
        if t.startswith("W/"):
            t = t[2:]
        if t == etag:
            return True
    return False


def mime_of(name: str) -> str:
    t, _ = mimetypes.guess_type(name or "")
    return t or "application/octet-stream"


def parse_range(v: str, size: int):
    """返回 (start, end) 或 "unsat"；语法不认识返回 None（按整份回 200）。"""
    if not v or not size:
        return None
    m = re.match(r"^\s*bytes\s*=\s*(\d*)\s*-\s*(\d*)\s*$", v)
    if not m:
        return None
    a, b = m.group(1), m.group(2)
    if not a and not b:
        return None
    if not a:                                   # bytes=-N 后缀
        n = int(b)
        if n <= 0:
            return "unsat"
        return (max(0, size - n), size - 1)
    start = int(a)
    end = int(b) if b else size - 1
    if start >= size or start > end:
        return "unsat"
    return (start, min(end, size - 1))


def parse_content_range(v: str):
    """bytes start-end/total → (start, end, total)；不认识返回 None。"""
    if not v:
        return None
    m = re.match(r"^\s*bytes\s+(\d+)-(\d+)/(\d+)\s*$", v)
    if not m:
        return None
    a, b, t = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if a > b or b >= t:
        return None
    return a, b, t


def parse_progress_query(v: str):
    """bytes */total 或 bytes */* → total（或 None）；不是进度查询返回 False。

    客户端靠它问「服务端已经收到第几个字节了」，不携带任何 body。
    必须单独拦住：parse_content_range 认不出来，落到普通 PUT 分支会把
    一个空 body 当成整份内容，直接把文件覆盖成 0 字节。
    """
    if not v:
        return False
    m = re.match(r"^\s*bytes\s+\*/(\d+|\*)\s*$", v)
    if not m:
        return False
    t = m.group(1)
    return None if t == "*" else int(t)


def href_for(path: str, is_dir: bool) -> str:
    p = quote(norm(path), safe="/")
    if is_dir and not p.endswith("/"):
        p += "/"
    return p


def xesc(s: str) -> str:
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace('"', "&quot;"))


def prop_xml(e, path: str, is_dir: bool) -> str:
    mtime = e.mtime if e else time.time()
    try:
        created = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(mtime))
    except (OverflowError, OSError, ValueError):
        created = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    size = "" if is_dir else str(e.size if e else 0)
    ctype = "httpd/unix-directory" if is_dir else mime_of(base(path))
    rt = "<D:resourcetype><D:collection/></D:resourcetype>" if is_dir else "<D:resourcetype/>"
    name = "/" if norm(path) == "/" else base(path)
    tag = etag_of(e)
    etag = f"<D:getetag>{xesc(tag)}</D:getetag>" if tag else ""
    return (f"<D:response><D:href>{xesc(href_for(path, is_dir))}</D:href>"
            f"<D:propstat><D:prop><D:displayname>{xesc(name)}</D:displayname>{rt}"
            f"<D:getcontentlength>{size}</D:getcontentlength>"
            f"<D:getcontenttype>{ctype}</D:getcontenttype>"
            f"<D:getlastmodified>{http_date(mtime)}</D:getlastmodified>"
            f"{etag}<D:creationdate>{created}</D:creationdate></D:prop>"
            f"<D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>")


class DAV:
    def __init__(self, vfs, uploader=None, readonly=False):
        self.vfs = vfs
        self.up = uploader
        self.readonly = readonly
        self.user = os.environ.get("DAV_USER", "")
        self.password = os.environ.get("DAV_PASS", "")
        self._resume = {}          # path -> {"tmp", "have", "want"}

    # ---------- 认证 ----------
    def authorized(self, request) -> bool:
        if not self.password:
            return True
        h = request.headers.get("Authorization", "")
        if not h.lower().startswith("basic "):
            return False
        try:
            raw = base64.b64decode(h[6:]).decode("utf-8", "replace")
            u, _, p = raw.partition(":")
            return (hmac.compare_digest(u.encode(), self.user.encode())
                    and hmac.compare_digest(p.encode(), self.password.encode()))
        except Exception:
            return False

    @staticmethod
    def unauthorized():
        return Response(401, text="auth required",
                        headers={"WWW-Authenticate": 'Basic realm="WebDAV"'})

    def _dest(self, request) -> str:
        d = request.headers.get("Destination", "")
        if not d:
            return ""
        p = unquote(urlsplit(d).path)
        if bad(p):
            return ""
        return norm(p)

    # ---------- 入口 ----------
    async def handle(self, request):
        if not self.authorized(request):
            return self.unauthorized()
        if bad(request.path):
            return Response(400, text="bad path")
        m = request.method.upper()
        try:
            if m == "OPTIONS":
                return Response(200, headers={"DAV": DAV_CLASS, "Allow": ALLOW,
                                              "MS-Author-Via": "DAV"})
            if m == "PROPFIND":
                return await self.propfind(request)
            if m == "HEAD":
                return await self.get(request, head=True)
            if m == "GET":
                return await self.get(request)
            if m == "PUT":
                return await self.put(request)
            if m == "DELETE":
                return await self.delete(request)
            if m == "MKCOL":
                return await self.mkcol(request)
            if m == "MOVE":
                return await self.move(request)
            if m == "COPY":
                return await self.copy(request)
            if m == "PROPPATCH":
                if self.readonly:
                    return Response(403, text="readonly")
                return Response(207, headers={"Content-Type": "application/xml"},
                                body=b'<?xml version="1.0"?><D:multistatus xmlns:D="DAV:"/>')
        except FileNotFoundError:
            return Response(404, text="not found")
        except NotADirectoryError as e:
            return Response(409, text=f"parent is not a collection: {e}")
        except FileExistsError as e:
            return Response(409, text=str(e) or "conflict")
        except PermissionError:
            return Response(409, text="conflict")
        except Exception as e:
            log.exception("处理 %s %s 失败: %s", m, request.path, e)
            return Response(500, text="internal error")
        finally:
            if m in ("PUT", "DELETE", "MKCOL", "MOVE", "COPY"):
                await self.after_write()
        return Response(405, text="method not allowed", headers={"Allow": ALLOW})

    async def after_write(self):
        """写操作只改本地索引；快照交给后台空闲时推。"""
        self.vfs.mark_dirty()
        if self.up is not None:
            self.up.nudge()

    # ---------- PROPFIND ----------
    async def propfind(self, request):
        path = norm(request.path)
        e = self.vfs.get(path)
        if e is None:
            return Response(404, text="not found")
        depth = (request.headers.get("Depth") or "1").strip().lower()
        if depth not in ("0", "1"):
            return Response(403, text="only Depth 0/1",
                            headers={"DAV": "1", "X-DAV-Error": "propfind-finite-depth"})
        items = [(path, e)]
        if depth == "1" and e.is_dir:
            kids = self.vfs.children(path)
            if len(kids) > PROPFIND_MAX:
                return Response(507, text="too many entries")
            items.extend((k.path, k) for k in kids)
        body = ('<?xml version="1.0" encoding="utf-8"?>\n<D:multistatus xmlns:D="DAV:">'
                + "".join(prop_xml(en, p, en.is_dir) for p, en in items)
                + "</D:multistatus>")
        return Response(207, body=body.encode("utf-8"),
                        headers={"Content-Type": 'application/xml; charset="utf-8"'})

    # ---------- GET / HEAD ----------
    async def get(self, request, head=False):
        global _active_streams
        path = norm(request.path)
        e = self.vfs.get(path)
        if e is None:
            return Response(404, text="not found")
        if e.is_dir:
            return Response(405, text="is a collection")
        etag = etag_of(e)
        mtime = e.mtime or time.time()
        headers = {
            "ETag": etag,
            "Last-Modified": http_date(mtime),
            "Accept-Ranges": "bytes",
            "Content-Type": mime_of(base(path)),
        }
        if etag_match(request.headers.get("If-None-Match"), etag):
            return Response(304, headers=headers)
        ims = request.headers.get("If-Modified-Since")
        if ims:
            ts = _ts(ims)
            if ts is not None and int(mtime) <= int(ts):
                return Response(304, headers=headers)
        rng = None
        if not head:
            # If-Range 是「Range 的校验器」：它说内容没变才做部分响应，
            # 说变了（或压根认不出来）就当没给 Range，整份回 200。
            ir = (request.headers.get("If-Range") or "").strip()
            fresh = True
            if ir:
                if etag and ir == etag:
                    fresh = True
                else:
                    ts = _ts(ir)
                    fresh = ts is not None and int(mtime) <= int(ts)
            if fresh:
                rng = parse_range(request.headers.get("Range", ""), e.size)
        if rng == "unsat":
            return Response(416, headers={**headers, "Content-Range": f"bytes */{e.size}"})
        if rng:
            start, end = rng
            headers["Content-Range"] = f"bytes {start}-{end}/{e.size}"
            headers["Content-Length"] = str(end - start + 1)
            status = 206
        else:
            start, end = 0, max(0, e.size - 1)
            headers["Content-Length"] = str(e.size)
            status = 200
        if head or e.size == 0:
            return Response(status, headers=headers)
        return (status, headers, start, end - start + 1)

    # 流式响应由 main 层负责（aiohttp 的 StreamResponse 需要 request），这里只算头部

    # ---------- PUT ----------
    async def put(self, request):
        if self.readonly:
            return Response(403, text="readonly")
        path = norm(request.path)
        e = self.vfs.get(path)
        if e is not None and e.is_dir:
            return Response(409, text="is a collection")
        existed = e is not None
        cl = request.content_length
        raw_cr = request.headers.get("Content-Range", "")
        q = parse_progress_query(raw_cr)
        if q is not False:
            # 进度查询：只回服务端已收到的长度，绝不写字节
            st = self._resume.get(path)
            off = st["have"] if st and (q is None or st["want"] == q) else 0
            return Response(308, headers={"X-Upload-Offset": str(off)})
        cr = parse_content_range(raw_cr)
        if cr:
            return await self._put_chunked(request, path, cr, existed)
        if request.headers.get("X-Content-Hash") and cl == 0:
            return self._teleport(request, path, existed)
        with cachemod.hold(cl or 0) as ok:
            if not ok:
                return Response(507, text="insufficient storage")
        tmp = self.vfs.staging_tmp()
        fd = os.open(tmp, os.O_CREAT | os.O_TRUNC | os.O_WRONLY
                     | getattr(os, "O_NOFOLLOW", 0), 0o600)
        dg = tg.Digest()
        checked = 0
        try:
            with os.fdopen(fd, "wb") as f:
                async for chunk in request.content.iter_chunked(CHUNK):
                    dg.update(chunk)
                    await asyncio.to_thread(f.write, chunk)
                    if not cl and dg.size - checked >= (64 << 20):
                        checked = dg.size
                        if not cachemod.ensure(dg.size):
                            return Response(507, text="insufficient storage")
            if cl is not None and dg.size != cl:
                log.debug("PUT %s 收到的字节数 %d 与声明 %d 不符", path, dg.size, cl)
                return Response(400, text="incomplete body")
            h = dg.hex()
            mtime = self._mtime(request)
            e2 = self.vfs.finish_put(path, tmp, h, dg.size, mtime)
            return Response(204 if existed else 201, headers={"ETag": etag_of(e2)})
        except ConnectionResetError:
            # 客户端中途跑了：字节全丢，索引一个字都不写
            log.debug("PUT %s 连接中断，已收 %d 字节", path, dg.size)
            return Response(400, text="aborted")
        finally:
            try:
                if os.path.exists(tmp):
                    os.unlink(tmp)
            except OSError:
                pass

    def _teleport(self, request, path, existed):
        """秒传：`PUT` 空 body + `X-Content-Hash`，服务端验过货就直接建路径。

        客户端要的 hash 就是 ETag 里冒号前面那截（`"<hash>-<size>"`），
        所以第一次老老实实传完、从响应的 ETag 里记住，之后同内容就不传了。

        `X-Content-Length` 可选：给了就和库里的 size 对一下，防止 128 位
        摘要各自算错或者客户端拿错 hash 时建出一条尺寸不对的路径。
        """
        h = (request.headers.get("X-Content-Hash") or "").strip().lower()
        if not re.fullmatch(r"[0-9a-f]{32}", h):
            return Response(400, text="bad hash")
        raw = request.headers.get("X-Content-Length")
        size = None
        if raw is not None:
            try:
                size = int(raw)
            except ValueError:
                return Response(400, text="bad content length")
            if size < 0:
                return Response(400, text="bad content length")
        entry, err = self.vfs.teleport(path, h, self._mtime(request), size)
        if err is not None:
            if err == "dir":
                return Response(409, text="is a collection")
            return Response(412, text=err, headers={"X-Content-Hash": h})
        log.debug("秒传 %s ← %s", path, h)
        return Response(204 if existed else 201,
                        headers={"ETag": etag_of(entry)})

    async def _put_chunked(self, request, path, cr, existed):
        """Content-Range 续传：分多次把同一个文件写完，最后一段收齐才算完成。

        状态（临时文件 + 累计摘要）挂在 self._resume[path] 上活过一次请求：
        摘要跟着分块累计，收尾时直接取 hex，不再把临时文件整个重读一遍。
        """
        start, end, total = cr
        self._expire_resume()
        st = self._resume.get(path)
        if st is None or st["want"] != total or start == 0:
            if st is not None:
                self._forget(path)          # 旧临时文件不能留在 staging 里
            st = {"tmp": self.vfs.staging_tmp("cr"), "have": 0, "want": total,
                  "dg": tg.Digest(), "ts": time.time()}
            self._resume[path] = st
        st["ts"] = time.time()
        if start != st["have"]:
            return Response(400, text="bad offset",
                            headers={"X-Upload-Offset": str(st["have"])})
        fd = os.open(st["tmp"], os.O_CREAT | os.O_WRONLY | os.O_APPEND
                     | getattr(os, "O_NOFOLLOW", 0), 0o600)
        got = 0
        try:
            with os.fdopen(fd, "wb") as f:
                os.ftruncate(f.fileno(), st["dg"].size)   # 强制文件长度 == 摘要长度
                async for chunk in request.content.iter_chunked(CHUNK):
                    await asyncio.to_thread(f.write, chunk)
                    st["dg"].update(chunk)   # 落盘之后才算，摘要与文件严格同步
                    got += len(chunk)
        except asyncio.CancelledError:
            self._sync(st)                   # 进程要退出：状态先对齐再抛
            raise
        except ConnectionResetError:
            self._sync(st)                   # 客户端跑了：保留已收字节，可以续
            log.debug("续传 %s 连接中断，已收 %d/%d", path, st["dg"].size, total)
            return Response(400, text="aborted")
        except OSError:
            self._sync(st)
            raise
        st["have"] = start + got
        if st["have"] < total:
            return Response(308, headers={"X-Upload-Offset": str(st["have"])})
        tmp = st["tmp"]
        h = st["dg"].hex()
        size = st["dg"].size
        self._resume.pop(path, None)
        e2 = self.vfs.finish_put(path, tmp, h, size, self._mtime(request))
        return Response(204 if existed else 201, headers={"ETag": etag_of(e2)})

    def _expire_resume(self, ttl=3600.0):
        """没人认领的续传状态不会永远占着 staging。"""
        now = time.time()
        for p in [p for p, s in self._resume.items() if now - s.get("ts", now) > ttl]:
            self._forget(p)

    def _forget(self, path: str):
        """丢掉一条续传状态，顺手删掉它的临时文件。"""
        st = self._resume.pop(path, None)
        if st and st.get("tmp"):
            try:
                os.unlink(st["tmp"])
            except OSError:
                pass

    def _sync(self, st):
        """异常中断后把磁盘长度对齐到已算入摘要的字节数。"""
        try:
            os.truncate(st["tmp"], st["dg"].size)
            st["have"] = st["dg"].size
        except OSError:
            self._resume = {k: v for k, v in self._resume.items() if v is not st}
            try:
                os.unlink(st["tmp"])
            except OSError:
                pass

    @staticmethod
    def _mtime(request) -> int:
        raw = request.headers.get("X-OC-Mtime") or request.headers.get("X-Update-Mtime")
        try:
            v = int(float(raw)) if raw and raw != "false" else None
        except (ValueError, OverflowError):
            v = None
        if v is not None and not (0 < v <= 2 ** 31 - 1):
            v = None
        return v or int(time.time())

    # ---------- DELETE / MKCOL ----------
    async def delete(self, request):
        if self.readonly:
            return Response(403, text="readonly")
        path = norm(request.path)
        if path == "/":
            return Response(403, text="cannot delete root")
        if self.vfs.get(path) is None:
            return Response(404, text="not found")
        await self.vfs.delete(path)
        return Response(204)

    async def mkcol(self, request):
        if self.readonly:
            return Response(403, text="readonly")
        path = norm(request.path)
        cur = self.vfs.get(path)
        if cur is not None:
            return Response(405, text="exists")
        await self.vfs.mkdir(path)
        return Response(201)

    # ---------- MOVE / COPY ----------
    async def _move_copy(self, request, is_move: bool):
        if self.readonly:
            return Response(403, text="readonly")
        src = norm(request.path)
        dst = self._dest(request)
        if not dst:
            return Response(400, text="no destination")
        if dst == src:
            return Response(204)
        se = self.vfs.get(src)
        if se is None:
            return Response(404, text="not found")
        if dst == "/" or src.startswith(dst.rstrip("/") + "/"):
            return Response(409, text="destination is an ancestor of source")
        if dst.startswith(src.rstrip("/") + "/"):
            return Response(409, text="cannot move into itself")
        de = self.vfs.get(dst)
        if de is not None:
            if de.is_dir:
                return Response(409, text="destination is a collection")
            if se.is_dir != de.is_dir:
                return Response(409, text="resource type mismatch")
            if (request.headers.get("Overwrite") or "T").strip().upper() == "F":
                return Response(412, text="destination exists")
        if is_move:
            await self.vfs.move(src, dst)
        else:
            await self.vfs.copy(src, dst)
        return Response(204 if de is not None else 201)

    async def move(self, request):
        return await self._move_copy(request, True)

    async def copy(self, request):
        return await self._move_copy(request, False)
