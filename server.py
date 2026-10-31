"""用 Telegram 当存储的 WebDAV 服务（aiohttp 实现）。

用法:
  export TG_API_ID=... TG_API_HASH=... TG_PHONE=+86... TG_CHAT=-100...
  export DAV_USER=admin DAV_PASS=xxx PORT=8087
  python server.py            # 首次会要求输入验证码
"""
import asyncio
import base64
import json
import logging
import hashlib
import os
import sys
import tempfile
import time
import uuid
from email.utils import formatdate, parsedate_to_datetime
from urllib.parse import quote, unquote, urlsplit

from aiohttp import web
from telethon import TelegramClient

import logger
from store import cache_file, LockBusy, make_session, is_path_session, Store, norm, parent_of, base_of

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

ALLOW = "OPTIONS, HEAD, GET, PUT, DELETE, PROPFIND, PROPPATCH, MKCOL, MOVE, COPY, LOCK, UNLOCK"
DAV_CLASS = "1, 2"


# ---------------- 工具 ----------------
def http_date(ts: float) -> str:
    return formatdate(ts or time.time(), usegmt=True)


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
        "Accept-Ranges": "none",
    })


async def dav_get(request, store):
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
        return web.Response(text=html, content_type="text/html; charset=utf-8")
    resp = web.StreamResponse(status=200, headers={
        "Content-Type": "application/octet-stream",
        "Content-Disposition": f"attachment; filename*=UTF-8''{quote(e.name)}",
        "Content-Length": str(e.size),
        "Last-Modified": http_date(e.mtime),
    })
    await resp.prepare(request)
    try:
        async for chunk in store.iter_content(path):
            await resp.write(chunk)
    except Exception as ex:
        log.warning("下载中断 %s: %s", path, ex)
    await resp.write_eof()
    return resp


async def dav_put(request, store):
    if READONLY:
        return web.Response(status=403, text="readonly")
    path = norm(request.path)
    e = store.get(path)
    if e and e.is_dir:
        return web.Response(status=409, text="is a collection")
    # 落到磁盘上的缓存目录，而不是 /tmp（tmpfs，等于内存）
    tmp = cache_file(hashlib.sha256(path.encode()).hexdigest()[:16] + ".part")
    fd = os.open(tmp, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(fd, "wb") as f:
            async for chunk in request.content.iter_chunked(1 << 20):
                f.write(chunk)
        # 客户端若带了 X-OC-Mtime（ownCloud/Nextcloud 约定）就用它，否则交给 store 取文件 mtime
        raw = request.headers.get("X-OC-Mtime") or request.headers.get("X-Update-Mtime")
        try:
            mtime = int(float(raw)) if raw and raw != "false" else None
        except ValueError:
            mtime = None
        if mtime is not None and mtime <= 0:
            mtime = None
        existed = store.get(path) is not None
        await store.put_file(path, tmp, mtime)
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
    await store.delete(path)
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
    if store.get(src) is None:
        return web.Response(status=404)
    if request.headers.get("Overwrite", "T") == "T":
        if store.get(dst) is None:
            await store.move(src, dst)
            return web.Response(status=201)
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
    if store.get(src) is None:
        return web.Response(status=404)
    if store.get(dst) is not None:
        await store.delete(dst)
    await store.copy(src, dst)
    return web.Response(status=201)


_LOCK_TOKENS: dict[str, str] = {}


async def dav_lock(request, store):
    path = norm(request.path)
    token = _LOCK_TOKENS.get(path) or f"opaquelocktoken:{uuid.uuid4()}"
    _LOCK_TOKENS[path] = token
    e = store.get(path)
    timeout = request.headers.get("Timeout", "Second-3600")
    body = f"""<?xml version="1.0" encoding="utf-8"?>
<D:prop xmlns:D="DAV:"><D:lockdiscovery><D:activelock>
<D:locktype><D:write/></D:locktype><D:lockscope><D:exclusive/></D:lockscope>
<D:depth>infinity</D:depth>
<D:timeout>{xesc(timeout)}</D:timeout>
<D:locktoken><D:href>{token}</D:href></D:locktoken>
</D:activelock></D:lockdiscovery></D:prop>"""
    return web.Response(status=200, body=body.encode(), content_type="application/xml",
                        headers={"Lock-Token": f"<{token}>"})


async def dav_unlock(request, store):
    path = norm(request.path)
    tok = request.headers.get("Lock-Token", "").strip("<>")
    if _LOCK_TOKENS.get(path) == tok:
        _LOCK_TOKENS.pop(path, None)
        return web.Response(status=204)
    return web.Response(status=409)


async def dav_proppatch(request, store):
    return web.Response(status=207, body=b'<?xml version="1.0"?><D:multistatus xmlns:D="DAV:"/>',
                        content_type="application/xml")


async def handle(request, store):
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
            return await dav_get(request, store)
        if m == "PUT":
            return await dav_put(request, store)
        if m == "DELETE":
            return await dav_delete(request, store)
        if m == "MKCOL":
            return await dav_mkcol(request, store)
        if m == "MOVE":
            return await dav_move(request, store)
        if m == "COPY":
            return await dav_copy(request, store)
        if m == "LOCK":
            return await dav_lock(request, store)
        if m == "UNLOCK":
            return await dav_unlock(request, store)
        if m == "PROPPATCH":
            return await dav_proppatch(request, store)
    except LockBusy:
        return web.Response(status=503, text="index lock busy, retry later",
                            headers={"Retry-After": "3"})
    except FileNotFoundError:
        return web.Response(status=404, text="not found")
    except FileExistsError:
        return web.Response(status=405, text="exists")
    except Exception as ex:
        log.exception("处理 %s %s 失败: %s", m, request.path, ex)
        return web.Response(status=500, text=str(ex))
    finally:
        # 写操作立刻把索引快照推上去，缩短两个节点互相覆盖的窗口
        if m in ("PUT", "DELETE", "MKCOL", "MOVE", "COPY") and store.dirty:
            try:
                await store.snapshot()
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
            json.dump({"chat": cid}, open(cfg_path, "w"))
            return cid
    from telethon.tl.functions.channels import CreateChannelRequest
    r = await client(CreateChannelRequest(title=title, about="tgnd storage", megagroup=True))
    cid = int(f"-100{r.chats[0].id}")
    json.dump({"chat": cid}, open(cfg_path, "w"))
    log.info("已创建存储群 %s (%s)", title, cid)
    return cid


async def main():
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
        os.makedirs(os.path.dirname(sess), exist_ok=True)
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

    app = web.Application(client_max_size=0)
    app.router.add_route("*", "/{tail:.*}", lambda r: handle(r, store))
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info("WebDAV 已启动: http://0.0.0.0:%d/", PORT)
    while True:
        await asyncio.sleep(30)
        try:
            if store.dirty:                 # 兜底：有未落盘的改动就补一次快照
                await store.snapshot()
        except Exception as e:
            log.warning("后台快照失败: %s", e)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
