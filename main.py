"""启动入口：锁、索引、Telegram、后台上传、WebDAV 监听。

两把锁：
    env 文件      本机只允许一个实例
    TG 快照 caption  全局只允许一个实例
任何一把失效，服务停止接受新请求、把在途的活干完、退出。
"""
import asyncio
import fcntl
import logging
import os
import signal
import sys
import time

from aiohttp import web
from telethon import TelegramClient

import cache
import logger
from index import Index
from tg import TGStore
from vfs import VFS
import webdav

log = logging.getLogger("main")

API_ID = int(os.environ.get("TG_API_ID", "0"))
API_HASH = os.environ.get("TG_API_HASH", "")
SESSION = os.environ.get("TG_SESSION", "tgnd")
CHAT = os.environ.get("TG_CHAT", "")
DB_PATH = os.environ.get("TGND_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), "tgnd.db"))
ENV_PATH = os.environ.get("TGND_ENV", os.path.join(os.path.dirname(os.path.abspath(__file__)), "env"))
PORT = int(os.environ.get("PORT", "8090"))
BIND = os.environ.get("DAV_BIND", "0.0.0.0")
SNAPSHOT_SEC = float(os.environ.get("SNAPSHOT_SEC", "10"))


# ---------- 本地单实例锁 ----------
class LocalLock:
    """flock 一个文件，全程持有。文件被删/被换掉（inode 变了）视为锁失效。"""

    def __init__(self, path: str):
        self.path = path
        self.fd = None
        self.ino = None

    async def acquire(self, tries: int = 6):
        """拿不到就 2^n 秒后重试（1,2,4,8,16,32），全失败返回 False。"""
        for n in range(tries):
            try:
                self.fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o644)
            except OSError as e:
                log.error("打不开锁文件 %s: %s", self.path, e)
                return False
            try:
                fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                st = os.fstat(self.fd)
                self.ino = (st.st_dev, st.st_ino)
                log.info("拿到本地锁 %s", self.path)
                return True
            except OSError:
                os.close(self.fd)
                self.fd = None
                wait = 2 ** n
                log.warning("本地锁被占用，%d 秒后重试（%d/%d）", wait, n + 1, tries)
                await asyncio.sleep(wait)
        return False

    def check(self) -> bool:
        """锁文件还在、还是当初那个 inode 吗。"""
        if self.fd is None:
            return False
        try:
            st = os.stat(self.path)
        except OSError:
            return False
        return (st.st_dev, st.st_ino) == self.ino

    def release(self):
        if self.fd is None:
            return
        try:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
        except OSError:
            pass
        try:
            os.close(self.fd)
        except OSError:
            pass
        self.fd = None


# ---------- 后台上传 ----------
class Uploader:
    def __init__(self, vfs: VFS, workers: int = 1):
        self.vfs = vfs
        self.workers = max(1, workers)
        self.wake = asyncio.Event()
        self.tasks = []
        self.busy = set()
        self.stopping = False
        self.fails = {}

    def start(self):
        for i in range(self.workers):
            self.tasks.append(asyncio.create_task(self._loop(), name=f"up-{i}"))
        pend = self.vfs.pending()
        log.info("后台上传已启动（%d 路），待传 %d 个", self.workers, len(pend))
        if pend:
            self.nudge()

    def nudge(self):
        self.wake.set()

    async def _loop(self):
        while not self.stopping:
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass
            self.wake.clear()
            for path, _h, _m in self.vfs.pending():
                if self.stopping:
                    return
                if path in self.busy:
                    continue
                self.busy.add(path)
                try:
                    await self.vfs.upload_pending(path)
                except Exception as e:
                    self.fails[path] = self.fails.get(path, 0) + 1
                    log.warning("上传失败 %s（第 %d 次）: %s", path, self.fails[path], e)
                    if self.fails[path] < 3:
                        self.nudge()
                finally:
                    self.busy.discard(path)

    async def stop(self):
        self.stopping = True
        self.wake.set()
        for t in self.tasks:
            t.cancel()
        for t in self.tasks:
            try:
                await t
            except BaseException:
                pass


# ---------- 后台维护 ----------
class Keeper:
    """快照备份 + 锁检查。"""

    def __init__(self, store: TGStore, vfs: VFS):
        self.store = store
        self.vfs = vfs
        self.stop = asyncio.Event()
        self.task = None

    def start(self):
        self.task = asyncio.create_task(self._loop(), name="keeper")

    async def _loop(self):
        last_check = 0.0
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=1.0)
                return
            except asyncio.TimeoutError:
                pass
            now = time.time()
            # 快照：有改动且空闲够久才推
            if self.store.dirty and now - self.store._last_write >= SNAPSHOT_SEC:
                try:
                    await self.store.push_snapshot()
                except Exception as e:
                    log.warning("后台快照失败: %s", e)
            # 锁：每 30 秒看一眼
            if now - last_check >= 30:
                last_check = now
                try:
                    st = await self.store.check_lock()
                except Exception as e:
                    log.warning("检查 TG 锁失败（继续运行）: %s", e)
                    continue
                if st == "own":
                    continue
                if st == "idle":
                    log.warning("TG 锁被清空，尝试重新获取")
                    try:
                        await self.store.acquire(timeout=10)
                        log.info("重新拿到 TG 锁")
                        continue
                    except Exception as e:
                        log.error("重新获取锁失败: %s", e)
                else:
                    log.error("TG 锁已失效（%s），准备退出", st)
                self.stop.set()

    async def close(self):
        self.stop.set()
        if self.task:
            try:
                await self.task
            except BaseException:
                pass


# ---------- WebDAV 桥接 ----------
def make_app(dav: webdav.DAV):
    app = web.Application(client_max_size=0)

    async def handler(request):
        res = await dav.handle(request)
        if isinstance(res, webdav.Response):
            return web.Response(status=res.status, body=res.body, headers=res.headers)
        status, headers, start, length = res
        if webdav._active_streams >= webdav.MAX_STREAMS:
            return web.Response(status=503, text="too many streams",
                                headers={"Retry-After": "2"})
        webdav._active_streams += 1
        try:
            resp = web.StreamResponse(status=status, headers=headers)
            await resp.prepare(request)
            async for chunk in dav.vfs.iter_content(request.path, start, length):
                await resp.write(chunk)
            await resp.write_eof()
            return resp
        except (ConnectionResetError, asyncio.CancelledError):
            raise
        finally:
            webdav._active_streams -= 1

    app.router.add_route("*", "/{tail:.*}", handler)
    return app


# ---------- 启动 ----------
async def ensure_chat(client, title="tgnd"):
    cfg = os.path.join(os.path.dirname(os.path.abspath(__file__)), "chat.json")
    if os.path.exists(cfg):
        try:
            import json
            return int(json.load(open(cfg))["chat"])
        except Exception:
            pass
    async for d in client.iter_dialogs():
        if d.is_channel and (d.title or "") == title:
            import json
            with open(cfg, "w") as f:
                json.dump({"chat": d.id}, f)
            return d.id
    from telethon.tl.functions.channels import CreateChannelRequest
    r = await client(CreateChannelRequest(title=title, about="tgnd storage", megagroup=True))
    cid = int(f"-100{r.chats[0].id}")
    import json
    with open(cfg, "w") as f:
        json.dump({"chat": cid}, f)
    return cid


async def main():
    logger.configure(log_file=os.environ.get("TGND_LOG", ""),
                     log_max=int(os.environ.get("TGND_LOG_MAX") or 0))
    os.makedirs(cache.CACHE_DIR, exist_ok=True)

    lock = LocalLock(ENV_PATH)
    if not await lock.acquire():
        log.error("另一个实例正在运行（%s 被占用），退出", ENV_PATH)
        return 1

    if not (API_ID and API_HASH):
        log.error("缺 TG_API_ID / TG_API_HASH")
        return 1

    client = TelegramClient(SESSION, API_ID, API_HASH, device_model="tgnd",
                            system_version="Linux", app_version="tgnd")
    await client.connect()
    if not await client.is_user_authorized():
        log.error("session 未授权")
        return 1
    me = await client.get_me()
    log.info("登录身份: %s", getattr(me, "username", None) or me.id)

    if CHAT:
        chat = int(CHAT) if str(CHAT).lstrip("-").isdigit() else CHAT
    else:
        chat = await ensure_chat(client)

    index = Index(DB_PATH)
    store = TGStore(client, chat, index, cache)
    vfs = VFS(index, store)

    # 锁：自己的残留直接接管（token 由 session 派生，重启后不变）；否则拉远端再抢
    st = await store.check_lock()
    if st == "own":
        log.info("接管上次留下的锁，以本地索引为准")
        store.holding = True
        store.dirty = index.meta("dirty") == "1"
    elif st in ("idle", "gone"):
        await store.pull_snapshot(merge=True)
        await store.acquire()
    else:
        log.error("TG 锁被另一个实例持有，本机不启动（本设计只允许一个 server）")
        return 1

    if "--reindex" in sys.argv:
        log.info("重建索引……")
        await store.reindex()
        await store.push_snapshot()
        await store.release()
        await client.disconnect()
        index.close()
        lock.release()
        return 0

    up = Uploader(vfs, int(os.environ.get("UPLOAD_QUEUE_WORKERS", "1") or 1))
    dav = webdav.DAV(vfs, up, readonly=os.environ.get("DAV_READONLY", "0") == "1")
    keeper = Keeper(store, vfs)

    up.start()
    keeper.start()

    app = make_app(dav)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, BIND, PORT)
    await site.start()
    log.info("WebDAV 已启动: http://%s:%d/", BIND, PORT)

    stop = asyncio.Event()
    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
            except NotImplementedError:
                pass
    except Exception:
        pass

    try:
        while not stop.is_set():
            if keeper.stop.is_set() or not lock.check():
                log.error("锁失效，准备退出")
                break
            try:
                await asyncio.wait_for(stop.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass
    finally:
        log.info("正在退出：停止接受新请求，等待在途任务")
        await site.stop()
        await up.stop()
        await keeper.close()
        try:
            if store.dirty:
                await store.push_snapshot()
        except Exception as e:
            log.warning("退出前快照失败: %s", e)
        try:
            await store.release()
        except Exception as e:
            log.warning("释放 TG 锁失败: %s", e)
        await runner.cleanup()
        await client.disconnect()
        lock.release()
        index.close()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        pass
