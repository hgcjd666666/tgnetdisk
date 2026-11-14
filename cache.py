"""本地缓存：上传暂存与读缓存，一个预算、两条通道。

    $CACHE/staging/<hash>   上传中的字节（唯一实体）
    $CACHE/blobs/<hash>     读缓存（上传完成或从 TG 下完）

缓存键就是内容摘要 hash：内容一变 hash 就变，键自然失效，不需要指纹。
上传完成时 `rename(staging/<hash>, blobs/<hash>)`——同盘原子，一份字节
从头到尾只占一份磁盘。

预算只统计这两个目录的总和，但**只有 blobs 里的条目能被淘汰**：staging 里
的文件正在上传，删了就是丢数据。
"""
import os
import time

CACHE_DIR = os.environ.get("TGND_CACHE") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "cache")

DEFAULT_LIMIT_GB = 16.0


def _dir(name: str) -> str:
    d = os.path.join(CACHE_DIR, name)
    os.makedirs(d, exist_ok=True)
    return d


def blob_dir() -> str:
    return _dir("blobs")


def staging_dir() -> str:
    return _dir("staging")


def blob_path(h: str) -> str:
    return os.path.join(blob_dir(), h) if h else ""


def staging_path(h: str) -> str:
    return os.path.join(staging_dir(), h) if h else ""


def limit() -> int:
    """缓存总预算（字节）。CACHE_MAX_GB=0 表示不限制。"""
    try:
        gb = float(os.environ.get("CACHE_MAX_GB", DEFAULT_LIMIT_GB) or DEFAULT_LIMIT_GB)
    except ValueError:
        gb = DEFAULT_LIMIT_GB
    return int(gb * (1024 ** 3)) if gb > 0 else 0


def _scan(d: str):
    """返回 (总字节, [(mtime, size, path)])。点开头的临时文件计入占用但不进淘汰列表。"""
    total, items = 0, []
    try:
        it = os.scandir(d)
    except OSError:
        return 0, items
    with it:
        for de in it:
            try:
                if not de.is_file(follow_symlinks=False):
                    continue
                st = de.stat(follow_symlinks=False)
            except OSError:
                continue
            total += st.st_size
            if not de.name.startswith("."):
                items.append((st.st_mtime, st.st_size, de.path))
    return total, items


def usage():
    """(总占用, 可淘汰条目)。"""
    a, items = _scan(blob_dir())
    b, _ = _scan(staging_dir())
    return a + b, items


def trim(need: int = 0, keep: str = "") -> int:
    """按 mtime 从旧到新删 blobs，直到总量 + need 落回预算。返回释放的字节数。"""
    cap = limit()
    if cap <= 0:
        return 0
    total, items = usage()
    if total + need <= cap:
        return 0
    items.sort()
    freed = 0
    for _mt, size, p in items:
        if total + need <= cap:
            break
        if p == keep:
            continue
        try:
            os.unlink(p)
        except OSError:
            continue
        total -= size
        freed += size
    return freed


def find(h: str) -> str:
    """找这份内容的本地副本：blobs 优先，其次 staging（上传中）。没有返回空串。"""
    if not h:
        return ""
    p = blob_path(h)
    try:
        if os.path.getsize(p) > 0 or os.path.exists(p):
            return p
    except OSError:
        pass
    p = staging_path(h)
    try:
        if os.path.exists(p):
            return p
    except OSError:
        pass
    return ""


def touch(p: str):
    """刷新 LRU 位置。失败无所谓。"""
    try:
        os.utime(p, None)
    except OSError:
        pass


def commit(h: str) -> str:
    """上传完成：staging → blobs。返回缓存路径，失败返回空串。"""
    src, dst = staging_path(h), blob_path(h)
    if not h or not os.path.exists(src):
        return ""
    cap = limit()
    if cap > 0:
        size = os.path.getsize(src)
        if size > cap:
            return ""                     # 单份就超预算，别缓存了
        trim(need=0)                      # rename 不增加占用，先削回预算内
    try:
        os.replace(src, dst)
        return dst
    except OSError:
        return ""


def put(h: str, src: str) -> str:
    """把一份下完的临时文件放进缓存。装不下就返回空串（调用方照常交付字节）。"""
    cap = limit()
    if not h or not os.path.exists(src):
        return ""
    try:
        size = os.path.getsize(src)
    except OSError:
        return ""
    if size <= 0 or (cap > 0 and size > cap):
        return ""
    if cap > 0:
        trim(need=0)
    dst = blob_path(h)
    try:
        os.replace(src, dst)
        return dst
    except OSError:
        return ""


def drop(h: str):
    """删掉两份（缓存和暂存）。"""
    for p in (blob_path(h), staging_path(h)):
        if not p:
            continue
        try:
            os.unlink(p)
        except OSError:
            pass


def has_staging(h: str) -> bool:
    return bool(h) and os.path.exists(staging_path(h))


def sweep(max_age: float = 0.0) -> int:
    """删掉 staging 里的临时上传文件（点文件），返回清掉的文件数。

    这些是 PUT 写到一半留下的：正常路径会在 finally 里自己删，只有崩溃或
    进程被杀才会残留。它们计入预算却不会被 trim 淘汰，所以启动时必须清。
    """
    n = 0
    try:
        it = os.scandir(staging_dir())
    except OSError:
        return 0
    with it:
        for de in it:
            try:
                if not de.name.startswith(".") or not de.is_file(follow_symlinks=False):
                    continue
                st = de.stat(follow_symlinks=False)
                if max_age and time.time() - st.st_mtime < max_age:
                    continue
                os.unlink(de.path)
                n += 1
            except OSError:
                continue
    return n


# ---------- 上传预算 ----------
_reserved = 0


def ensure(size: int) -> bool:
    """这么大的文件还能不能落盘。超预算先淘汰缓存，仍不够就拒绝。"""
    cap = limit()
    if cap <= 0:
        return True
    total, _ = usage()
    need = size + _reserved
    if total + need > cap:
        trim(need=need)
        total, _ = usage()
    return total + need <= cap


class hold:
    """从「预检查」到「字节真的落进 staging」之间预留空间，防并发 PUT 撑爆预算。

        with cache.hold(size) as ok:
            if not ok: ...
    """

    def __init__(self, size: int):
        self.size = max(0, int(size))
        self._taken = False

    def __enter__(self):
        global _reserved
        if self.size <= 0:
            return True
        if not ensure(self.size):
            return False
        _reserved += self.size
        self._taken = True
        return True

    def __exit__(self, *exc):
        global _reserved
        # 注意：__enter__ 返回 False 时 __exit__ 照样会跑，没占用的不能退
        if self._taken:
            _reserved -= self.size
            self._taken = False
        return False
