"""统一输出层 + 定长环形日志文件。

    TGND_LOG=路径         日志落这个文件（定长环形，写满回头覆盖旧的）
    TGND_LOG_MAX=字节数    正文区大小，默认 1 MiB

不落盘的东西：进度。tty 上原地覆盖；重定向到文件时按 3 秒节流写进环形日志，
不会无限增长（那个文件本身也封顶）。

日志文件格式（第一行是 64 字节信息区，过滤掉第一行剩下的就是日志）：
    [0:16]   信息区：b"TGNDLOG" + 正文大小(4B BE) + 写入指针(4B BE) + 1B 保留
    [16]     b"\n" 行分隔
    [17:]    正文区，固定长度，指针处是下一次写入位置

写满后从正文区头部继续覆盖，所以可能有一处断行（接受的代价）。
读的时候从指针位置绕一圈就是时间顺序。
"""
import os
import sys
import time

MAGIC = b"TGNDLOG"
INFO_LEN = 16                      # 信息区固定 16 字节（8 magic + 4 size + 4 pos）
HDR_LEN = INFO_LEN + 1             # 信息区 + 一个 \n 行分隔

_LOG = os.environ.get("TGND_LOG", "")
_tty = sys.stdout.isatty()
_prog_ts = 0.0


class RingLog:
    """定长环形日志：文件大小恒定，新日志覆盖最旧的。"""

    def __init__(self, path: str, body: int):
        self.path = path
        self.body = max(256, int(body))
        if os.path.exists(path) and os.path.getsize(path) >= HDR_LEN:
            self.fp = open(path, "r+b")
            self.fp.seek(0)
            raw = self.fp.read(HDR_LEN)
            try:
                if raw[:7] != MAGIC:
                    raise ValueError("magic")
                self.body = int.from_bytes(raw[8:12], "big") or self.body
                self.pos = int.from_bytes(raw[12:16], "big") % self.body
            except Exception:
                self.pos = 0
            if os.path.getsize(path) != HDR_LEN + self.body:
                self.fp.truncate(0)
                self._init_file()
        else:
            self.fp = open(path, "w+b")
            self._init_file()

    def _hdr(self, pos: int) -> bytes:
        """64 字节信息区 + \n：magic | 正文大小 | 写入指针 | 48 字节预留。"""
        return (MAGIC + self.body.to_bytes(4, "big") + pos.to_bytes(4, "big")
                + b"\n")

    def _init_file(self):
        self.pos = 0
        self.fp.seek(0)
        self.fp.write(self._hdr(0))
        self.fp.write(b"\0" * self.body)
        self.fp.flush()

    def write(self, line: str):
        data = (line.rstrip("\n") + "\n").encode("utf-8", "replace")
        if len(data) > self.body:
            data = data[-self.body:]
        off = self.pos
        first = min(len(data), self.body - off)
        self.fp.seek(HDR_LEN + off)
        self.fp.write(data[:first])
        if first < len(data):
            self.fp.seek(HDR_LEN)
            self.fp.write(data[first:])
        self.pos = (off + len(data)) % self.body
        self.fp.seek(0)
        self.fp.write(self._hdr(self.pos))
        self.fp.flush()


_ring: RingLog | None = None
_ring_tried = False
_MAX = int(os.environ.get("TGND_LOG_MAX") or (1 << 20))


def configure(log_file: str = "", log_max: int = 0):
    """进程入口统一调一次：决定日志落哪、多大。"""
    global _LOG, _MAX, _ring, _ring_tried
    _LOG = log_file or _LOG
    if log_max:
        _MAX = int(log_max)
    _ring = None
    _ring_tried = False


def _get_ring():
    global _ring, _ring_tried
    if _ring is None and not _ring_tried:
        _ring_tried = True
        if _LOG:
            try:
                _ring = RingLog(_LOG, _MAX)
            except Exception:
                _ring = None
    return _ring


def log(msg="", flush=True):
    """普通消息：一行一条，同时写环形日志文件。"""
    print(msg, flush=flush)
    ring = _get_ring()
    if ring:
        try:
            ring.write(str(msg))
        except Exception:
            pass


def progress(msg):
    """进度：tty 原地覆盖；非 tty（nohup/重定向）3 秒节流写进环形日志。"""
    global _prog_ts
    if _tty:
        sys.stdout.write("\r\033[K" + msg)
        sys.stdout.flush()
        return
    now = time.time()
    if now - _prog_ts >= 3:
        _prog_ts = now
        log(msg)


def done():
    """进度收尾换行。"""
    if _tty:
        sys.stdout.write("\n")
        sys.stdout.flush()


def read_log(path: str) -> str:
    """按时间顺序读出环形日志（第一行文件头会被跳过）。"""
    with open(path, "rb") as fp:
        raw = fp.read()
    if not raw.startswith(MAGIC) or len(raw) < HDR_LEN:
        return raw.decode("utf-8", "replace")
    try:
        sz = int.from_bytes(raw[8:12], "big")
        p = int.from_bytes(raw[12:16], "big")
        body = raw[HDR_LEN:][:sz]
        p %= max(1, len(body))
    except Exception:
        return raw[HDR_LEN:].decode("utf-8", "replace")
    seq = body[p:] + body[:p]
    if p and body[p - 1:p] != b"\n":       # 指针落在行中间才丢那半截行
        cut = seq.find(b"\n")
        if cut >= 0:
            seq = seq[cut + 1:]
    return seq.decode("utf-8", "replace").lstrip("\0").rstrip("\0")


if __name__ == "__main__":
    src = sys.argv[1] if len(sys.argv) > 1 else _LOG
    if not src:
        sys.exit("用法: python logger.py <日志文件>  (或设 TGND_LOG)")
    sys.stdout.write(read_log(src))
