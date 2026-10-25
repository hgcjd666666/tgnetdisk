"""统一输出层 + 定长环形日志文件。

    TGND_LOG=路径          环形日志（定长，写满覆盖旧的）。什么都往里写，
                          包括高频进度——反正有上限，不会把磁盘吃光。
    TGND_LOG_MAX=字节数     环形正文区大小，默认 1 MiB
    TGND_LOG_APPEND=路径    普通追加日志，只收 log() 的低频事件（不含进度）。

日志文件格式（第一行是 64 字节信息区，过滤掉第一行剩下的就是日志）：
    [0:7]    b"TGNDLOG"
    [7:11]   正文大小 (BE uint32)
    [11:15]  写入指针 (BE uint32)
    [15]     b"\n"
    [16:]    正文区，固定长度，指针处是下一次写入位置

写满后从正文区头部继续覆盖，所以可能有一处断行（接受的代价）。
读的时候从指针位置绕一圈就是时间顺序。
"""
import os
import sys
import time

MAGIC = b"TGNDLOG"
INFO_LEN = 15                      # 7 magic + 4 size + 4 pos
HDR_LEN = INFO_LEN + 1             # 信息区 + 一个 \n 行分隔

_LOG = os.environ.get("TGND_LOG", "")
_APPEND = os.environ.get("TGND_LOG_APPEND", "")
_tty = sys.stdout.isatty()


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
                self.body = int.from_bytes(raw[7:11], "big") or self.body
                self.pos = int.from_bytes(raw[11:15], "big") % self.body
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

    def reset(self, keep_pos: bool = True):
        """清空环形缓冲：正文全填 0，指针默认保持原位（别的进程还在写，挪了会错位）。"""
        pos = self.pos if keep_pos else 0
        self.fp.seek(0)
        self.fp.write(self._hdr(pos))
        self.fp.write(b"\0" * self.body)
        self.fp.flush()
        if not keep_pos:
            self.pos = 0

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


def configure(log_file: str = "", log_max: int = 0, append_file: str = ""):
    """进程入口统一调一次：环形日志落哪、多大；普通日志落哪。"""
    global _LOG, _APPEND, _MAX, _ring, _ring_tried
    _LOG = log_file or _LOG
    _APPEND = append_file or _APPEND
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


def _append(msg: str):
    if not _APPEND:
        return
    try:
        with open(_APPEND, "a", encoding="utf-8") as fp:
            fp.write(str(msg) + "\n")
    except OSError:
        pass


def log(msg="", flush=True):
    """普通事件：终端 + 环形日志 + 可选追加日志。"""
    print(msg, flush=flush)
    ring = _get_ring()
    if ring:
        try:
            ring.write(str(msg))
        except Exception:
            pass
    _append(msg)


def progress(msg):
    """进度：tty 原地覆盖 + 写进环形日志（详细），不进普通追加日志。"""
    if _tty:
        sys.stdout.write("\r\033[K" + msg)
        sys.stdout.flush()
    ring = _get_ring()
    if ring:
        try:
            ring.write(str(msg))
        except Exception:
            pass


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
        sz = int.from_bytes(raw[7:11], "big")
        p = int.from_bytes(raw[11:15], "big")
        body = raw[HDR_LEN:][:sz]
        p %= max(1, len(body))
    except Exception:
        return raw[HDR_LEN:].decode("utf-8", "replace")
    seq = body[p:] + body[:p]
    cut = seq.find(b"\n")                 # 绕回去时开头几乎是半截行，按行边界对齐（多丢一行旧的，无所谓）
    if cut >= 0:
        seq = seq[cut + 1:]
    return seq.decode("utf-8", "replace").lstrip("\0").rstrip("\0")


if __name__ == "__main__":
    src = sys.argv[1] if len(sys.argv) > 1 else _LOG
    if not src:
        sys.exit("用法: python logger.py <日志文件>  (或设 TGND_LOG)")
    sys.stdout.write(read_log(src))
