"""虚拟路径的规范化。

WebDAV 的路径不是本机文件系统路径：`os.path` 会按平台处理反斜杠和盘符，
`posixpath` 又保留开头的 `//`、不管 Unicode 形态。所以这里包一层，所有
入库、查库、比较路径的地方都必须先过 `norm()`，否则同一个目录会变成两个键。

约定：根是 `/`，没有末尾斜杠；`.a` 和 `.a/` 是同一个东西。
"""
import posixpath
import unicodedata


def norm(p: str) -> str:
    """把任意路径规范成唯一形式：绝对、无 `.`/`..`/空段、NFC、无末尾斜杠。"""
    if not p:
        return "/"
    s = unicodedata.normalize("NFC", str(p))
    # 先把开头的斜杠收成一个，否则 posixpath 会把 "//a" 原样留着
    out = posixpath.normpath("/" + s.lstrip("/"))
    return out or "/"


def bad(p: str) -> bool:
    """控制字符（含 NUL）不能进索引：SQLite 收得下，但查询、日志、HTTP 头全会被它搞坏。"""
    return any(ord(c) < 0x20 or ord(c) == 0x7f for c in (p or ""))


def base(p: str) -> str:
    """最后一段。根返回空串。"""
    p = norm(p)
    return "" if p == "/" else p.rsplit("/", 1)[-1]


def parent(p: str) -> str:
    """父目录。根的父目录还是根。"""
    p = norm(p)
    if p == "/":
        return "/"
    return p.rsplit("/", 1)[0] or "/"


def under(p: str, prefix: str) -> bool:
    """p 是否在 prefix 子树内（不含 prefix 自身）。`/ab` 不在 `/a` 下面。"""
    p, prefix = norm(p), norm(prefix)
    if prefix == "/":
        return p != "/"
    return p.startswith(prefix + "/")


