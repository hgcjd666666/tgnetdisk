"""索引：内容 / 路径 / 目录 / 元信息，四张表。

    content(hash PK, size, parts)      一份内容一行，秒传靠它一对多
    files  (path PK, hash, mtime, status)   status=1 表示字节还在本地上传暂存里
    dirs   (path PK, mtime)
    meta   (k PK, v)

为什么内容和路径分表：同一份内容会被多个路径引用（秒传），hash 作主键就
只能存一个路径。分开以后「哪些路径引用这份内容」是一次 GROUP BY 的事。

所有路径都必须已经过 paths.norm()，Index 不再做规范化。
"""
import contextlib
import json
import os
import sqlite3
from urllib.parse import quote

from paths import norm

SCHEMA = """
CREATE TABLE IF NOT EXISTS content(
    hash TEXT PRIMARY KEY,
    size INTEGER DEFAULT 0,
    parts TEXT DEFAULT '{}');
CREATE TABLE IF NOT EXISTS files(
    path TEXT PRIMARY KEY,
    hash TEXT NOT NULL,
    mtime INTEGER DEFAULT 0,
    status INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS dirs(path TEXT PRIMARY KEY, mtime INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
CREATE INDEX IF NOT EXISTS idx_files_hash ON files(hash);
"""

# 上传中（字节在本地暂存，还没上 TG）与已完成
STATUS_PENDING = 1
STATUS_DONE = 0


def load_parts(raw) -> dict:
    """parts 在库里是 JSON 字符串：{"0": 123, "1": 124}。key 必须是 int。"""
    try:
        d = json.loads(raw or "{}")
        return {int(k): int(v) for k, v in d.items() if v}
    except (ValueError, TypeError, AttributeError):
        return {}


def dump_parts(parts: dict) -> str:
    return json.dumps({str(k): int(v) for k, v in (parts or {}).items() if v},
                      separators=(",", ":"))


class Entry:
    """一条路径。dir 的 hash 是空串，size 是 0。"""

    __slots__ = ("path", "is_dir", "hash", "size", "parts", "mtime", "status")

    def __init__(self, path, is_dir, hash="", size=0, parts=None, mtime=0, status=0):
        self.path = path
        self.is_dir = is_dir
        self.hash = hash or ""
        self.size = size or 0
        self.parts = parts or {}
        self.mtime = mtime or 0
        self.status = status or 0

    @property
    def pending(self) -> bool:
        return self.status == STATUS_PENDING

    def __repr__(self):
        return f"<Entry {self.path} {'dir' if self.is_dir else self.size}>"


class Index:
    def __init__(self, path: str):
        self.path = path
        d = os.path.dirname(os.path.abspath(path))
        if d:
            os.makedirs(d, exist_ok=True)
        with self._conn() as db:
            db.executescript(SCHEMA)
        self._migrate_legacy()

    # ---------- 连接 ----------
    @contextlib.contextmanager
    def _conn(self, create: bool = True):
        """一次操作一条连接，用完立刻关。

        必须真的 close：连接一直开着的话 WAL 永远不 checkpoint，restore()
        换掉主库文件后旧的 -wal 还会被重放，等于白替换。
        """
        uri = "file:" + quote(self.path) + "?mode=" + ("rwc" if create else "rw")
        db = sqlite3.connect(uri, uri=True, check_same_thread=False, timeout=30)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=NORMAL")
        try:
            yield db
            db.commit()
        finally:
            db.close()

    def close(self):
        pass

    # ---------- 旧库迁移 ----------
    def _cols(self, db, table: str) -> set:
        try:
            return {r[1] for r in db.execute(f"PRAGMA table_info({table})")}
        except sqlite3.Error:
            return set()

    def _migrate_legacy(self):
        """老库是 files(path, hash, size, mtime, parts, status)：把内容拆到 content 表。

        数据本身没变（hash 和分片消息 id 原样保留），只是把「一份内容」从
        每个路径行里提出来。迁移在事务里做，失败就整个回滚。
        """
        with self._conn() as db:
            cols = self._cols(db, "files")
            if not cols or "size" not in cols or "parts" not in cols:
                return
            db.execute("BEGIN")
            try:
                db.execute("""INSERT OR IGNORE INTO content(hash, size, parts)
                              SELECT hash, MAX(size), parts FROM files
                              WHERE hash != '' AND hash IS NOT NULL GROUP BY hash""")
                db.execute("""CREATE TABLE files_new(
                                path TEXT PRIMARY KEY,
                                hash TEXT NOT NULL,
                                mtime INTEGER DEFAULT 0,
                                status INTEGER DEFAULT 0)""")
                db.execute("""INSERT OR REPLACE INTO files_new(path, hash, mtime, status)
                              SELECT path, hash, mtime, COALESCE(status, 0) FROM files""")
                db.execute("DROP TABLE files")
                db.execute("ALTER TABLE files_new RENAME TO files")
                db.execute("CREATE INDEX IF NOT EXISTS idx_files_hash ON files(hash)")
                db.commit()
            except Exception:
                db.rollback()
                raise

    # ---------- 内容 ----------
    def content(self, h: str):
        """返回 (size, parts)；没有返回 None。"""
        if not h:
            return None
        with self._conn() as db:
            r = db.execute("SELECT size, parts FROM content WHERE hash=?", (h,)).fetchone()
        return (r[0], load_parts(r[1])) if r else None

    def content_put(self, h: str, size: int, parts: dict):
        if not h:
            return
        with self._conn() as db:
            db.execute("INSERT INTO content(hash, size, parts) VALUES(?,?,?) "
                       "ON CONFLICT(hash) DO UPDATE SET size=excluded.size, parts=excluded.parts",
                       (h, int(size), dump_parts(parts)))

    def content_drop(self, h: str) -> bool:
        """内容没人引用了才删。返回是否真的删了。"""
        if not h:
            return False
        with self._conn() as db:
            n = db.execute("SELECT COUNT(*) FROM files WHERE hash=?", (h,)).fetchone()[0]
            if n:
                return False
            db.execute("DELETE FROM content WHERE hash=?", (h,))
        return True

    def content_exists(self, h: str) -> bool:
        if not h:
            return False
        with self._conn() as db:
            return db.execute("SELECT 1 FROM content WHERE hash=?", (h,)).fetchone() is not None

    # ---------- 路径 ----------
    def get(self, path: str):
        path = norm(path)
        with self._conn() as db:
            if path == "/":
                return Entry("/", True, mtime=self._dir_mtime(db, "/"))
            r = db.execute("SELECT hash, mtime, status FROM files WHERE path=?",
                           (path,)).fetchone()
            if r:
                size, parts = self._content(db, r[0])
                return Entry(path, False, r[0], size, parts, r[1], r[2])
            r = db.execute("SELECT mtime FROM dirs WHERE path=?", (path,)).fetchone()
            if r:
                return Entry(path, True, mtime=r[0])
        return None

    def _dir_mtime(self, db, path: str) -> int:
        r = db.execute("SELECT mtime FROM dirs WHERE path=?", (path,)).fetchone()
        return r[0] if r else 0

    def _content(self, db, h: str):
        r = db.execute("SELECT size, parts FROM content WHERE hash=?", (h,)).fetchone()
        return (r[0], load_parts(r[1])) if r else (0, {})

    def children(self, path: str):
        """直接子项，文件在前、目录在后，按名字排。"""
        path = norm(path)
        prefix = "/" if path == "/" else path + "/"
        start = len(prefix) + 1          # SQLite substr 从 1 开始
        pat = prefix + "%"
        out = []
        with self._conn() as db:
            rows = db.execute(
                "SELECT f.path, f.hash, f.mtime, f.status, c.size "
                "FROM files f LEFT JOIN content c ON c.hash=f.hash "
                "WHERE f.path LIKE ? AND instr(substr(f.path, ?), '/')=0",
                (pat, start)).fetchall()
            for p, h, mt, st, sz in rows:
                out.append(Entry(p, False, h, sz or 0, None, mt, st))
            rows = db.execute(
                "SELECT path, mtime FROM dirs WHERE path LIKE ? AND instr(substr(path, ?), '/')=0",
                (pat, start)).fetchall()
            for p, mt in rows:
                out.append(Entry(p, True, mtime=mt))
        out.sort(key=lambda e: (e.is_dir, e.path))
        return out

    def put_file(self, path: str, h: str, mtime: int, status: int = STATUS_DONE):
        path = norm(path)
        with self._conn() as db:
            db.execute("INSERT INTO files(path, hash, mtime, status) VALUES(?,?,?,?) "
                       "ON CONFLICT(path) DO UPDATE SET hash=excluded.hash, "
                       "mtime=excluded.mtime, status=excluded.status",
                       (path, h, int(mtime), int(status)))
            db.execute("DELETE FROM dirs WHERE path=?", (path,))

    def put_dir(self, path: str, mtime: int):
        path = norm(path)
        if path == "/":
            with self._conn() as db:
                db.execute("INSERT OR IGNORE INTO dirs(path, mtime) VALUES('/', ?)", (int(mtime),))
            return
        with self._conn() as db:
            db.execute("INSERT INTO dirs(path, mtime) VALUES(?,?) "
                       "ON CONFLICT(path) DO UPDATE SET mtime=excluded.mtime",
                       (path, int(mtime)))
            db.execute("DELETE FROM files WHERE path=?", (path,))

    def set_status(self, path: str, status: int):
        with self._conn() as db:
            db.execute("UPDATE files SET status=? WHERE path=?", (int(status), norm(path)))

    def drop(self, path: str) -> int:
        """删一条（不递归）。返回删掉的行数。"""
        path = norm(path)
        if path == "/":
            return 0
        with self._conn() as db:
            n = db.execute("DELETE FROM files WHERE path=?", (path,)).rowcount
            n += db.execute("DELETE FROM dirs WHERE path=?", (path,)).rowcount
        return n

    def list_under(self, path: str):
        """[(path, hash, is_dir)]：path 及其整棵子树，不删。"""
        path = norm(path)
        like = "/%" if path == "/" else path.rstrip("/") + "/%"
        with self._conn() as db:
            rows = db.execute("SELECT path, hash, 0 FROM files WHERE path=? OR path LIKE ?",
                              (path, like)).fetchall()
            rows += db.execute("SELECT path, '', 1 FROM dirs WHERE path=? OR path LIKE ?",
                               (path, like)).fetchall()
        return [(r[0], r[1], bool(r[2])) for r in rows]

    def take_under(self, path: str):
        """一次事务摘掉 path 及其整棵子树，返回 [(path, hash, is_dir)]。"""
        path = norm(path)
        like = "/%" if path == "/" else path.rstrip("/") + "/%"
        with self._conn() as db:
            rows = db.execute("SELECT path, hash, 0 FROM files WHERE path=? OR path LIKE ?",
                              (path, like)).fetchall()
            rows += db.execute("SELECT path, '', 1 FROM dirs WHERE path=? OR path LIKE ?",
                               (path, like)).fetchall()
            if path != "/":
                db.execute("DELETE FROM files WHERE path=? OR path LIKE ?", (path, like))
                db.execute("DELETE FROM dirs WHERE path=? OR path LIKE ?", (path, like))
        return [(r[0], r[1], bool(r[2])) for r in rows]

    def hash_of(self, path: str):
        with self._conn() as db:
            r = db.execute("SELECT hash FROM files WHERE path=?", (norm(path),)).fetchone()
        return r[0] if r else None

    def referenced(self, hashes: list) -> dict:
        """{hash: 引用数}。删除/覆盖时用它判断内容还能不能丢。"""
        if not hashes:
            return {}
        out = {}
        with self._conn() as db:
            for i in range(0, len(hashes), 500):
                batch = hashes[i:i + 500]
                q = ",".join("?" * len(batch))
                for h, n in db.execute(
                        f"SELECT hash, COUNT(*) FROM files WHERE hash IN ({q}) GROUP BY hash",
                        batch):
                    out[h] = n
        return out

    def all_files(self):
        with self._conn() as db:
            return db.execute("SELECT path, hash, mtime, status FROM files").fetchall()

    def all_dirs(self):
        with self._conn() as db:
            return db.execute("SELECT path, mtime FROM dirs").fetchall()

    def dump(self, dst: str):
        """把库压成一份可独立打开的 sqlite 文件（快照用）。"""
        with self._conn(create=False) as src:
            out = sqlite3.connect(dst)
            try:
                src.backup(out)
                out.commit()
            finally:
                out.close()

    def restore(self, src: str):
        """用另一份库整体替换自己。失败时本地库保持原样。"""
        if not os.path.exists(src):
            raise FileNotFoundError(src)
        for suf in ("-wal", "-shm"):        # 旧日志文件必须一起清掉
            try:
                os.unlink(self.path + suf)
            except OSError:
                pass
        tmp = self.path + ".restore"
        try:
            os.unlink(tmp)
        except OSError:
            pass
        os.replace(src, tmp)
        try:
            c = sqlite3.connect(tmp)
            try:
                if not c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='files'").fetchone():
                    raise RuntimeError("不是有效的索引库")
                if c.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise RuntimeError("索引库校验失败")
            finally:
                c.close()
            os.replace(tmp, self.path)
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass
        with self._conn() as db:
            db.executescript(SCHEMA)

    # ---------- meta ----------
    def meta(self, k: str, default=None):
        with self._conn() as db:
            r = db.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
        return r[0] if r else default

    def set_meta(self, k: str, v):
        with self._conn() as db:
            db.execute("INSERT INTO meta(k, v) VALUES(?,?) "
                       "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, str(v)))

    def remove_meta(self, k: str):
        with self._conn() as db:
            db.execute("DELETE FROM meta WHERE k=?", (k,))
