#!/usr/bin/env python3
"""对目录下每个文件算快速哈希，结果写成 JSON，便于两边比对。

用法:
    python hashdir.py <目录> [-o out.json] [-j 8] [-a xxh3_128|xxh3_64|md5] [-q]

输出格式:
    {
      "root": "/data/music",
      "algo": "xxh3_128",
      "count": 42,
      "total": 123456789,
      "files": {
        "一只喀秋鲨/01.flac": {"size": 30219876, "hash": "3f2a...", "mtime": 1759001234},
        ...
      }
    }
"""
import argparse
import hashlib
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

try:
    import xxhash
except ImportError:
    xxhash = None


def make_hasher(algo):
    if algo.startswith("xxh"):
        if xxhash is None:
            sys.exit("需要 xxhash：uv pip install xxhash")
        return getattr(xxhash, algo)()
    return hashlib.new(algo)


def hash_file(path, algo, bufsize=8 << 20):
    h = make_hasher(algo)
    with open(path, "rb") as f:
        while True:
            b = f.read(bufsize)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root")
    ap.add_argument("-o", "--out", default="")
    ap.add_argument("-j", "--jobs", type=int, default=min(8, os.cpu_count() or 4))
    ap.add_argument("-a", "--algo", default="xxh3_128",
                    choices=["xxh3_128", "xxh3_64", "md5", "sha256", "blake2b"])
    ap.add_argument("-q", "--quiet", action="store_true")
    args = ap.parse_args()

    root = os.path.abspath(args.root)
    if not os.path.isdir(root):
        sys.exit(f"不是目录: {root}")

    files = []
    for dirpath, _, names in os.walk(root):
        for n in names:
            p = os.path.join(dirpath, n)
            if os.path.isfile(p):
                files.append(p)
    files.sort()

    out = {}
    total = 0
    t0 = time.time()
    done = 0
    errors = 0
    last_tick = 0.0

    def worker(p):
        st = os.stat(p)
        rel = os.path.relpath(p, root)
        try:
            h = hash_file(p, args.algo)
        except Exception as e:
            return rel, {"size": st.st_size, "error": f"{type(e).__name__}: {e}",
                         "mtime": int(st.st_mtime)}
        return rel, {"size": st.st_size, "hash": h, "mtime": int(st.st_mtime)}

    with ThreadPoolExecutor(max_workers=args.jobs) as ex:
        for rel, info in ex.map(worker, files):
            out[rel] = info
            total += info.get("size", 0)
            done += 1
            if "error" in info:
                errors += 1
            now = time.time()
            if not args.quiet and (now - last_tick >= 0.3 or done == len(files)):
                last_tick = now
                rate = total / max(1e-6, now - t0) / 1048576
                pct = done * 100 // len(files)
                print(f"\r{done}/{len(files)} {pct:3d}%  {total/1073741824:.2f} GB  "
                      f"{rate:.0f} MB/s", end="", file=sys.stderr, flush=True)

    if not args.quiet:
        print(file=sys.stderr)

    doc = {"root": root, "algo": args.algo, "count": len(out), "total": total,
           "elapsed": round(time.time() - t0, 2), "files": out}
    text = json.dumps(doc, ensure_ascii=False, indent=1, sort_keys=True)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(text)
        print(f"写入 {args.out}：{len(out)} 个文件，{total/1073741824:.2f} GB，"
              f"{doc['elapsed']}s，错误 {errors} 个", file=sys.stderr)
    else:
        print(text)


if __name__ == "__main__":
    main()
