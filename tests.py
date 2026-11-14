#!/usr/bin/env python3
"""tgnd 自测入口：按组跑用例，默认用内存版 Telegram。

用法:
    python tests.py            # 全量用例（零网络）
    python tests.py -k put     # 只跑名字含 put 的
    python tests.py --real     # 连真 Telegram（会产生真实消息）
    python tests.py --full     # 连锁与快照一起测

用例分在 tests_store / tests_race / tests_dav / tests_upload / tests_cache /
tests_edge，公共设施在 tests_support。
"""
import asyncio
import os
import sys
import tempfile
import traceback

import tests_support
from tests_support import FakeClient, RESULTS, rec
from store import Store, SqliteBackend, make_session


CASES = [
    ("空文件", "t_empty"),
    ("1字节", "t_one_byte"),
    ("中文+空格", "t_unicode"),
    ("特殊字符", "t_special"),
    ("深目录", "t_deep"),
    ("多分片7片", "t_multislice"),
    ("秒传共享分片", "t_dedup"),
    ("同名覆盖", "t_overwrite"),
    ("cp+mv", "t_copy_move"),
    ("删除文件", "t_delete"),
    ("递归删目录", "t_rmdir"),
    ("mkdir+空目录", "t_mkdir"),
    ("reindex 全量", "t_reindex"),
    ("emoji 路径", "t_emoji_path"),
    ("move 自覆盖保护", "t_move_guard"),
    ("手动上传归入 lost+found", "t_manual_to_lostfound"),
    ("历史分片格式", "t_part_list_format"),
    ("缺分片必须报错", "t_missing_slice"),
    ("WebDAV 语义", "t_dav_basic"),
    (".. 路径规范化", "t_norm_dotdot"),
    ("覆盖失败保留旧文件", "t_overwrite_fail_keeps_old"),
    ("父路径是文件", "t_parent_is_file"),
    ("append 记录总大小", "t_append_total_size"),
    ("13 片顺序", "t_many_slices"),
    ("lost+found 不参与秒传", "t_lostfound_not_dedup_source"),
    ("引用计数按消息 id", "t_refcount_dedup"),
    ("move 到祖先被拒", "t_move_ancestor"),
    ("同名 tgnd.db.xz 不丢", "t_manual_named_snapshot"),
    ("同名 hash.0 不覆盖", "t_foreign_part_msg"),
    ("环形超长行", "t_ring_long_line"),
    ("锁-空频道自建快照", "t_lock_bootstrap"),
    ("锁-重复acquire幂等", "t_lock_reacquire"),
    ("锁-双节点互斥", "t_lock_busy"),
    ("快照里的 dirty 恒为 0", "t_snapshot_dirty_clean"),
    ("竞态-多线程写不同路径", "t_race_backend_writers"),
    ("竞态-多线程写同一路径", "t_race_backend_same_path"),
    ("竞态-restore 风暴", "t_race_restore_storm"),
    ("竞态-dump 与写并发", "t_race_dump_while_write"),
    ("竞态-三节点抢锁", "t_race_lock_contend"),
    ("竞态-并发秒传", "t_race_put_dedup"),
    ("竞态-边删边读", "t_race_delete_read"),
    ("竞态-双进程共存", "t_race_process_coexist"),
    ("竞态-并发传不同文件", "t_race_put_many_different"),
    ("竞态-并发覆盖同路径", "t_race_put_same_path"),
    ("竞态-边写边读内容", "t_race_write_read_content"),
    ("竞态-推送中写入", "t_race_snapshot_while_write"),
    ("竞态-mv 与 cp 并发", "t_race_mv_cp"),
    ("竞态-kill 后恢复", "t_race_kill_recovery"),
    ("缓存-默认关闭", "t_cache_default_off"),
    ("缓存-命中不碰TG", "t_cache_hit"),
    ("缓存-超上限不落盘", "t_cache_oversize"),
    ("缓存-LRU淘汰最旧", "t_cache_lru"),
    ("缓存-覆盖重传失效", "t_cache_invalidate_overwrite"),
    ("缓存-秒传共享同一份", "t_cache_dedup_share"),
    ("缓存-move后仍命中", "t_cache_move_hit"),
    ("缓存-残片丢弃重下", "t_cache_corrupt"),
    ("缓存-断连不留残迹", "t_cache_abort_no_leftover"),
    ("缓存-并发读只落一份", "t_cache_concurrent"),
    ("缓存-传完即本地命中", "t_cache_adopt_after_upload"),
    ("缓存-删除回收", "t_cache_drop_on_delete"),
    ("缓存-共享引用不误删", "t_cache_shared_not_dropped"),
    ("缓存-不同内容互不串", "t_cache_path_independent"),
    ("上传-建消息失败不重传", "t_sendmedia_retry_reuses_upload"),
    ("上传-复用已有分片", "t_slice_reuse_from_lostfound"),
    ("上传-切片变了不复用", "t_slice_reuse_rejects_mismatch"),
    ("上传-PUT立刻返回", "t_up_accept"),
    ("上传-后台传完清staging", "t_up_finish"),
    ("上传-删掉未传完的", "t_up_delete_pending"),
    ("上传-移动后仍能传完", "t_up_move_pending"),
    ("上传-覆盖未传完的", "t_up_overwrite_pending"),
    ("上传-崩溃后重启续传", "t_up_crash_recover"),
    ("上传-清无主staging", "t_up_gc_orphan_staging"),
    ("范围-Store分段读", "t_range_store"),
    ("范围-HTTP 206与416", "t_range_http"),
    ("范围-传完后从TG读", "t_range_after_upload"),
    ("范围-命中缓存seek", "t_range_cache_hit"),
    ("补洞-Range 顺带落盘", "t_fill_from_range"),
    ("补洞-后台补齐并转正", "t_fill_complete"),
    ("补洞-全0块打成稀疏", "t_fill_punch_zeros"),
    ("补洞-已有缓存不重建", "t_fill_no_partial_after_full_get"),
    ("补洞-缓存关时不建", "t_fill_off_when_cache_off"),
    ("边界-小于一块的文件", "t_edge_tiny_file"),
    ("边界-正好一块", "t_edge_one_block"),
    ("边界-尾块不足一块", "t_edge_tail_block"),
    ("边界-全份与后缀Range", "t_edge_whole_and_suffix"),
    ("边界-空文件Range", "t_edge_empty_file"),
    ("边界-并发Range不同块", "t_edge_concurrent_ranges"),
    ("边界-部分缓存被篡改", "t_edge_tampered_partial"),
    ("边界-部分缓存长度变了", "t_edge_partial_size_changed"),
    ("边界-跨分片Range", "t_edge_multislice_range"),
    ("边界-上传中Range不建缓存", "t_edge_staging_range"),
    ("边界-部分缓存不被淘汰", "t_edge_part_not_evicted"),
    ("边界-部分缓存过期清理", "t_edge_part_ttl"),
    ("边界-补洞失败保留", "t_edge_fill_failure_keeps_partial"),
    ("边界-补洞关闭", "t_edge_fill_rate_zero"),
    ("边界-全量与Range并发", "t_edge_full_get_and_range_race"),
    ("单设备-文件锁跨进程互斥", "t_lock_file_across_processes"),
    ("备份-写完不立即推快照", "t_backup_idle_only"),
    ("备份-只留最近N份", "t_backup_keep_n"),
    ("单设备-写操作零 TG 往返", "t_write_no_tg_rpc"),
]


async def main():
    only = None
    if "-k" in sys.argv:
        only = sys.argv[sys.argv.index("-k") + 1]
    group = None
    if "--group" in sys.argv:
        group = sys.argv[sys.argv.index("--group") + 1]

    if "--full" in sys.argv:
        # 默认走 file 模式（单设备）。--full 才把分布式锁接回来，
        # 那几个锁用例断言的正是 TG 锁协议。
        os.environ["TGND_LOCK"] = "tg"

    if "--real" in sys.argv:                    # 连真实 Telegram
        from telethon import TelegramClient
        api_id = int(os.environ["TG_API_ID"])
        api_hash = os.environ["TG_API_HASH"]
        chat = os.environ["TG_CHAT"]
        chat = int(chat) if str(chat).lstrip("-").isdigit() else chat
        client = TelegramClient(make_session(os.environ["TG_SESSION"]), api_id, api_hash,
                                device_model=os.environ.get("TG_DEVICE", "tgnd-test"),
                                system_version="Linux", app_version="tgnd")
        await client.connect()
        st = Store(client, chat)
        st.fast = "--full" not in sys.argv
        await st.load()
    else:                                       # 默认：内存版 TG
        client = FakeClient()
        st = Store(client, 1, backend=SqliteBackend(tempfile.mktemp(suffix=".db")))
        st.fast = "--full" not in sys.argv

    ctx = type("Ctx", (), {"st": st, "client": client})()

    # 各组把函数注册成 {名字: 可调用}，入口按 CASES 的原顺序跑，保证输出稳定
    import tests_store
    import tests_race
    import tests_dav
    import tests_upload
    import tests_cache
    import tests_edge
    fns = {}
    for mod in (tests_store, tests_race, tests_dav, tests_upload,
                tests_cache, tests_edge):
        if group and mod.__name__ != group:
            continue
        for name, fn in await mod.main(ctx):
            assert name not in fns, "用例重名: %s" % name
            fns[name] = fn

    for label, fname in CASES:
        if fname not in fns:
            continue
        fn = fns[fname]
        if only and only not in label:
            continue
        try:
            await fn(ctx)
            rec(label, True)
        except AssertionError as e:
            rec(label, False, str(e) or "断言失败")
        except Exception as e:
            rec(label, False, "%s: %s" % (type(e).__name__, e))
            traceback.print_exc()

    # 收尾：删测试目录并提交索引
    try:
        if st.get("/_test"):
            await st.delete("/_test")
    except Exception as e:
        print("清理失败:", e)

    if not st.fast:
        try:
            await st.wait_released(10)
        except Exception:
            pass

    await client.disconnect()
    ok = sum(1 for _, o, _ in RESULTS if o)
    print("\n===== PASS=%d FAIL=%d =====" % (ok, len(RESULTS) - ok))
    return 0 if ok == len(RESULTS) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
