"""
main.py — 入口
流程：启动 SeaTalk → 采集消息 → SQLite 存储 → ChromaDB 索引 → 启动 Web

用法：
  python main.py                  # 完整流程（重启 SeaTalk → 采集 → 索引 → Web）
  python main.py --web-only       # 仅启动 Web，跳过采集
  python main.py --skip-collect   # 跳过采集，只做索引 + Web（SeaTalk 无需运行）
  python main.py --days 7         # 覆盖配置中的 time_range_days
"""

import warnings
warnings.filterwarnings("ignore", message=".*logfire.*", category=UserWarning)

import argparse
import os
import shutil
import sys

import yaml

import web
from indexer import VectorIndexer
from logger import setup_logger, get_logger
from storage import SQLiteStorage


def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def parse_args():
    parser = argparse.ArgumentParser(
        description="SeaTalk 语义搜索",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  python main.py                  完整流程（重启 SeaTalk → 采集 → 索引 → Web）
  python main.py --web-only       仅启动 Web 服务，跳过一切采集步骤
  python main.py --skip-collect   跳过采集，对已有数据补充索引后启动 Web
  python main.py --days 7         采集最近 7 天（覆盖 config.yaml 中的设置）
        """,
    )
    parser.add_argument(
        "-w", "--web-only",
        action="store_true",
        help="仅启动 Web 服务，不重启 SeaTalk，不采集，不索引",
    )
    parser.add_argument(
        "-s", "--skip-collect",
        action="store_true",
        help="跳过 SeaTalk 重启和消息采集，对库中未索引消息补充向量化后启动 Web",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        metavar="N",
        help="采集最近 N 天的消息，覆盖 config.yaml 中的 time_range_days",
    )
    parser.add_argument(
        "-r", "--reset-index",
        action="store_true",
        help="清空向量索引（data/chroma），用于修复索引损坏问题，配合 -s 使用",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    config = load_config()

    # --days 参数覆盖配置
    if args.days is not None:
        config["seatalk"]["time_range_days"] = args.days

    log_path = config.get("storage", {}).get("log_path", "logs/app.log")
    max_days = config.get("storage", {}).get("log_max_days", 7)
    setup_logger(log_path, max_days=max_days)
    log = get_logger()

    log.info("=" * 50)
    log.info("SeaTalk 语义搜索系统 启动")
    log.info("=" * 50)

    sqlite_path = config["storage"]["sqlite_path"]
    chroma_path = config["storage"]["chroma_path"]
    storage = SQLiteStorage(sqlite_path)

    # --reset-index：清空向量索引目录，并将所有消息重置为未索引状态
    if args.reset_index:
        if os.path.exists(chroma_path):
            shutil.rmtree(chroma_path)
            log.info(f"已清空向量索引目录: {chroma_path}")
        storage.reset_all_indexed()
        log.info("已将所有消息重置为未索引状态，将重新向量化")

    if args.web_only:
        # ------------------------------------------------------------------
        # 仅 Web 模式：直接跳到索引加载 + 启动服务
        # ------------------------------------------------------------------
        log.info("模式: 仅启动 Web（--web-only）")

    else:
        if not args.skip_collect:
            # ----------------------------------------------------------------
            # 完整流程：重启 SeaTalk → 采集
            # ----------------------------------------------------------------
            import launcher
            from collector import SeaTalkCollector

            cdp_port = config["seatalk"]["cdp_port"]
            launcher.restart_seatalk(cdp_port)

            log.info(f"开始采集最近 {config['seatalk']['time_range_days']} 天的聊天记录...")
            collector = SeaTalkCollector(config)
            try:
                messages = collector.collect()
            except Exception as e:
                log.error(f"消息采集失败: {e}", exc_info=True)
                sys.exit(1)

            new_count = storage.upsert_messages(messages)
            log.info(f"SQLite 写入完成，共 {len(messages)} 条，新增 {new_count} 条")
            storage.write_sync_log(len(messages), new_count, "success")
        else:
            log.info("模式: 跳过采集（--skip-collect），使用已有数据")

        # --------------------------------------------------------------------
        # 向量索引（增量）：完整流程和 --skip-collect 都执行
        # --------------------------------------------------------------------
        indexer = VectorIndexer(config)

        # 清除已入库但属于纯占位内容的旧向量记录
        placeholder_ids = storage.get_placeholder_ids()
        if placeholder_ids:
            log.info(f"清除 {len(placeholder_ids)} 条占位消息的向量记录（[图片]/[视频]/[贴图]）...")
            indexer.delete_by_ids(placeholder_ids)
            storage.mark_unindexed(placeholder_ids)

        chunk_size = config.get("embedding", {}).get("index_chunk_size", 2000)
        total_indexed = 0
        while True:
            unindexed = storage.get_unindexed_messages(limit=chunk_size)
            if not unindexed:
                break
            log.info(f"向量化进度：本批 {len(unindexed)} 条（已完成 {total_indexed} 条）...")
            indexer.build_index(unindexed)
            storage.mark_indexed([m["id"] for m in unindexed])
            total_indexed += len(unindexed)

        if total_indexed:
            log.info(f"向量化完成，共新增 {total_indexed} 条")
        else:
            log.info("所有消息已是最新索引，无需重新向量化")

        log.info(f"ChromaDB 当前共 {indexer.collection_count()} 条向量记录")

    # ------------------------------------------------------------------
    # 启动 Web 服务（所有模式都执行）
    # ------------------------------------------------------------------
    if args.web_only:
        # web-only 模式下延迟加载 indexer（避免 --web-only 时不必要的模型加载警告）
        indexer = VectorIndexer(config)

    web_cfg = config.get("web", {})
    host = web_cfg.get("host", "127.0.0.1")
    port = web_cfg.get("port", 12345)

    web.init_web(indexer, storage, config)
    log.info(f"启动 Web 服务: http://{host}:{port}")
    web.run(host, port)


if __name__ == "__main__":
    main()
