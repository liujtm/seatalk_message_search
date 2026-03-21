"""
web.py — FastAPI Web 服务
提供搜索界面和 API，支持分页，后台异步同步。
"""

import math
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from logger import get_logger

log = get_logger()

app = FastAPI(title="SeaTalk 语义搜索")
templates = Jinja2Templates(directory="templates")

# 自定义 Jinja2 过滤器
def _ts_to_str(ts) -> str:
    try:
        return datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""

templates.env.filters["timestamp_to_str"] = _ts_to_str

# 全局组件引用（由 main.py 注入）
_indexer = None
_storage = None
_config: dict = {}

# 同步任务状态
_sync_status: Dict[str, Any] = {"running": False, "last_result": None}

# 清理任务状态
_purge_status: Dict[str, Any] = {"running": False, "last_result": None}


def init_web(indexer, storage, config: dict) -> None:
    global _indexer, _storage, _config
    _indexer = indexer
    _storage = storage
    _config = config


# ---------------------------------------------------------------------------
# 页面路由
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    last_sync = _storage.get_last_sync() if _storage else None
    total = _storage.total_message_count() if _storage else 0
    indexed = _indexer.collection_count() if _indexer else 0
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "total_messages": total,
            "indexed_messages": indexed,
            "last_sync": last_sync,
        },
    )


@app.get("/stats", response_class=HTMLResponse)
async def stats_page(request: Request):
    return templates.TemplateResponse("stats.html", {"request": request})


# ---------------------------------------------------------------------------
# 搜索 API
# ---------------------------------------------------------------------------

@app.get("/api/search")
async def search(
    q: str = "",
    page: int = 1,
    page_size: int = 0,
    time_from: int = 0,
    time_to: int = 0,
    session_ids: str = "",       # 逗号分隔的 session_id 列表
    session_type: str = "all",   # "all" | "group" | "contact"
    sender_ids: str = "",        # 逗号分隔的 sender_id 列表
):
    effective_page_size = page_size or _config.get("web", {}).get("page_size", 20)
    top_k = _config.get("web", {}).get("search_top_k", 500)

    sid_list: List[str] = [s.strip() for s in session_ids.split(",") if s.strip()] if session_ids else []
    sndr_list: List[str] = [s.strip() for s in sender_ids.split(",") if s.strip()] if sender_ids else []

    # 未指定具体会话但指定了类型时，自动展开该类型的所有 session_id
    if not sid_list and session_type in ("group", "contact"):
        sid_list = _storage.get_session_ids_by_type(session_type)

    # ------------------------------------------------------------------
    # 空查询：按时间倒序浏览，SQL 分页
    # ------------------------------------------------------------------
    if not q.strip():
        total, page_results = _storage.get_messages_by_filter(
            time_from=time_from, time_to=time_to,
            session_ids=sid_list or None,
            sender_ids=sndr_list or None,
            page=page, page_size=effective_page_size,
        )
        total_pages = math.ceil(total / effective_page_size) if total else 1
        page = max(1, min(page, total_pages))
        for r in page_results:
            ts = r.get("timestamp", 0)
            r["datetime"] = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts else ""
            r["score"] = None
        return {
            "query": "",
            "total": total,
            "page": page,
            "page_size": effective_page_size,
            "total_pages": total_pages,
            "results": page_results,
        }

    # ------------------------------------------------------------------
    # 有查询词：向量 + 关键词混合搜索
    # ------------------------------------------------------------------
    if not _indexer or _indexer.collection_count() == 0:
        return JSONResponse({"error": "向量索引为空，请先同步聊天记录"}, status_code=503)

    # --- 向量语义搜索 ---
    vector_results = _indexer.search(
        q, top_k=top_k,
        time_from=time_from, time_to=time_to,
        session_ids=sid_list or None,
        sender_ids=sndr_list or None,
    )
    merged: dict = {}
    for rank, r in enumerate(vector_results):
        r["score"] = round(r["score"], 4)
        r["_vector_rank"] = rank
        merged[r["id"]] = r

    # --- SQLite 关键词搜索（按 token 分别匹配） ---
    keyword_limit = top_k
    total_tokens = max(1, len(q.split()))
    keyword_results = _storage.keyword_search(
        q, limit=keyword_limit,
        time_from=time_from, time_to=time_to,
        session_ids=sid_list or None,
        sender_ids=sndr_list or None,
    )
    for r in keyword_results:
        match_ratio = r.get("matched_tokens", 1) / total_tokens
        is_phrase = r.get("phrase_match", False)
        # 精确短语命中额外加分：确保排在仅语义相近的向量结果前面
        phrase_bonus = 0.35 if is_phrase else 0.0
        bonus = round(0.4 * match_ratio + phrase_bonus, 4)
        if r["id"] in merged:
            merged[r["id"]]["score"] = min(1.0, merged[r["id"]]["score"] + bonus)
            merged[r["id"]]["_keyword_hit"] = True
        else:
            # 纯关键词命中：精确短语 0.95，普通 0.6
            r["score"] = round((0.6 + phrase_bonus) * match_ratio + phrase_bonus * 0.05, 4) if is_phrase \
                else round(0.5 + 0.1 * match_ratio, 4)
            r["_keyword_hit"] = True
            r["_vector_rank"] = len(merged) + 9999
            merged[r["id"]] = r

    results = list(merged.values())
    results.sort(key=lambda x: (x["score"], x["timestamp"]), reverse=True)

    total = len(results)
    total_pages = math.ceil(total / effective_page_size) if total else 1
    page = max(1, min(page, total_pages))
    start = (page - 1) * effective_page_size
    page_results = results[start: start + effective_page_size]

    for r in page_results:
        ts = r.get("timestamp", 0)
        r["datetime"] = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts else ""
        r.pop("_vector_rank", None)

    return {
        "query": q,
        "total": total,
        "page": page,
        "page_size": effective_page_size,
        "total_pages": total_pages,
        "results": page_results,
    }


# ---------------------------------------------------------------------------
# 会话列表 API
# ---------------------------------------------------------------------------

@app.get("/api/sessions")
async def get_sessions():
    if not _storage:
        return {"groups": [], "contacts": []}
    return _storage.get_sessions()


# ---------------------------------------------------------------------------
# 发送者列表 API
# ---------------------------------------------------------------------------

@app.get("/api/senders")
async def get_senders(session_ids: str = ""):
    if not _storage:
        return {"senders": []}
    sid_list = [s.strip() for s in session_ids.split(",") if s.strip()] if session_ids else []
    return {"senders": _storage.get_senders(sid_list or None)}


# ---------------------------------------------------------------------------
# 统计 API
# ---------------------------------------------------------------------------

@app.get("/api/stats")
async def get_stats():
    if not _storage:
        return JSONResponse({"error": "存储未初始化"}, status_code=503)
    stats = _storage.get_stats()
    # 格式化时间戳
    for key in ("earliest", "latest"):
        ts = stats.get(key)
        stats[f"{key}_str"] = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts else "-"
    return stats


# ---------------------------------------------------------------------------
# 同步 API
# ---------------------------------------------------------------------------

@app.post("/api/sync")
async def trigger_sync(days: int = 0):
    if _sync_status["running"]:
        return JSONResponse({"message": "同步正在进行中，请稍候..."}, status_code=409)

    thread = threading.Thread(target=_run_sync, args=(days,), daemon=True)
    thread.start()
    return {"message": "同步已启动，请稍后刷新页面查看结果"}


def _run_sync(days: int = 0) -> None:
    _sync_status["running"] = True
    try:
        from collector import SeaTalkCollector
        sync_config = _config
        if days > 0:
            import copy
            sync_config = copy.deepcopy(_config)
            sync_config["seatalk"]["time_range_days"] = days
        collector = SeaTalkCollector(sync_config)
        messages = collector.collect()

        new_count = _storage.upsert_messages(messages)
        log.info(f"同步完成，共 {len(messages)} 条，新增 {new_count} 条")

        placeholder_ids = _storage.get_placeholder_ids()
        if placeholder_ids:
            _indexer.delete_by_ids(placeholder_ids)
            _storage.mark_unindexed(placeholder_ids)

        chunk_size = _config.get("embedding", {}).get("index_chunk_size", 2000)
        total_indexed = 0
        while True:
            unindexed = _storage.get_unindexed_messages(limit=chunk_size)
            if not unindexed:
                break
            _indexer.build_index(unindexed)
            _storage.mark_indexed([m["id"] for m in unindexed])
            total_indexed += len(unindexed)

        _storage.write_sync_log(len(messages), new_count, "success")
        _sync_status["last_result"] = {
            "status": "success",
            "msg_count": len(messages),
            "new_count": new_count,
        }
    except Exception as e:
        log.error(f"同步失败: {e}", exc_info=True)
        _storage.write_sync_log(0, 0, f"error: {e}")
        _sync_status["last_result"] = {"status": "error", "error": str(e)}
    finally:
        _sync_status["running"] = False


@app.get("/api/sync/status")
async def sync_status():
    return {
        "running": _sync_status["running"],
        "last_result": _sync_status["last_result"],
        "total_messages": _storage.total_message_count() if _storage else 0,
        "indexed_messages": _indexer.collection_count() if _indexer else 0,
    }


# ---------------------------------------------------------------------------
# 清理旧数据 API
# ---------------------------------------------------------------------------

@app.post("/api/purge")
async def trigger_purge(days: int = 60):
    if _purge_status["running"]:
        return JSONResponse({"message": "清理正在进行中，请稍候..."}, status_code=409)
    if days < 1:
        return JSONResponse({"message": "天数必须大于 0"}, status_code=400)

    thread = threading.Thread(target=_run_purge, args=(days,), daemon=True)
    thread.start()
    return {"message": f"正在清理 {days} 天前的数据..."}


def _run_purge(days: int) -> None:
    _purge_status["running"] = True
    try:
        deleted_ids, deleted_count = _storage.purge_old_messages(days)

        # 分批从 ChromaDB 删除向量
        batch_size = 5000
        for i in range(0, len(deleted_ids), batch_size):
            batch = deleted_ids[i:i + batch_size]
            _indexer.delete_by_ids(batch)

        _purge_status["last_result"] = {
            "status": "success",
            "deleted_count": deleted_count,
            "days": days,
        }
        log.info(f"清理完成：删除 {deleted_count} 条 {days} 天前的数据")
    except Exception as e:
        log.error(f"清理失败: {e}", exc_info=True)
        _purge_status["last_result"] = {"status": "error", "error": str(e)}
    finally:
        _purge_status["running"] = False


@app.get("/api/purge/status")
async def purge_status():
    return {
        "running": _purge_status["running"],
        "last_result": _purge_status["last_result"],
        "total_messages": _storage.total_message_count() if _storage else 0,
        "indexed_messages": _indexer.collection_count() if _indexer else 0,
    }


# ---------------------------------------------------------------------------
# 启动入口
# ---------------------------------------------------------------------------

def run(host: str, port: int) -> None:
    log.info(f"Web 服务已启动: http://{host}:{port}")
    uvicorn.run(app, host=host, port=port, log_level="warning")
