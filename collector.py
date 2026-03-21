"""
collector.py — SeaTalk 消息采集
通过 CDP WebSocket 注入 JS，查询 SeaTalk 内部 SQLite + Redux store，
采集结构化消息并过滤告警群。
"""

import json
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
import websocket

from logger import get_logger

log = get_logger()


# ---------------------------------------------------------------------------
# 数据结构
# ---------------------------------------------------------------------------

@dataclass
class Message:
    mid: str
    session_id: str
    session_name: str
    sender_id: str
    sender_name: str
    timestamp: int
    content: str
    content_type: str          # text / image / file / video / link / other


# ---------------------------------------------------------------------------
# CDP Helper
# ---------------------------------------------------------------------------

class CDPHelper:
    """封装 Chrome DevTools Protocol WebSocket 通信"""

    def __init__(self, port: int):
        self.port = port
        self._ws: Optional[websocket.WebSocket] = None
        self._msg_id = 0
        self._pending: Dict[int, dict] = {}
        self._lock = threading.Lock()
        self._recv_thread: Optional[threading.Thread] = None
        self._running = False

    # ------------------------------------------------------------------
    # 连接管理
    # ------------------------------------------------------------------

    def connect(self) -> None:
        ws_url = self._find_seatalk_ws_url()
        log.debug(f"Connecting to CDP WebSocket: {ws_url}")
        self._ws = websocket.create_connection(ws_url, timeout=10)
        self._running = True
        self._recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
        self._recv_thread.start()
        log.debug("CDP WebSocket connected")

    def close(self) -> None:
        self._running = False
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass

    def reconnect(self) -> None:
        """断开并重新建立 CDP WebSocket 连接"""
        self.close()
        time.sleep(2)
        self.connect()

    def _find_seatalk_ws_url(self) -> str:
        resp = requests.get(f"http://localhost:{self.port}/json", timeout=5)
        targets = resp.json()
        for t in targets:
            if t.get("type") == "page" and "SeaTalk" in t.get("title", ""):
                url = t.get("webSocketDebuggerUrl", "")
                if url:
                    return url
        raise RuntimeError("未找到 SeaTalk 渲染页面的 CDP target，请确认 SeaTalk 已登录")

    # ------------------------------------------------------------------
    # 消息收发
    # ------------------------------------------------------------------

    def _next_id(self) -> int:
        with self._lock:
            self._msg_id += 1
            return self._msg_id

    def _recv_loop(self) -> None:
        while self._running:
            try:
                raw = self._ws.recv()
                msg = json.loads(raw)
                msg_id = msg.get("id")
                if msg_id is not None and msg_id in self._pending:
                    self._pending[msg_id]["result"] = msg
                    self._pending[msg_id]["event"].set()
            except websocket.WebSocketConnectionClosedException:
                break
            except Exception:
                pass  # recv timeout, 继续循环等待下一条消息

    def _send(self, method: str, params: dict, timeout: int = 30) -> dict:
        msg_id = self._next_id()
        event = threading.Event()
        self._pending[msg_id] = {"event": event, "result": None}
        payload = json.dumps({"id": msg_id, "method": method, "params": params})
        self._ws.send(payload)
        if not event.wait(timeout=timeout):
            del self._pending[msg_id]
            raise TimeoutError(f"CDP 请求超时: {method}")
        result = self._pending.pop(msg_id)["result"]
        return result

    # ------------------------------------------------------------------
    # JS 执行
    # ------------------------------------------------------------------

    def evaluate(self, expression: str) -> Any:
        """同步执行 JS，返回结果值"""
        result = self._send(
            "Runtime.evaluate",
            {"expression": expression, "returnByValue": True},
        )
        return self._unwrap(result)

    def async_evaluate(self, expression: str, timeout: int = 60) -> Any:
        """异步执行 JS（awaitPromise=true），适用于 sqlite.all()"""
        result = self._send(
            "Runtime.evaluate",
            {
                "expression": expression,
                "returnByValue": True,
                "awaitPromise": True,
                "timeout": timeout * 1000,
            },
            timeout=timeout + 5,
        )
        return self._unwrap(result)

    @staticmethod
    def _unwrap(cdp_result: dict) -> Any:
        if "error" in cdp_result:
            raise RuntimeError(f"CDP error: {cdp_result['error']}")
        result = cdp_result.get("result", {}).get("result", {})
        if result.get("type") == "string":
            return result["value"]
        if result.get("type") in ("number", "boolean"):
            return result["value"]
        if result.get("subtype") == "null" or result.get("type") == "undefined":
            return None
        raise RuntimeError(f"Unexpected CDP result type: {result}")


# ---------------------------------------------------------------------------
# 消息内容解析
# ---------------------------------------------------------------------------

def _parse_content(t: str, c_raw: str) -> tuple[str, str]:
    """
    解析消息类型 t 和原始内容字段 c_raw，
    返回 (content_text, content_type)
    """
    try:
        c = json.loads(c_raw) if isinstance(c_raw, str) else c_raw
    except Exception:
        return (c_raw or "", "other")

    # 系统消息直接跳过（调用方已过滤，这里作为保险）
    if t.startswith("c.g.") or t.startswith("sys."):
        return ("", "system")

    # 富文本/转发：m 数组（优先于普通文本判断）
    if isinstance(c, dict) and "m" in c and isinstance(c["m"], list):
        parts = []
        for seg in c["m"]:
            tag = seg.get("tag", "")
            seg_c = seg.get("c", {})
            if tag == "text":
                parts.append(seg_c.get("c", ""))
            elif tag == "image":
                parts.append("[图片]")
            elif tag == "file":
                parts.append(f"[文件: {seg_c.get('n', '')}]")
            elif tag == "video":
                parts.append("[视频]")
        return (" ".join(p for p in parts if p), "text")

    # 文本消息
    if t == "text":
        text = c.get("c", "")
        return (text, "text")

    # 图片
    if t == "image":
        return ("[图片]", "image")

    # 文件
    if t == "file":
        name = c.get("n", "未知文件")
        return (f"[文件: {name}]", "file")

    # 视频
    if t == "video":
        return ("[视频]", "video")

    # 贴纸/表情
    if t in ("sticker", "sticker.c"):
        return ("[贴图]", "other")

    # 文章/链接
    if t == "article":
        title = c.get("t", "")
        url = c.get("c", "")
        text = f"[链接] {title} {url}".strip()
        return (text, "link")

    # 兜底：尝试取 c.c
    if isinstance(c, dict) and "c" in c:
        return (str(c["c"]), "other")

    return ("", "other")


# ---------------------------------------------------------------------------
# SeaTalk 采集器
# ---------------------------------------------------------------------------

class SeaTalkCollector:

    def __init__(self, config: dict):
        self.port: int = config["seatalk"]["cdp_port"]
        self.days: int = config["seatalk"]["time_range_days"]
        self.max_per_session: int = config["seatalk"]["max_messages_per_session"]
        self.ignore_patterns: List[re.Pattern] = [
            re.compile(p) for p in config["seatalk"].get("ignore_group_patterns", [])
        ]
        self._cdp = CDPHelper(self.port)

    # ------------------------------------------------------------------
    # 主流程
    # ------------------------------------------------------------------

    def collect(self) -> List[Message]:
        """采集所有活跃会话的消息，返回结构化消息列表"""
        self._cdp.connect()
        try:
            # 等待 Redux store 和 SQLite 接口就绪（SeaTalk 启动后需要一段初始化时间）
            log.info("等待 Redux store / SQLite 初始化...")
            for attempt in range(20):
                try:
                    ready = self._cdp.evaluate(
                        "(typeof store !== 'undefined' && typeof sqlite !== 'undefined') ? 'ok' : 'wait'"
                    )
                    if ready == "ok":
                        log.info("Redux store 和 SQLite 已就绪")
                        break
                except Exception:
                    pass
                log.debug(f"等待 store/sqlite 就绪，重试 {attempt + 1}/20...")
                time.sleep(3)
            else:
                raise RuntimeError("Redux store 或 SQLite 长时间未就绪，请确认 SeaTalk 已完全加载")

            cutoff_ts = int(time.time()) - self.days * 86400

            log.info("正在从 Redux store 加载群名/用户名...")
            session_names = self._load_session_names()
            uid_names = self._load_uid_names()
            log.info(f"已加载 {len(session_names)} 个会话名，{len(uid_names)} 个用户名")

            log.info("正在查询活跃会话...")
            sessions = self._find_active_sessions(cutoff_ts)
            log.info(f"发现活跃会话共 {len(sessions)} 个（最近 {self.days} 天）")

            # 过滤告警群
            filtered = self._filter_sessions(sessions, session_names)
            ignored_count = len(sessions) - len(filtered)
            log.info(
                f"过滤后剩余 {len(filtered)} 个会话"
                + (f"（已忽略 {ignored_count} 个匹配群）" if ignored_count else "")
            )

            all_messages: List[Message] = []
            for idx, (sid, _cnt) in enumerate(filtered, start=1):
                session_name = session_names.get(sid, "")   # 找不到名字时为空，不用 sid 自身兜底
                session_type = "群" if sid.startswith("group-") else "私聊"
                log.info(f"[{idx}/{len(filtered)}] 正在采集: {session_name}（{session_type}）")

                msgs = self._collect_session(sid, session_name, session_names, uid_names, cutoff_ts)
                log.debug(f"  → {len(msgs)} 条消息")
                all_messages.extend(msgs)

            log.info(f"采集完成，共 {len(all_messages)} 条消息")
            return all_messages

        finally:
            self._cdp.close()

    # ------------------------------------------------------------------
    # Redux store 查询
    # ------------------------------------------------------------------

    def _load_session_names(self) -> Dict[str, str]:
        js = """
        (function() {
            var state = store.getState();
            var result = {};
            var groupInfo = (state.contact || {}).groupInfo || {};
            for (var gid in groupInfo) {
                if (groupInfo[gid] && groupInfo[gid].name) result['group-' + gid] = groupInfo[gid].name;
            }
            var userInfo = (state.contact || {}).userInfo || {};
            for (var uid in userInfo) {
                if (userInfo[uid] && userInfo[uid].name) result['buddy-' + uid] = userInfo[uid].name;
            }
            return JSON.stringify(result);
        })()
        """
        raw = self._cdp.evaluate(js)
        return json.loads(raw) if raw else {}

    def _load_uid_names(self) -> Dict[str, str]:
        js = """
        (function() {
            var ui = (store.getState().contact || {}).userInfo || {};
            var names = {};
            for (var uid in ui) {
                if (ui[uid] && ui[uid].name) names[uid] = ui[uid].name;
            }
            return JSON.stringify(names);
        })()
        """
        raw = self._cdp.evaluate(js)
        return json.loads(raw) if raw else {}

    # ------------------------------------------------------------------
    # 会话查询
    # ------------------------------------------------------------------

    def _find_active_sessions(self, cutoff_ts: int) -> List[tuple]:
        """返回 [(sid, msg_count), ...]，按消息数降序，带重试"""
        sql = (
            f"SELECT sid, COUNT(*) as cnt, MAX(ts) as last_ts "
            f"FROM chat_message "
            f"WHERE ts >= {cutoff_ts} "
            f"GROUP BY sid "
            f"ORDER BY cnt DESC"
        )
        js = f"(async()=>{{ var r=await sqlite.all(`{sql}`); return JSON.stringify(r) }})()"

        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                # 预检：用轻量查询确认 SQLite 连接真正可用
                self._cdp.async_evaluate(
                    "(async()=>{ var r=await sqlite.all('SELECT 1 as ok'); return JSON.stringify(r) })()",
                    timeout=15,
                )
                raw = self._cdp.async_evaluate(js, timeout=120)
                rows = json.loads(raw) if raw else []
                return [(r["sid"], r["cnt"]) for r in rows]
            except (TimeoutError, Exception) as e:
                log.warning(f"活跃会话查询失败（第 {attempt}/{max_retries} 次）: {e}")
                if attempt < max_retries:
                    log.info("正在重连 CDP 并重试...")
                    self._cdp.reconnect()
                    # 重连后重新等待 SQLite 就绪
                    time.sleep(5)
                else:
                    raise

    def _filter_sessions(
        self, sessions: List[tuple], session_names: Dict[str, str]
    ) -> List[tuple]:
        result = []
        for sid, cnt in sessions:
            name = session_names.get(sid, "")
            if any(p.search(name) for p in self.ignore_patterns):
                log.debug(f"忽略会话: {name} ({sid})")
                continue
            result.append((sid, cnt))
        return result

    # ------------------------------------------------------------------
    # 消息采集
    # ------------------------------------------------------------------

    def _collect_session(
        self,
        sid: str,
        session_name: str,
        session_names: Dict[str, str],
        uid_names: Dict[str, str],
        cutoff_ts: int,
    ) -> List[Message]:
        sql = (
            f"SELECT mid, u, ts, t, c "
            f"FROM chat_message "
            f"WHERE sid = '{sid}' AND ts >= {cutoff_ts} "
            f"ORDER BY ts ASC "
            f"LIMIT {self.max_per_session}"
        )
        js = f"(async()=>{{ var r=await sqlite.all(`{sql}`); return JSON.stringify(r) }})()"
        raw = self._cdp.async_evaluate(js)
        rows = json.loads(raw) if raw else []

        messages: List[Message] = []
        for row in rows:
            t = row.get("t", "")
            # 过滤系统消息
            if t.startswith("c.g.") or t.startswith("sys."):
                continue

            c_raw = row.get("c", "")
            content, content_type = _parse_content(t, c_raw)

            # 内容为空的消息跳过
            if not content.strip():
                continue

            sender_id = str(row.get("u", ""))
            sender_name = uid_names.get(sender_id, sender_id)

            messages.append(Message(
                mid=str(row.get("mid", "")),
                session_id=sid,
                session_name=session_name,
                sender_id=sender_id,
                sender_name=sender_name,
                timestamp=int(row.get("ts", 0)),
                content=content,
                content_type=content_type,
            ))

        return messages
