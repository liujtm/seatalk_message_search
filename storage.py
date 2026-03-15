"""
storage.py — SQLite 持久化层
消息去重 upsert、索引状态追踪、同步日志
"""

import sqlite3
import time
from contextlib import contextmanager
from typing import List, Optional

import jieba

from collector import Message
from logger import get_logger

log = get_logger()

CREATE_MESSAGES = """
CREATE TABLE IF NOT EXISTS messages (
    id            TEXT PRIMARY KEY,
    session_id    TEXT NOT NULL,
    session_name  TEXT,
    sender_id     TEXT,
    sender_name   TEXT,
    timestamp     INTEGER NOT NULL,
    content       TEXT,
    content_type  TEXT,
    indexed       INTEGER DEFAULT 0,
    created_at    INTEGER
)
"""

CREATE_SESSIONS = """
CREATE TABLE IF NOT EXISTS sessions (
    session_id   TEXT PRIMARY KEY,
    session_name TEXT,
    updated_at   INTEGER
)
"""

CREATE_SYNC_LOG = """
CREATE TABLE IF NOT EXISTS sync_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    synced_at   INTEGER,
    msg_count   INTEGER,
    new_count   INTEGER,
    status      TEXT
)
"""

CREATE_IDX_TS = "CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(timestamp)"
CREATE_IDX_SID = "CREATE INDEX IF NOT EXISTS idx_messages_sid ON messages(session_id)"


class SQLiteStorage:

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    @contextmanager
    def _conn(self):
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        try:
            yield con
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

    def _init_db(self) -> None:
        import os
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        with self._conn() as con:
            con.execute(CREATE_MESSAGES)
            con.execute(CREATE_SESSIONS)
            con.execute(CREATE_SYNC_LOG)
            con.execute(CREATE_IDX_TS)
            con.execute(CREATE_IDX_SID)
        log.debug(f"SQLite 初始化完成: {self.db_path}")

    # ------------------------------------------------------------------
    # 消息写入
    # ------------------------------------------------------------------

    def upsert_messages(self, messages: List[Message]) -> int:
        """
        批量 upsert 消息，返回新增条数。
        已存在（相同 mid）的记录跳过（不覆盖 indexed 状态）。
        """
        now = int(time.time())
        new_count = 0
        with self._conn() as con:
            for msg in messages:
                cur = con.execute(
                    """
                    INSERT OR IGNORE INTO messages
                        (id, session_id, session_name, sender_id, sender_name,
                         timestamp, content, content_type, indexed, created_at)
                    VALUES (?,?,?,?,?,?,?,?,0,?)
                    """,
                    (
                        msg.mid,
                        msg.session_id,
                        msg.session_name,
                        msg.sender_id,
                        msg.sender_name,
                        msg.timestamp,
                        msg.content,
                        msg.content_type,
                        now,
                    ),
                )
                if cur.rowcount > 0:
                    new_count += 1

                # 更新 sessions 表
                con.execute(
                    """
                    INSERT OR REPLACE INTO sessions (session_id, session_name, updated_at)
                    VALUES (?,?,?)
                    """,
                    (msg.session_id, msg.session_name, now),
                )

        log.debug(f"upsert {len(messages)} 条，新增 {new_count} 条")
        return new_count

    # ------------------------------------------------------------------
    # 索引状态
    # ------------------------------------------------------------------

    # 纯占位文本，无语义价值，不应进入向量索引
    _PLACEHOLDER_CONTENTS = {"[图片]", "[视频]", "[贴图]"}

    def get_unindexed_messages(self, limit: int) -> List[dict]:
        """返回尚未向量化的消息（indexed=0），排除纯占位内容，最多返回 limit 条"""
        with self._conn() as con:
            rows = con.execute(
                """
                SELECT id, session_id, session_name, sender_id, sender_name,
                       timestamp, content, content_type
                FROM messages
                WHERE indexed = 0
                  AND content != ''
                  AND content NOT IN ('[图片]', '[视频]', '[贴图]')
                  AND content_type IN ('text', 'link', 'file', 'image', 'video', 'other')
                ORDER BY timestamp ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_placeholder_ids(self) -> List[str]:
        """返回库中所有纯占位内容消息的 id，用于从向量索引中清除"""
        with self._conn() as con:
            rows = con.execute(
                """
                SELECT id FROM messages
                WHERE content IN ('[图片]', '[视频]', '[贴图]')
                  AND indexed = 1
                """
            ).fetchall()
        return [r["id"] for r in rows]

    def mark_unindexed(self, ids: List[str]) -> None:
        """将指定消息重置为未索引状态"""
        if not ids:
            return
        with self._conn() as con:
            con.executemany(
                "UPDATE messages SET indexed=0 WHERE id=?",
                [(i,) for i in ids],
            )

    def reset_all_indexed(self) -> None:
        """将所有消息重置为未索引状态（配合清空 ChromaDB 使用）"""
        with self._conn() as con:
            con.execute("UPDATE messages SET indexed=0")
        log.info("所有消息已重置为未索引状态")

    def mark_indexed(self, ids: List[str]) -> None:
        """标记消息为已向量化"""
        if not ids:
            return
        with self._conn() as con:
            con.executemany(
                "UPDATE messages SET indexed=1 WHERE id=?",
                [(i,) for i in ids],
            )
        log.debug(f"标记 {len(ids)} 条消息为已索引")

    # ------------------------------------------------------------------
    # 同步日志
    # ------------------------------------------------------------------

    def write_sync_log(self, msg_count: int, new_count: int, status: str) -> None:
        with self._conn() as con:
            con.execute(
                "INSERT INTO sync_log (synced_at, msg_count, new_count, status) VALUES (?,?,?,?)",
                (int(time.time()), msg_count, new_count, status),
            )

    def get_last_sync(self) -> Optional[dict]:
        with self._conn() as con:
            row = con.execute(
                "SELECT * FROM sync_log ORDER BY id DESC LIMIT 1"
            ).fetchone()
        return dict(row) if row else None

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def keyword_search(self, query: str, limit: int = 500,
                       time_from: int = 0, time_to: int = 0,
                       session_ids: List[str] = None,
                       sender_ids: List[str] = None) -> List[dict]:
        """
        按 token 分别做 LIKE 搜索，合并结果并记录每条消息命中了几个 token。
        中文 token 额外展开为单字匹配（如"删除"→同时尝试"删"/"除"），
        使"删一个"也能命中"删除" token。
        支持 time_from / time_to（Unix 时间戳）和 session_ids 过滤。
        返回列表中每条记录额外包含 matched_tokens（int）字段。
        """
        tokens = [t.strip() for t in query.split() if t.strip()]
        if not tokens:
            return []

        # 构造过滤条件
        filter_clauses: List[str] = []
        filter_params: List = []
        if time_from > 0:
            filter_clauses.append("AND timestamp >= ?")
            filter_params.append(time_from)
        if time_to > 0:
            filter_clauses.append("AND timestamp <= ?")
            filter_params.append(time_to)
        if session_ids:
            ph = ",".join("?" * len(session_ids))
            filter_clauses.append(f"AND session_id IN ({ph})")
            filter_params.extend(session_ids)
        if sender_ids:
            ph = ",".join("?" * len(sender_ids))
            filter_clauses.append(f"AND sender_id IN ({ph})")
            filter_params.extend(sender_ids)
        filter_sql = " ".join(filter_clauses)

        merged: dict = {}
        with self._conn() as con:
            for token in tokens:
                # jieba 分词展开：仅保留 ≥2字的实词作为子词，单字虚词丢弃
                # 例："事情在并行" → ["事情", "并行"]；"删除" → (无子词)
                sub_words = [
                    w for w in jieba.lcut(token, cut_all=False)
                    if len(w) >= 2 and w != token
                ]

                # 先查完整短语命中的 id，标记为精确命中
                phrase_hit_ids: set = set()
                rows = con.execute(
                    f"SELECT id FROM messages WHERE content LIKE ? {filter_sql} LIMIT ?",
                    [f"%{token}%"] + filter_params + [limit],
                ).fetchall()
                for row in rows:
                    phrase_hit_ids.add(row["id"])

                # 再查子词命中的 id
                char_hit_ids: set = set()
                for word in sub_words:
                    rows = con.execute(
                        f"SELECT id FROM messages WHERE content LIKE ? {filter_sql} LIMIT ?",
                        [f"%{word}%"] + filter_params + [limit],
                    ).fetchall()
                    for row in rows:
                        char_hit_ids.add(row["id"])

                token_hit_ids = phrase_hit_ids | char_hit_ids

                if not token_hit_ids:
                    continue

                placeholders = ",".join("?" * len(token_hit_ids))
                rows = con.execute(
                    f"""
                    SELECT id, session_id, session_name, sender_id, sender_name,
                           timestamp, content, content_type
                    FROM messages WHERE id IN ({placeholders})
                    """,
                    list(token_hit_ids),
                ).fetchall()
                for row in rows:
                    r = dict(row)
                    if r["id"] in merged:
                        merged[r["id"]]["matched_tokens"] += 1
                        # 有一次是精确短语命中就标记
                        if r["id"] in phrase_hit_ids:
                            merged[r["id"]]["phrase_match"] = True
                    else:
                        r["matched_tokens"] = 1
                        r["phrase_match"] = r["id"] in phrase_hit_ids
                        merged[r["id"]] = r

        return list(merged.values())

    def get_messages_by_filter(
        self,
        time_from: int = 0,
        time_to: int = 0,
        session_ids: List[str] = None,
        sender_ids: List[str] = None,
        page: int = 1,
        page_size: int = 20,
    ):
        """按条件过滤消息，按时间倒序分页，返回 (total, rows)"""
        clauses, params = [], []
        if time_from > 0:
            clauses.append("timestamp >= ?")
            params.append(time_from)
        if time_to > 0:
            clauses.append("timestamp <= ?")
            params.append(time_to)
        if session_ids:
            ph = ",".join("?" * len(session_ids))
            clauses.append(f"session_id IN ({ph})")
            params.extend(session_ids)
        if sender_ids:
            ph = ",".join("?" * len(sender_ids))
            clauses.append(f"sender_id IN ({ph})")
            params.extend(sender_ids)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._conn() as con:
            total = con.execute(
                f"SELECT COUNT(*) as cnt FROM messages {where}", params
            ).fetchone()["cnt"]
            offset = (page - 1) * page_size
            rows = con.execute(
                f"""
                SELECT id, session_id, session_name, sender_id, sender_name,
                       timestamp, content, content_type
                FROM messages {where}
                ORDER BY timestamp DESC
                LIMIT ? OFFSET ?
                """,
                params + [page_size, offset],
            ).fetchall()
        return total, [dict(r) for r in rows]

    def get_senders(self, session_ids: List[str] = None, limit: int = 300) -> List[dict]:
        """返回发送者列表（最新 sender_name），可按 session_ids 过滤，按消息数降序"""
        clauses, params = [], []
        if session_ids:
            ph = ",".join("?" * len(session_ids))
            clauses.append(f"session_id IN ({ph})")
            params.extend(session_ids)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._conn() as con:
            rows = con.execute(
                f"""
                SELECT m.sender_id,
                       (SELECT sender_name FROM messages
                        WHERE sender_id = m.sender_id
                        ORDER BY timestamp DESC LIMIT 1) AS sender_name,
                       COUNT(*) AS msg_count
                FROM messages m
                {where}
                GROUP BY m.sender_id
                ORDER BY msg_count DESC
                LIMIT ?
                """,
                params + [limit],
            ).fetchall()
        return [dict(r) for r in rows]

    def get_session_ids_by_type(self, session_type: str) -> List[str]:
        """返回指定类型（group/contact）的所有 session_id"""
        if session_type == "group":
            prefix = "group-%"
        elif session_type == "contact":
            prefix = "buddy-%"
        else:
            return []
        with self._conn() as con:
            rows = con.execute(
                "SELECT DISTINCT session_id FROM messages WHERE session_id LIKE ?",
                (prefix,),
            ).fetchall()
        return [r["session_id"] for r in rows]

    def get_sessions(self) -> dict:
        """返回数据库中所有群和私聊的列表及消息数，按消息数降序"""
        with self._conn() as con:
            rows = con.execute(
                """
                SELECT m.session_id,
                       COALESCE(
                           NULLIF(s.session_name, m.session_id),
                           NULLIF(m.session_name, m.session_id)
                       ) AS session_name,
                       COUNT(*) AS msg_count
                FROM messages m
                LEFT JOIN sessions s ON m.session_id = s.session_id
                GROUP BY m.session_id
                ORDER BY msg_count DESC
                """
            ).fetchall()
        groups, contacts = [], []
        for r in rows:
            d = dict(r)
            if d["session_id"].startswith("group-"):
                groups.append(d)
            elif d["session_id"].startswith("buddy-"):
                contacts.append(d)
        return {"groups": groups, "contacts": contacts}

    def get_stats(self) -> dict:
        """返回聊天记录统计数据"""
        with self._conn() as con:
            total = con.execute(
                "SELECT COUNT(*) as cnt FROM messages"
            ).fetchone()["cnt"]

            tr = con.execute(
                "SELECT MIN(timestamp) as earliest, MAX(timestamp) as latest FROM messages"
            ).fetchone()

            daily = con.execute(
                """
                SELECT date(timestamp, 'unixepoch', 'localtime') as day,
                       COUNT(*) as cnt
                FROM messages
                GROUP BY day ORDER BY day ASC
                """
            ).fetchall()

            top_groups = con.execute(
                """
                SELECT COALESCE(
                           NULLIF(s.session_name, m.session_id),
                           NULLIF(m.session_name, m.session_id),
                           m.session_id
                       ) AS session_name,
                       COUNT(*) AS cnt
                FROM messages m
                LEFT JOIN sessions s ON m.session_id = s.session_id
                WHERE m.session_id LIKE 'group-%'
                GROUP BY m.session_id ORDER BY cnt DESC LIMIT 20
                """
            ).fetchall()

            top_contacts = con.execute(
                """
                SELECT COALESCE(
                           NULLIF(s.session_name, m.session_id),
                           NULLIF(m.session_name, m.session_id),
                           m.session_id
                       ) AS session_name,
                       COUNT(*) AS cnt
                FROM messages m
                LEFT JOIN sessions s ON m.session_id = s.session_id
                WHERE m.session_id LIKE 'buddy-%'
                GROUP BY m.session_id ORDER BY cnt DESC LIMIT 20
                """
            ).fetchall()

            # 按小时分布（0-23）
            hourly = con.execute(
                """
                SELECT CAST(strftime('%H', timestamp, 'unixepoch', 'localtime') AS INTEGER) as hour,
                       COUNT(*) as cnt
                FROM messages
                GROUP BY hour ORDER BY hour ASC
                """
            ).fetchall()

            # 按星期分布（0=周日, 1=周一, ..., 6=周六）
            weekday = con.execute(
                """
                SELECT CAST(strftime('%w', timestamp, 'unixepoch', 'localtime') AS INTEGER) as dow,
                       COUNT(*) as cnt
                FROM messages
                GROUP BY dow ORDER BY dow ASC
                """
            ).fetchall()

        return {
            "total": total,
            "earliest": tr["earliest"],
            "latest": tr["latest"],
            "daily": [dict(r) for r in daily],
            "top_groups": [dict(r) for r in top_groups],
            "top_contacts": [dict(r) for r in top_contacts],
            "hourly": [dict(r) for r in hourly],
            "weekday": [dict(r) for r in weekday],
        }

    def get_messages_by_ids(self, ids: List[str]) -> List[dict]:
        if not ids:
            return []
        placeholders = ",".join("?" * len(ids))
        with self._conn() as con:
            rows = con.execute(
                f"""
                SELECT id, session_id, session_name, sender_id, sender_name,
                       timestamp, content, content_type
                FROM messages
                WHERE id IN ({placeholders})
                """,
                ids,
            ).fetchall()
        return [dict(r) for r in rows]

    def total_message_count(self) -> int:
        with self._conn() as con:
            row = con.execute("SELECT COUNT(*) as cnt FROM messages").fetchone()
        return row["cnt"] if row else 0
