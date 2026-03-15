"""
indexer.py — ChromaDB 向量索引
使用 sentence-transformers 本地模型生成 embedding，增量写入 ChromaDB。
"""

import contextlib
import io
import os
import warnings
from typing import List, Optional

import chromadb
from chromadb.config import Settings
from sentence_transformers import SentenceTransformer

# 屏蔽 HF Hub 未登录的 Python warning
warnings.filterwarnings("ignore", message=".*unauthenticated.*")

from logger import get_logger


@contextlib.contextmanager
def _suppress_stdout_stderr():
    """临时重定向 stdout/stderr，屏蔽第三方库直接打印的噪音输出"""
    with open(os.devnull, "w") as devnull:
        old_stdout, old_stderr = os.dup(1), os.dup(2)
        os.dup2(devnull.fileno(), 1)
        os.dup2(devnull.fileno(), 2)
        try:
            yield
        finally:
            os.dup2(old_stdout, 1)
            os.dup2(old_stderr, 2)
            os.close(old_stdout)
            os.close(old_stderr)

log = get_logger()

COLLECTION_NAME = "seatalk_messages"


class VectorIndexer:

    def __init__(self, config: dict):
        model_name: str = config["embedding"]["model_name"]
        self.batch_size: int = config["embedding"]["batch_size"]
        chroma_path: str = config["storage"]["chroma_path"]

        os.makedirs(chroma_path, exist_ok=True)

        log.info(f"正在加载 Embedding 模型: {model_name}")
        with _suppress_stdout_stderr():
            self._model = SentenceTransformer(model_name)
        log.info("Embedding 模型加载完成")

        self._client = chromadb.PersistentClient(
            path=chroma_path,
            settings=Settings(anonymized_telemetry=False),
        )
        self._collection = self._client.get_or_create_collection(
            name=COLLECTION_NAME,
            metadata={"hnsw:space": "cosine"},
        )
        log.debug(f"ChromaDB collection '{COLLECTION_NAME}' ready, "
                  f"current count: {self._collection.count()}")

    # ------------------------------------------------------------------
    # 增量索引
    # ------------------------------------------------------------------

    def build_index(self, messages: List[dict]) -> int:
        """
        对 messages 列表生成 embedding 并写入 ChromaDB。
        返回实际写入条数。
        """
        if not messages:
            log.info("没有需要索引的新消息")
            return 0

        log.info(f"正在对 {len(messages)} 条新消息生成向量...")
        total_indexed = 0
        batches = [
            messages[i: i + self.batch_size]
            for i in range(0, len(messages), self.batch_size)
        ]

        for batch_idx, batch in enumerate(batches, start=1):
            texts = [m["content"] for m in batch]
            ids = [m["id"] for m in batch]
            metadatas = [
                {
                    "session_id": m["session_id"],
                    "session_name": m["session_name"] or "",
                    "sender_id": m["sender_id"] or "",
                    "sender_name": m["sender_name"] or "",
                    "timestamp": int(m["timestamp"]),
                    "content_type": m["content_type"] or "",
                }
                for m in batch
            ]

            embeddings = self._model.encode(
                texts,
                batch_size=self.batch_size,
                show_progress_bar=False,
                normalize_embeddings=True,
            ).tolist()

            # ChromaDB upsert（幂等）
            self._collection.upsert(
                ids=ids,
                embeddings=embeddings,
                documents=texts,
                metadatas=metadatas,
            )
            total_indexed += len(batch)
            log.debug(f"向量索引批次 [{batch_idx}/{len(batches)}] 完成，共 {total_indexed} 条")

        log.info(f"向量索引完成，共写入 {total_indexed} 条")
        return total_indexed

    # ------------------------------------------------------------------
    # 语义搜索
    # ------------------------------------------------------------------

    def search(self, query: str, top_k: int = 200,
               time_from: int = 0, time_to: int = 0,
               session_ids: List[str] = None,
               sender_ids: List[str] = None) -> List[dict]:
        """
        语义搜索，支持时间范围和会话 ID 过滤。
        返回按相似度排序的结果列表。
        """
        if self._collection.count() == 0:
            return []

        # 构造 ChromaDB where 子句
        where = self._build_where(time_from, time_to, session_ids, sender_ids)

        query_embedding = self._model.encode(
            [query],
            normalize_embeddings=True,
        ).tolist()

        try:
            n_results = min(top_k, self._collection.count())
            kwargs = dict(
                query_embeddings=query_embedding,
                n_results=n_results,
                include=["documents", "metadatas", "distances"],
            )
            if where:
                kwargs["where"] = where
            results = self._collection.query(**kwargs)
        except Exception as e:
            log.debug(f"ChromaDB query error: {e}")
            return []

        output = []
        ids = results.get("ids", [[]])[0]
        documents = results.get("documents", [[]])[0]
        metadatas = results.get("metadatas", [[]])[0]
        distances = results.get("distances", [[]])[0]

        for msg_id, doc, meta, dist in zip(ids, documents, metadatas, distances):
            score = round(1.0 - dist, 4)
            output.append({
                "id": msg_id,
                "score": score,
                "content": doc,
                "session_id": meta.get("session_id", ""),
                "session_name": meta.get("session_name", ""),
                "sender_id": meta.get("sender_id", ""),
                "sender_name": meta.get("sender_name", ""),
                "timestamp": meta.get("timestamp", 0),
                "content_type": meta.get("content_type", ""),
            })

        return output

    @staticmethod
    def _build_where(time_from: int, time_to: int,
                     session_ids: List[str],
                     sender_ids: List[str] = None) -> Optional[dict]:
        """构造 ChromaDB where 过滤条件"""
        conditions = []
        if time_from > 0:
            conditions.append({"timestamp": {"$gte": time_from}})
        if time_to > 0:
            conditions.append({"timestamp": {"$lte": time_to}})
        if session_ids:
            conditions.append({"session_id": {"$in": session_ids}})
        if sender_ids:
            conditions.append({"sender_id": {"$in": sender_ids}})
        if not conditions:
            return None
        if len(conditions) == 1:
            return conditions[0]
        return {"$and": conditions}

    def delete_by_ids(self, ids: List[str]) -> None:
        """从 ChromaDB 中删除指定 id 的向量记录"""
        existing = self._collection.get(ids=ids)["ids"]
        if existing:
            self._collection.delete(ids=existing)
            log.debug(f"从 ChromaDB 删除 {len(existing)} 条向量记录")

    def collection_count(self) -> int:
        return self._collection.count()
