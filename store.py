"""SQLite-based notice storage with deduplication."""

import os
import json
import hashlib
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Optional, Set
from contextlib import contextmanager


class NoticeStore:
    """基于 SQLite 的公告存储，支持去重和历史查询。"""

    def __init__(self, db_path: Optional[str] = None):
        if db_path is None:
            db_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "notices.db",
            )
        self.db_path = db_path
        self._init_db()

    @contextmanager
    def _connect(self, readonly=False):
        """Yield a connection; auto-commit on normal exit."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            if not readonly:
                conn.commit()
        finally:
            conn.close()

    def _init_db(self):
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS notices (
                    id          TEXT PRIMARY KEY,
                    title       TEXT NOT NULL,
                    link        TEXT,
                    publish_date TEXT,
                    area_type   TEXT DEFAULT 'unknown',
                    matched_keywords TEXT,
                    crawl_time  TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_crawl_time ON notices(crawl_time)
            """)

    @staticmethod
    def _make_id(title: str, publish_date: str) -> str:
        raw = f"{title.strip()}|{publish_date.strip()}"
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    def exists(self, title: str, publish_date: str) -> bool:
        nid = self._make_id(title, publish_date)
        with self._connect(readonly=True) as conn:
            row = conn.execute(
                "SELECT 1 FROM notices WHERE id = ?", (nid,)
            ).fetchone()
            return row is not None

    def insert(self, notice: Dict) -> bool:
        """插入一条公告，若已存在则返回 False。"""
        nid = self._make_id(
            notice.get("title", ""), notice.get("publish_date", "")
        )
        if self.exists(notice.get("title", ""), notice.get("publish_date", "")):
            return False
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO notices "
                "(id, title, link, publish_date, area_type, matched_keywords, crawl_time) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    nid,
                    notice.get("title", ""),
                    notice.get("link", ""),
                    notice.get("publish_date", ""),
                    notice.get("area_type", "unknown"),
                    json.dumps(
                        notice.get("matched_keywords", []), ensure_ascii=False
                    ),
                    notice.get(
                        "crawl_time",
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    ),
                ),
            )
        return True

    def get_recent_ids(self, days: int = 7) -> Set[str]:
        """返回最近 N 天内已存在的公告 ID 集合。"""
        cutoff = datetime.now() - timedelta(days=days)
        with self._connect(readonly=True) as conn:
            rows = conn.execute(
                "SELECT id FROM notices WHERE crawl_time > ?",
                (cutoff.strftime("%Y-%m-%d %H:%M:%S"),),
            ).fetchall()
            return {row["id"] for row in rows}
