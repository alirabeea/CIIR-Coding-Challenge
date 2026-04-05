from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from threading import Lock
from typing import Any


class PersistentCache:
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()
        self._connection = sqlite3.connect(self.db_path, check_same_thread=False)
        with self._connection:
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS cache_entries (
                    namespace TEXT NOT NULL,
                    cache_key TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    expires_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (namespace, cache_key)
                )
                """
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_cache_entries_expires_at ON cache_entries (expires_at)"
            )

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def _lookup(self, namespace: str, cache_key: str) -> tuple[str, float, float] | None:
        return self._connection.execute(
            """
            SELECT payload, expires_at, updated_at
            FROM cache_entries
            WHERE namespace = ? AND cache_key = ?
            """,
            (namespace, cache_key),
        ).fetchone()

    def get(self, namespace: str, cache_key: str) -> Any | None:
        now = time.time()
        with self._lock:
            row = self._lookup(namespace, cache_key)
            if row is None:
                return None

            payload, expires_at, _ = row
            if expires_at <= now:
                self._connection.execute(
                    "DELETE FROM cache_entries WHERE namespace = ? AND cache_key = ?",
                    (namespace, cache_key),
                )
                self._connection.commit()
                return None

        return json.loads(payload)

    def get_with_state(
        self,
        namespace: str,
        cache_key: str,
        *,
        allow_stale: bool = False,
        stale_ttl_seconds: float = 0.0,
    ) -> tuple[Any | None, str]:
        now = time.time()
        with self._lock:
            row = self._lookup(namespace, cache_key)
            if row is None:
                return None, "miss"

            payload, expires_at, _ = row
            if expires_at > now:
                return json.loads(payload), "fresh"

            stale_window_expires_at = expires_at + max(stale_ttl_seconds, 0.0)
            if allow_stale and stale_window_expires_at > now:
                return json.loads(payload), "stale"

            self._connection.execute(
                "DELETE FROM cache_entries WHERE namespace = ? AND cache_key = ?",
                (namespace, cache_key),
            )
            self._connection.commit()
            return None, "miss"

    def set(self, namespace: str, cache_key: str, payload: Any, ttl_seconds: float) -> None:
        now = time.time()
        serialized = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO cache_entries (namespace, cache_key, payload, expires_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(namespace, cache_key) DO UPDATE SET
                    payload = excluded.payload,
                    expires_at = excluded.expires_at,
                    updated_at = excluded.updated_at
                """,
                (namespace, cache_key, serialized, now + ttl_seconds, now),
            )
            self._connection.execute(
                "DELETE FROM cache_entries WHERE expires_at <= ?",
                (now,),
            )
