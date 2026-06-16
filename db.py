import sqlite3
import threading
import os
from contextlib import contextmanager
from typing import Optional, Iterator

DB_LOCK = threading.RLock()


class Database:
    def __init__(self, db_path: str = "outbox_demo.db"):
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(
                self.db_path,
                isolation_level=None,
                timeout=30.0,
                check_same_thread=False,
            )
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def init_schema(self):
        conn = self._get_conn()
        with DB_LOCK:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_no TEXT UNIQUE NOT NULL,
                    user_id TEXT NOT NULL,
                    amount REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'CREATED',
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS outbox (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT UNIQUE NOT NULL,
                    aggregate_type TEXT NOT NULL,
                    aggregate_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'PENDING',
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    published_at TEXT,
                    next_retry_at TEXT NOT NULL DEFAULT (datetime('now')),
                    error_message TEXT,
                    dead_letter_at TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_outbox_status_next ON outbox(status, next_retry_at);

                CREATE INDEX IF NOT EXISTS idx_outbox_dead ON outbox(status) WHERE status = 'DEAD';

                -- 兼容升级：给旧表补新列（SQLite 无视已存在列的 ADD COLUMN 会报错，所以用 PRAGMA 判断）
                -- (由于 SQLite 不支持 IF NOT EXISTS ADD COLUMN，以下通过应用层 try/except 跳过)

                CREATE TABLE IF NOT EXISTS idempotency_store (
                    consumer_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    processed_at TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY (consumer_id, event_id)
                );
            """)

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self._get_conn()
        with DB_LOCK:
            try:
                conn.execute("BEGIN IMMEDIATE")
                yield conn
                conn.execute("COMMIT")
            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                raise

    def execute(self, sql: str, params: tuple = ()):
        conn = self._get_conn()
        with DB_LOCK:
            return conn.execute(sql, params)

    def executemany(self, sql: str, params_seq):
        conn = self._get_conn()
        with DB_LOCK:
            return conn.executemany(sql, params_seq)

    def fetchall(self, sql: str, params: tuple = ()):
        conn = self._get_conn()
        with DB_LOCK:
            cur = conn.execute(sql, params)
            return cur.fetchall()

    def fetchone(self, sql: str, params: tuple = ()):
        conn = self._get_conn()
        with DB_LOCK:
            cur = conn.execute(sql, params)
            return cur.fetchone()


db = Database()
