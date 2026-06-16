import sqlite3
from typing import Callable
from functools import wraps

from db import db, DB_LOCK
from message_queue import DomainEvent


class IdempotentConsumer:
    def __init__(self, consumer_id: str):
        self.consumer_id = consumer_id
        self._stats = {"processed": 0, "duplicates": 0, "errors": 0}

    def _try_acquire(self, event_id: str) -> bool:
        """
        利用 UNIQUE (consumer_id, event_id) 做原子抢占.
        插入成功 => 本线程抢到处理权;
        IntegrityError => 别的线程/进程已经抢到 => 判定为重复.
        注意: INSERT 不写 OR IGNORE, 故意让唯一约束抛异常,
        这样在并发环境下也能确保只有 1 个线程返回 True.
        """
        try:
            with db.transaction() as conn:
                conn.execute(
                    """
                    INSERT INTO idempotency_store (consumer_id, event_id)
                    VALUES (?, ?)
                    """,
                    (self.consumer_id, event_id),
                )
            return True
        except sqlite3.IntegrityError as e:
            if "UNIQUE" in str(e).upper() or "unique" in str(e).lower():
                return False
            raise

    def _release(self, event_id: str) -> None:
        """
        如果业务处理失败, 回滚掉刚才抢到的占位记录, 让后续重试能再次抢.
        """
        with db.transaction() as conn:
            conn.execute(
                """
                DELETE FROM idempotency_store
                WHERE consumer_id = ? AND event_id = ?
                """,
                (self.consumer_id, event_id),
            )

    def process(self, event: DomainEvent, handler: Callable[[DomainEvent], None]) -> bool:
        acquired = self._try_acquire(event.event_id)
        if not acquired:
            self._stats["duplicates"] += 1
            print(
                f"[{self.consumer_id}] DUPLICATE (原子抢占失败), event={event.event_id[:12]}..."
            )
            return False

        try:
            handler(event)
        except Exception as e:
            self._stats["errors"] += 1
            try:
                self._release(event.event_id)
            except Exception as release_err:
                print(
                    f"[{self.consumer_id}] WARN release failed for {event.event_id[:12]}: {release_err}"
                )
            raise e

        self._stats["processed"] += 1
        return True

    def wrap(self, handler: Callable[[DomainEvent], None]) -> Callable[[DomainEvent], None]:
        @wraps(handler)
        def wrapper(event: DomainEvent) -> None:
            self.process(event, handler)

        return wrapper

    @property
    def stats(self):
        return dict(self._stats)
