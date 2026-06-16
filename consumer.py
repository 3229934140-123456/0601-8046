from typing import Callable
from functools import wraps

from db import db
from message_queue import DomainEvent


class IdempotentConsumer:
    def __init__(self, consumer_id: str):
        self.consumer_id = consumer_id
        self._stats = {"processed": 0, "duplicates": 0}

    def is_processed(self, event_id: str) -> bool:
        row = db.fetchone(
            """
            SELECT 1 FROM idempotency_store
            WHERE consumer_id = ? AND event_id = ?
            """,
            (self.consumer_id, event_id),
        )
        return row is not None

    def mark_processed(self, event_id: str) -> None:
        with db.transaction() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO idempotency_store (consumer_id, event_id)
                VALUES (?, ?)
                """,
                (self.consumer_id, event_id),
            )

    def process(self, event: DomainEvent, handler: Callable[[DomainEvent], None]) -> bool:
        if self.is_processed(event.event_id):
            self._stats["duplicates"] += 1
            print(
                f"[{self.consumer_id}] DUPLICATE detected, skipping event={event.event_id}"
            )
            return False
        try:
            handler(event)
        except Exception as e:
            raise e
        self.mark_processed(event.event_id)
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
