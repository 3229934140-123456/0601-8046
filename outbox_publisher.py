import threading
import time
import random
from typing import Optional

from db import db
from order_service import OutboxRepository
from message_queue import MessageQueue, DomainEvent


class OutboxPublisher:
    def __init__(
        self,
        mq: MessageQueue,
        poll_interval_seconds: float = 1.0,
        batch_size: int = 10,
        topic_prefix: str = "events.",
    ):
        self.mq = mq
        self.poll_interval = poll_interval_seconds
        self.batch_size = batch_size
        self.topic_prefix = topic_prefix
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stats = {"published": 0, "failed": 0, "retries": 0}

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        print(f"[OutboxPublisher] Started (poll every {self.poll_interval}s)")

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=3)
        print(f"[OutboxPublisher] Stopped. Stats: {self._stats}")

    def _topic_for(self, aggregate_type: str, event_type: str) -> str:
        return f"{self.topic_prefix}{aggregate_type.lower()}.{event_type.lower()}"

    def _run_loop(self) -> None:
        consecutive_empty = 0
        while self._running:
            try:
                processed = self._process_batch()
                if processed == 0:
                    consecutive_empty += 1
                else:
                    consecutive_empty = 0
                sleep_for = self.poll_interval
                if consecutive_empty > 5:
                    sleep_for = min(self.poll_interval * 2, 5.0)
                time.sleep(sleep_for)
            except Exception as e:
                print(f"[OutboxPublisher] Loop error: {e}")
                time.sleep(self.poll_interval)

    def _process_batch(self) -> int:
        pending = OutboxRepository.fetch_pending(batch_size=self.batch_size)
        if not pending:
            return 0

        processed = 0
        for row in pending:
            if not self._running:
                break
            try:
                event = OutboxRepository.row_to_event(row)
                topic = self._topic_for(event.aggregate_type, event.event_type)
                self.mq.publish(topic, event)
                with db.transaction() as conn:
                    OutboxRepository.mark_published(event.event_id)
                self._stats["published"] += 1
                print(f"[OutboxPublisher] OK event={event.event_id} topic={topic}")
            except Exception as e:
                self._stats["failed"] += 1
                retry_delay = self._compute_backoff(row.get("retry_count", 0))
                try:
                    with db.transaction() as conn:
                        OutboxRepository.mark_failed(row["event_id"], retry_delay_seconds=retry_delay)
                    self._stats["retries"] += 1
                    print(
                        f"[OutboxPublisher] FAIL event={row['event_id']} "
                        f"error={e} retry_in={retry_delay}s retry_count={row.get('retry_count', 0)}"
                    )
                except Exception as inner:
                    print(f"[OutboxPublisher] Failed to mark failed: {inner}")
            processed += 1
        return processed

    @staticmethod
    def _compute_backoff(retry_count: int) -> int:
        base = 5
        max_delay = 300
        delay = base * (2 ** min(retry_count, 6))
        jitter = random.randint(0, 5)
        return min(delay + jitter, max_delay)
