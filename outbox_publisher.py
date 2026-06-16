import threading
import time
import random
from typing import Optional

from db import db
from order_service import OutboxRepository, DEFAULT_MAX_RETRY
from message_queue import MessageQueue, DomainEvent


class OutboxPublisher:
    def __init__(
        self,
        mq: MessageQueue,
        poll_interval_seconds: float = 1.0,
        batch_size: int = 10,
        topic_prefix: str = "events.",
        shard_index: int = 0,
        total_shards: int = 1,
        max_retry: int = DEFAULT_MAX_RETRY,
        name: Optional[str] = None,
    ):
        self.mq = mq
        self.poll_interval = poll_interval_seconds
        self.batch_size = batch_size
        self.topic_prefix = topic_prefix
        self.shard_index = shard_index
        self.total_shards = total_shards
        self.max_retry = max_retry
        self.name = name or f"Publisher-{shard_index}/{total_shards}"
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stats = {"published": 0, "failed_then_retry": 0, "moved_to_dead": 0}

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        print(
            f"[{self.name}] Started (poll every {self.poll_interval}s, "
            f"shard={self.shard_index}/{self.total_shards}, max_retry={self.max_retry})"
        )

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=3)
        print(f"[{self.name}] Stopped. Stats: {self._stats}")

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
                print(f"[{self.name}] Loop error: {e}")
                time.sleep(self.poll_interval)

    def _process_batch(self) -> int:
        pending = OutboxRepository.fetch_pending_sharded(
            shard_index=self.shard_index,
            total_shards=self.total_shards,
            batch_size=self.batch_size,
        )
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
                with db.transaction():
                    OutboxRepository.mark_published(event.event_id)
                self._stats["published"] += 1
                print(
                    f"[{self.name}] OK event={event.event_id[:12]}... topic={topic}"
                )
            except Exception as e:
                retry_delay = self._compute_backoff(row.get("retry_count", 0))
                error_msg = f"{type(e).__name__}: {e}"
                with db.transaction():
                    still_retriable = OutboxRepository.mark_failed(
                        row["event_id"],
                        error_message=error_msg,
                        retry_delay_seconds=retry_delay,
                        max_retry=self.max_retry,
                    )
                if still_retriable:
                    self._stats["failed_then_retry"] += 1
                    print(
                        f"[{self.name}] RETRY event={row['event_id'][:12]}... "
                        f"err={error_msg} in={retry_delay}s retry_count={row.get('retry_count', 0) + 1}"
                    )
                else:
                    self._stats["moved_to_dead"] += 1
                    print(
                        f"[{self.name}] DEAD event={row['event_id'][:12]}... "
                        f"after {row.get('retry_count', 0) + 1} attempts. err={error_msg}"
                    )
            processed += 1
        return processed

    @staticmethod
    def _compute_backoff(retry_count: int) -> int:
        base = 1
        max_delay = 30
        delay = base * (2 ** min(retry_count, 4))
        jitter = random.randint(0, 2)
        return min(delay + jitter, max_delay)
