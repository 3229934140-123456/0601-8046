import json
import uuid
import time
from typing import Optional, List
from dataclasses import dataclass, asdict

from db import db
from message_queue import DomainEvent


class OutboxStatus:
    PENDING = "PENDING"
    PUBLISHED = "PUBLISHED"
    FAILED = "FAILED"
    DEAD = "DEAD"


DEFAULT_MAX_RETRY = 5


class OutboxRepository:
    @staticmethod
    def insert_event(conn, event: DomainEvent) -> None:
        conn.execute(
            """
            INSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_id,
                event.aggregate_type,
                event.aggregate_id,
                event.event_type,
                json.dumps(event.payload, ensure_ascii=False),
                OutboxStatus.PENDING,
            ),
        )

    @staticmethod
    def fetch_pending(batch_size: int = 10) -> List[dict]:
        rows = db.fetchall(
            """
            SELECT * FROM outbox
            WHERE status IN ('PENDING', 'FAILED')
              AND next_retry_at <= datetime('now')
            ORDER BY id ASC
            LIMIT ?
            """,
            (batch_size,),
        )
        return [dict(r) for r in rows]

    @staticmethod
    def fetch_pending_sharded(
        shard_index: int,
        total_shards: int,
        batch_size: int = 10,
    ) -> List[dict]:
        """
        多实例分片轮询:
          - 每个 Publisher 实例分配 0..total_shards-1 的 shard_index
          - 通过主键 id 取模, 天然把不同主键段分摊给不同实例, 避免重复抢
          - SQLite 下这是最简单有效的多实例协作方案, 不需要分布式锁
        """
        if total_shards <= 1:
            return OutboxRepository.fetch_pending(batch_size)
        rows = db.fetchall(
            """
            SELECT * FROM outbox
            WHERE status IN ('PENDING', 'FAILED')
              AND next_retry_at <= datetime('now')
              AND (id % ?) = ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (total_shards, shard_index, batch_size),
        )
        return [dict(r) for r in rows]

    @staticmethod
    def mark_published(event_id: str) -> None:
        db.execute(
            """
            UPDATE outbox
            SET status = ?, published_at = datetime('now'), error_message = NULL
            WHERE event_id = ?
            """,
            (OutboxStatus.PUBLISHED, event_id),
        )

    @staticmethod
    def mark_failed(
        event_id: str,
        error_message: str,
        retry_delay_seconds: int = 5,
        max_retry: int = DEFAULT_MAX_RETRY,
    ) -> bool:
        """
        记录一次投递失败.
        返回值:
          - True:  仍在重试队列中 (retry_count 未达上限)
          - False: 已经被打入死信 (超过 max_retry), 后续轮询不会再捞它
        """
        current = db.fetchone(
            "SELECT retry_count FROM outbox WHERE event_id = ?",
            (event_id,),
        )
        if current is None:
            return False
        new_retry_count = current["retry_count"] + 1
        if new_retry_count > max_retry:
            db.execute(
                """
                UPDATE outbox
                SET status = ?,
                    retry_count = ?,
                    error_message = ?,
                    dead_letter_at = datetime('now')
                WHERE event_id = ?
                """,
                (OutboxStatus.DEAD, new_retry_count, error_message[:500], event_id),
            )
            return False
        else:
            db.execute(
                """
                UPDATE outbox
                SET status = ?,
                    retry_count = ?,
                    error_message = ?,
                    next_retry_at = datetime('now', ?)
                WHERE event_id = ?
                """,
                (
                    OutboxStatus.FAILED,
                    new_retry_count,
                    error_message[:500],
                    f"+{retry_delay_seconds} seconds",
                    event_id,
                ),
            )
            return True

    @staticmethod
    def list_dead_letters(limit: int = 100) -> List[dict]:
        rows = db.fetchall(
            """
            SELECT id, event_id, aggregate_type, event_type, retry_count,
                   error_message, dead_letter_at, created_at
            FROM outbox
            WHERE status = 'DEAD'
            ORDER BY dead_letter_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(r) for r in rows]

    @staticmethod
    def get_status_counts() -> dict:
        rows = db.fetchall("SELECT status, COUNT(*) c FROM outbox GROUP BY status")
        result = {OutboxStatus.PENDING: 0, OutboxStatus.PUBLISHED: 0,
                  OutboxStatus.FAILED: 0, OutboxStatus.DEAD: 0}
        for r in rows:
            result[r["status"]] = r["c"]
        result["TOTAL"] = sum(result.values())
        return result

    @staticmethod
    def row_to_event(row: dict) -> DomainEvent:
        return DomainEvent(
            event_id=row["event_id"],
            aggregate_type=row["aggregate_type"],
            aggregate_id=row["aggregate_id"],
            event_type=row["event_type"],
            payload=json.loads(row["payload"]),
            timestamp=row["created_at"],
        )


@dataclass
class Order:
    id: Optional[int]
    order_no: str
    user_id: str
    amount: float
    status: str
    created_at: Optional[str] = None


class OrderService:
    @staticmethod
    def create_order(order_no: str, user_id: str, amount: float) -> Order:
        order = Order(id=None, order_no=order_no, user_id=user_id, amount=amount, status="CREATED")

        event = DomainEvent(
            event_id=str(uuid.uuid4()),
            aggregate_type="Order",
            aggregate_id=order_no,
            event_type="OrderCreated",
            payload={
                "order_no": order_no,
                "user_id": user_id,
                "amount": amount,
                "status": "CREATED",
            },
        )

        with db.transaction() as conn:
            cursor = conn.execute(
                """
                INSERT INTO orders (order_no, user_id, amount, status)
                VALUES (?, ?, ?, ?)
                """,
                (order.order_no, order.user_id, order.amount, order.status),
            )
            order.id = cursor.lastrowid

            OutboxRepository.insert_event(conn, event)

        row = db.fetchone("SELECT * FROM orders WHERE id = ?", (order.id,))
        order.created_at = row["created_at"]
        return order

    @staticmethod
    def get_order(order_no: str) -> Optional[Order]:
        row = db.fetchone("SELECT * FROM orders WHERE order_no = ?", (order_no,))
        if not row:
            return None
        return Order(
            id=row["id"],
            order_no=row["order_no"],
            user_id=row["user_id"],
            amount=row["amount"],
            status=row["status"],
            created_at=row["created_at"],
        )
