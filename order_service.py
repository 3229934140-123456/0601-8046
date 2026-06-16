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
    def mark_published(event_id: str) -> None:
        db.execute(
            """
            UPDATE outbox
            SET status = ?, published_at = datetime('now')
            WHERE event_id = ?
            """,
            (OutboxStatus.PUBLISHED, event_id),
        )

    @staticmethod
    def mark_failed(event_id: str, retry_delay_seconds: int = 5) -> None:
        db.execute(
            """
            UPDATE outbox
            SET status = ?,
                retry_count = retry_count + 1,
                next_retry_at = datetime('now', ?)
            WHERE event_id = ?
            """,
            (OutboxStatus.FAILED, f"+{retry_delay_seconds} seconds", event_id),
        )

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
