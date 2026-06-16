import os
import sys
import time
import uuid

from db import db
from message_queue import mq, InMemoryMessageQueue, DomainEvent
from order_service import OrderService, OutboxRepository
from outbox_publisher import OutboxPublisher
from consumer import IdempotentConsumer


def cleanup_db():
    if os.path.exists("outbox_demo.db"):
        os.remove("outbox_demo.db")


def print_separator(title: str):
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def demo_1_normal_flow():
    print_separator("Demo 1: 正常业务流程 - 原子写入+可靠投递+幂等消费")

    cleanup_db()
    db.init_schema()
    global mq
    mq_local = InMemoryMessageQueue()

    publisher = OutboxPublisher(mq_local, poll_interval_seconds=0.3)

    consumed_orders = []

    def inventory_handler(event: DomainEvent):
        print(f"  [Inventory-Service] Processing OrderCreated: {event.payload}")
        consumed_orders.append(event.payload["order_no"])

    consumer = IdempotentConsumer(consumer_id="inventory-service")
    mq_local.subscribe("events.order.ordercreated", consumer.wrap(inventory_handler))

    publisher.start()

    print("\n  >>> 创建3个订单...")
    for i in range(1, 4):
        order_no = f"ORD-{int(time.time())}-{i}"
        order = OrderService.create_order(order_no=order_no, user_id=f"user{i}", amount=100.0 * i)
        print(f"  [Order-Service] 创建订单: {order.order_no}, amount={order.amount}")

    print("\n  >>> 等待 Publisher 投递和 Consumer 消费...")
    time.sleep(2)

    publisher.stop()
    mq_local.stop()

    print(f"\n  消费到的订单数量: {len(consumed_orders)}")
    print(f"  Publisher stats: {publisher._stats}")
    print(f"  Consumer stats: {consumer.stats}")

    outbox_rows = db.fetchall("SELECT event_id, status, event_type FROM outbox")
    print(f"\n  发件箱表状态:")
    for r in outbox_rows:
        print(f"    {r['event_id'][:12]}... -> {r['status']} ({r['event_type']})")

    assert len(consumed_orders) == 3, "应该消费到3个订单"
    print("\n  ✅ Demo 1 成功: 3条消息全部可靠投递并被消费")


def demo_2_atomicity():
    print_separator("Demo 2: 原子性验证 - 事务回滚时消息也不会入库")

    cleanup_db()
    db.init_schema()

    print("\n  >>> 模拟在同一事务中写入订单和消息后抛出异常...")

    order_no = f"ORD-ROLLBACK-{int(time.time())}"
    event_id = str(uuid.uuid4())
    try:
        with db.transaction() as tx_conn:
            tx_conn.execute(
                "INSERT INTO orders (order_no, user_id, amount, status) VALUES (?, ?, ?, ?)",
                (order_no, "user99", 999.9, "CREATED"),
            )
            event = DomainEvent(
                event_id=event_id,
                aggregate_type="Order",
                aggregate_id=order_no,
                event_type="OrderCreated",
                payload={"order_no": order_no},
            )
            OutboxRepository.insert_event(tx_conn, event)
            print(f"  已写入订单和发件箱消息，现在抛出异常触发回滚...")
            raise RuntimeError("模拟业务异常")
    except RuntimeError as e:
        print(f"  捕获异常: {e}")

    order_count = db.fetchone("SELECT COUNT(*) c FROM orders WHERE order_no = ?", (order_no,))["c"]
    outbox_count = db.fetchone("SELECT COUNT(*) c FROM outbox WHERE event_id = ?", (event_id,))["c"]

    print(f"\n  订单表中该订单数量: {order_count}")
    print(f"  发件箱表中该消息数量: {outbox_count}")

    assert order_count == 0, "订单应该被回滚"
    assert outbox_count == 0, "消息应该和订单一起被回滚"
    print("\n  ✅ Demo 2 成功: 事务回滚时订单和消息同时消失，保证原子性")


def demo_3_crash_recovery():
    print_separator("Demo 3: 崩溃恢复 - Publisher重启后自动补偿未投递消息")

    cleanup_db()
    db.init_schema()
    mq_local = InMemoryMessageQueue(simulate_network_failure_rate=1.0)

    publisher = OutboxPublisher(mq_local, poll_interval_seconds=0.2)

    print("\n  >>> 先创建3个订单（此时Publisher还未启动或MQ不可用）...")
    orders = []
    for i in range(1, 4):
        order_no = f"ORD-CRASH-{int(time.time())}-{i}"
        order = OrderService.create_order(order_no=order_no, user_id=f"user{i}", amount=200.0 * i)
        orders.append(order)
        print(f"  [Order-Service] 创建订单: {order.order_no}")

    pending = db.fetchone("SELECT COUNT(*) c FROM outbox WHERE status = 'PENDING'")["c"]
    print(f"\n  当前发件箱 PENDING 消息数: {pending}")
    assert pending == 3, "3条消息应该都在PENDING状态"

    print("\n  >>> 现在启动 Publisher 并把MQ恢复正常...")
    mq_local.simulate_network_failure_rate = 0.0

    consumed = []

    def handler(event: DomainEvent):
        consumed.append(event.event_id)
        print(f"  [Consumer] 收到补偿消息: {event.event_id[:12]}...")

    consumer = IdempotentConsumer("receiver")
    mq_local.subscribe("events.order.ordercreated", consumer.wrap(handler))

    publisher.start()
    time.sleep(2)
    publisher.stop()
    mq_local.stop()

    print(f"\n  补偿消费到的消息数: {len(consumed)}")
    print(f"  Consumer stats: {consumer.stats}")

    published = db.fetchone("SELECT COUNT(*) c FROM outbox WHERE status = 'PUBLISHED'")["c"]
    print(f"  发件箱 PUBLISHED 消息数: {published}")

    assert len(consumed) == 3, "崩溃恢复后3条消息应该都被补偿投递"
    assert published == 3, "所有消息都应标记为PUBLISHED"
    print("\n  ✅ Demo 3 成功: Publisher重启后自动扫描并补发所有未投递消息")


def demo_4_duplicate_detection():
    print_separator("Demo 4: 消费端幂等 - 重复消息自动识别并跳过")

    cleanup_db()
    db.init_schema()
    mq_local = InMemoryMessageQueue()

    processed_payloads = []

    def handler(event: DomainEvent):
        processed_payloads.append(event.payload["order_no"])
        print(f"  [Consumer] 实际执行业务: order={event.payload['order_no']}")

    consumer = IdempotentConsumer(consumer_id="dedup-consumer")
    wrapped_handler = consumer.wrap(handler)

    event = DomainEvent(
        event_id=str(uuid.uuid4()),
        aggregate_type="Order",
        aggregate_id="ORD-DUP-001",
        event_type="OrderCreated",
        payload={"order_no": "ORD-DUP-001", "amount": 500.0},
    )

    print("\n  >>> 第一次消费同一条消息...")
    wrapped_handler(event)
    print(f"  当前业务执行次数: {len(processed_payloads)}")

    print("\n  >>> 模拟MQ重复投递，消费同一条消息（相同event_id）...")
    wrapped_handler(event)
    wrapped_handler(event)
    wrapped_handler(event)
    print(f"  当前业务执行次数: {len(processed_payloads)}")

    print(f"\n  Consumer stats: {consumer.stats}")

    assert len(processed_payloads) == 1, "业务逻辑只应该执行1次"
    assert consumer.stats["duplicates"] == 3, "应该识别出3条重复消息"
    print("\n  ✅ Demo 4 成功: 重复消息被识别，业务逻辑只执行一次")


def main():
    try:
        demo_1_normal_flow()
        db.close()
    except Exception as e:
        print(f"\n❌ Demo 1 失败: {e}")
        import traceback
        traceback.print_exc()

    try:
        demo_2_atomicity()
        db.close()
    except Exception as e:
        print(f"\n❌ Demo 2 失败: {e}")
        import traceback
        traceback.print_exc()

    try:
        demo_3_crash_recovery()
        db.close()
    except Exception as e:
        print(f"\n❌ Demo 3 失败: {e}")
        import traceback
        traceback.print_exc()

    try:
        demo_4_duplicate_detection()
        db.close()
    except Exception as e:
        print(f"\n❌ Demo 4 失败: {e}")
        import traceback
        traceback.print_exc()

    print("\n" + "=" * 70)
    print("  所有演示完成！")
    print("=" * 70)

    cleanup_db()


if __name__ == "__main__":
    main()
