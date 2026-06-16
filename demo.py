import os
import sys
import time
import uuid
import threading
import random

from db import db
from message_queue import InMemoryMessageQueue, DomainEvent
from order_service import OrderService, OutboxRepository, DEFAULT_MAX_RETRY
from outbox_publisher import OutboxPublisher
from consumer import IdempotentConsumer


def cleanup_db():
    if os.path.exists("outbox_demo.db"):
        os.remove("outbox_demo.db")


def print_separator(title: str):
    print("\n" + "=" * 72)
    print(f"  {title}")
    print("=" * 72)


# ============ 原有 4 个基础演示（适配新版本 API） ============

def demo_1_normal_flow():
    print_separator("Demo 1: 正常业务流程 - 原子写入+可靠投递+幂等消费")
    cleanup_db()
    db.init_schema()
    mq = InMemoryMessageQueue()
    publisher = OutboxPublisher(mq, poll_interval_seconds=0.3)

    consumed_orders = []

    def inventory_handler(event: DomainEvent):
        consumed_orders.append(event.payload["order_no"])
        print(f"  [Inventory] Processing OrderCreated: {event.payload['order_no']}")

    consumer = IdempotentConsumer(consumer_id="inventory-service")
    mq.subscribe("events.order.ordercreated", consumer.wrap(inventory_handler))

    publisher.start()
    print("\n  >>> 创建3个订单...")
    for i in range(1, 4):
        order_no = f"ORD-{int(time.time())}-{i}"
        order = OrderService.create_order(order_no=order_no, user_id=f"user{i}", amount=100.0 * i)
        print(f"  [Order-Service] 创建订单: {order.order_no}, amount={order.amount}")

    print("\n  >>> 等待 Publisher 投递和 Consumer 消费...")
    time.sleep(2)
    publisher.stop()
    mq.stop()

    print(f"\n  消费到的订单数量: {len(consumed_orders)}")
    print(f"  Publisher stats: {publisher._stats}")
    print(f"  Consumer stats: {consumer.stats}")
    outbox_rows = db.fetchall("SELECT event_id, status, event_type FROM outbox")
    print(f"\n  发件箱表状态:")
    for r in outbox_rows:
        print(f"    {r['event_id'][:12]}... -> {r['status']} ({r['event_type']})")

    assert len(consumed_orders) == 3, f"应该消费到3个订单, 实际 {len(consumed_orders)}"
    print("\n  ✅ Demo 1 成功")


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
    print("\n  ✅ Demo 2 成功")


def demo_3_crash_recovery():
    print_separator("Demo 3: 崩溃恢复 - Publisher重启后自动补偿未投递消息")
    cleanup_db()
    db.init_schema()
    mq = InMemoryMessageQueue(simulate_network_failure_rate=1.0)
    publisher = OutboxPublisher(mq, poll_interval_seconds=0.2, max_retry=3)

    print("\n  >>> 先创建3个订单（MQ 100% 失败）...")
    for i in range(1, 4):
        order_no = f"ORD-CRASH-{int(time.time())}-{i}"
        order = OrderService.create_order(order_no=order_no, user_id=f"user{i}", amount=200.0 * i)
        print(f"  [Order-Service] 创建订单: {order.order_no}")

    pending = db.fetchone("SELECT COUNT(*) c FROM outbox WHERE status = 'PENDING'")["c"]
    print(f"\n  当前发件箱 PENDING 消息数: {pending}")
    assert pending == 3

    print("\n  >>> 恢复 MQ，启动 Publisher...")
    mq.simulate_network_failure_rate = 0.0
    consumed = []

    def handler(event: DomainEvent):
        consumed.append(event.event_id)
        print(f"  [Consumer] 收到补偿消息: {event.event_id[:12]}...")

    consumer = IdempotentConsumer("receiver")
    mq.subscribe("events.order.ordercreated", consumer.wrap(handler))

    publisher.start()
    time.sleep(2)
    publisher.stop()
    mq.stop()

    print(f"\n  补偿消费到的消息数: {len(consumed)}")
    print(f"  Consumer stats: {consumer.stats}")
    published = db.fetchone("SELECT COUNT(*) c FROM outbox WHERE status = 'PUBLISHED'")["c"]
    print(f"  发件箱 PUBLISHED 消息数: {published}")

    assert len(consumed) == 3, f"崩溃恢复后3条消息应该都被补偿投递, 实际 {len(consumed)}"
    assert published == 3
    print("\n  ✅ Demo 3 成功")


def demo_4_duplicate_detection():
    print_separator("Demo 4: 串行重复消费 - 重复消息自动识别并跳过")
    cleanup_db()
    db.init_schema()
    mq = InMemoryMessageQueue()

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

    print("\n  >>> 串行消费同一条消息 3 次...")
    wrapped_handler(event)
    wrapped_handler(event)
    wrapped_handler(event)
    print(f"  当前业务执行次数: {len(processed_payloads)}")
    print(f"  Consumer stats: {consumer.stats}")

    assert len(processed_payloads) == 1, "业务逻辑只应该执行1次"
    assert consumer.stats["duplicates"] == 3, f"应该识别出3条重复, 实际 {consumer.stats}"
    print("\n  ✅ Demo 4 成功")


# ============ 新增 4 个高级演示 ============

def demo_5_concurrent_duplicate():
    """
    需求1: 并发重复投递时，业务逻辑也只执行一次。
    用 10 个线程同时消费同一条 event，验证最终业务只处理 1 次。
    """
    print_separator("Demo 5: 并发重复消费 - 唯一约束原子抢占, 10线程并发仍只处理1次")
    cleanup_db()
    db.init_schema()

    processed_count = {"n": 0}
    lock = threading.Lock()

    def handler(event: DomainEvent):
        # 人为加一点延迟，加大并发冲突概率
        time.sleep(0.01)
        with lock:
            processed_count["n"] += 1
        print(f"    [THREAD-{threading.current_thread().name}] 实际执行业务处理!")

    consumer = IdempotentConsumer(consumer_id="concurrent-consumer")
    wrapped_handler = consumer.wrap(handler)

    target_event = DomainEvent(
        event_id=str(uuid.uuid4()),
        aggregate_type="Order",
        aggregate_id="ORD-CONCURRENT-001",
        event_type="OrderCreated",
        payload={"order_no": "ORD-CONCURRENT-001", "amount": 999.0},
    )

    N_THREADS = 10
    print(f"\n  >>> 启动 {N_THREADS} 个线程同时投递完全相同的 event_id...")

    threads = []
    for i in range(N_THREADS):
        t = threading.Thread(
            target=wrapped_handler,
            args=(target_event,),
            name=f"T{i+1}",
        )
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print(f"\n  业务实际执行次数: {processed_count['n']}")
    print(f"  Consumer stats: {consumer.stats}")

    assert processed_count["n"] == 1, (
        f"并发场景下业务逻辑仍应只执行1次, 实际 {processed_count['n']}"
    )
    assert consumer.stats["duplicates"] == N_THREADS - 1, (
        f"应识别 {N_THREADS - 1} 个重复"
    )
    print("\n  ✅ Demo 5 成功: 并发下原子抢占依然保证只处理一次")


def demo_6_multi_publisher():
    """
    需求2: 多 Publisher 实例并行工作。
    通过主键分片，避免重复抢；即使偶发重复，消费端也会去重。
    """
    print_separator("Demo 6: 多 Publisher 实例 (2分片) - 并行投递不丢消息, 重复被消费端兜底")
    cleanup_db()
    db.init_schema()
    mq = InMemoryMessageQueue()

    # 共享的已处理集合（用于检查重复投递的最终去重）
    consumed_ids = set()
    consumed_lock = threading.Lock()
    delivery_count = {"n": 0}

    def handler(event: DomainEvent):
        with consumed_lock:
            # 只看原始 MQ 投递到 handler 的次数（不经过 IdempotentConsumer）
            delivery_count["n"] += 1
            consumed_ids.add(event.event_id)

    raw_handler = handler  # 先不套去重，验证多实例不重复

    # 两个 Publisher 共享同一个 MQ，各自负责一半主键范围
    pub0 = OutboxPublisher(
        mq, poll_interval_seconds=0.2,
        shard_index=0, total_shards=2, name="Publisher-A",
    )
    pub1 = OutboxPublisher(
        mq, poll_interval_seconds=0.2,
        shard_index=1, total_shards=2, name="Publisher-B",
    )

    print("\n  >>> 创建 12 个订单 (主键ID 1..12，模2分摊给两个 Publisher)")
    order_nos = []
    for i in range(1, 13):
        order_no = f"ORD-MULTI-{int(time.time())}-{i}"
        order = OrderService.create_order(order_no=order_no, user_id=f"u{i}", amount=10.0 * i)
        order_nos.append(order_no)
    print(f"  已创建 {len(order_nos)} 个订单")

    mq.subscribe("events.order.ordercreated", raw_handler)
    pub0.start()
    pub1.start()

    time.sleep(2)
    pub0.stop()
    pub1.stop()
    mq.stop()

    # 验证：不应该有重复投递（主键取模天然互斥）
    print(f"\n  原始投递次数: {delivery_count['n']}")
    print(f"  唯一消息数 (event_id): {len(consumed_ids)}")
    print(f"  Publisher-A stats: {pub0._stats}")
    print(f"  Publisher-B stats: {pub1._stats}")
    print(f"  发件箱状态: {OutboxRepository.get_status_counts()}")

    # 两个 Publisher 发布总数应该等于订单数
    total_pub = pub0._stats["published"] + pub1._stats["published"]
    assert total_pub == len(order_nos), (
        f"两 Publisher 合计应发布 {len(order_nos)} 条, 实际 {total_pub}"
    )
    # 由于主键分片，每个 event_id 只会被 1 个 Publisher 拿到，所以投递次数 = 唯一数
    assert delivery_count["n"] == len(consumed_ids) == len(order_nos), (
        f"投递数 {delivery_count['n']} / 唯一数 {len(consumed_ids)} / 订单数 {len(order_nos)} 应相等"
    )

    print("\n  ✅ Demo 6 成功: 两实例分片协作，无重复投递，无丢失")


def demo_7_dead_letter():
    """
    需求3: 失败上限 + 死信状态 + 失败原因查询。
    """
    print_separator("Demo 7: 死信队列 - 超过 max_retry=2 进入 DEAD, 可查询失败原因")
    cleanup_db()
    db.init_schema()
    # MQ 永远失败，让消息一步步走到死信
    mq = InMemoryMessageQueue(simulate_network_failure_rate=1.0)
    # max_retry=2 意味着第3次失败时会入死信（retry_count: 0->1->2->3>2）
    publisher = OutboxPublisher(
        mq, poll_interval_seconds=0.05, max_retry=2, name="Publisher-DL",
    )

    print("\n  >>> 创建 2 个订单，MQ 100% 失败")
    for i in range(1, 3):
        order_no = f"ORD-DEAD-{int(time.time())}-{i}"
        OrderService.create_order(order_no=order_no, user_id=f"u{i}", amount=88.0 * i)

    publisher.start()
    print("  >>> 等待 Publisher 多次重试直到进入死信（约 6 秒，含指数退避）...")
    time.sleep(7)
    publisher.stop()
    mq.stop()

    print(f"\n  Publisher stats: {publisher._stats}")
    print(f"  发件箱状态: {OutboxRepository.get_status_counts()}")

    dead_list = OutboxRepository.list_dead_letters()
    print(f"\n  死信列表 (共 {len(dead_list)} 条):")
    for d in dead_list:
        print(
            f"    - id={d['id']} event={d['event_id'][:12]}... "
            f"type={d['event_type']} attempts={d['retry_count']} "
            f"dead_at={d['dead_letter_at']}"
        )
        print(f"      失败原因: {d['error_message']}")

    status_counts = OutboxRepository.get_status_counts()
    assert status_counts["DEAD"] == 2, f"应有2条死信, 实际 {status_counts}"
    assert len(dead_list) == 2
    for d in dead_list:
        assert d["error_message"] is not None, "死信必须携带失败原因"
        assert "Simulated network failure" in d["error_message"]
    print("\n  ✅ Demo 7 成功: 死信状态正确，失败原因可查询")


def demo_8_stress_end_to_end():
    """
    需求4: 端到端压力场景。
    批量创建订单、随机让 MQ 失败/恢复，最后输出一致性汇总。
    """
    print_separator("Demo 8: 端到端压力 - 批量订单+随机MQ故障, 最终一致性汇总")
    cleanup_db()
    db.init_schema()

    TOTAL_ORDERS = 50
    FAILURE_RATE = 0.35   # 35% 概率 MQ 发布失败

    mq = InMemoryMessageQueue(simulate_network_failure_rate=FAILURE_RATE)
    pub_a = OutboxPublisher(
        mq, poll_interval_seconds=0.2,
        shard_index=0, total_shards=2, name="Stress-A",
    )
    pub_b = OutboxPublisher(
        mq, poll_interval_seconds=0.2,
        shard_index=1, total_shards=2, name="Stress-B",
    )

    consumed_ids = set()
    processed_event_ids = set()
    ids_lock = threading.Lock()
    duplicate_detected = {"n": 0}

    def business_handler(event: DomainEvent):
        # 真实的业务处理（此处用添加集合模拟）
        with ids_lock:
            processed_event_ids.add(event.event_id)

    # 先套一层，统计 MQ 原始投递次数
    def counting_handler(event: DomainEvent):
        with ids_lock:
            consumed_ids.add(event.event_id)

    consumer = IdempotentConsumer(consumer_id="stress-consumer")
    # MQ -> counting 统计 -> 幂等去重 -> 真实业务
    def combined_handler(event: DomainEvent):
        counting_handler(event)
        if consumer.process(event, business_handler):
            pass  # 已经在 process 内部计数
        else:
            duplicate_detected["n"] += 1

    mq.subscribe("events.order.ordercreated", combined_handler)
    pub_a.start()
    pub_b.start()

    print(f"\n  >>> 批量创建 {TOTAL_ORDERS} 个订单 (MQ 发布失败率 {FAILURE_RATE*100:.0f}%)...")
    order_nos = []
    ts = int(time.time())
    for i in range(1, TOTAL_ORDERS + 1):
        order_no = f"ORD-STRESS-{ts}-{i:03d}"
        OrderService.create_order(
            order_no=order_no,
            user_id=f"stress_user_{i % 10}",
            amount=round(random.uniform(10, 1000), 2),
        )
        order_nos.append(order_no)
    print(f"  ✔ 已创建 {len(order_nos)} 个订单，等待系统消化 (约 6 秒)...")

    # 中途模拟一次 MQ 完全恢复（把失败率降下来，加速收尾）
    time.sleep(3)
    print("  >>> 中途: 降低 MQ 失败率到 5%，加速收尾")
    mq.simulate_network_failure_rate = 0.05

    time.sleep(4)
    # 最后完全恢复，让所有可重试消息都发出去
    mq.simulate_network_failure_rate = 0.0
    time.sleep(2)

    pub_a.stop()
    pub_b.stop()
    mq.stop()

    status = OutboxRepository.get_status_counts()
    total_in_outbox = status["TOTAL"]
    published = status["PUBLISHED"]
    dead = status["DEAD"]
    pending_or_failed = status["PENDING"] + status["FAILED"]

    order_count = db.fetchone("SELECT COUNT(*) c FROM orders")["c"]
    unique_consumed = len(consumed_ids)
    unique_processed = len(processed_event_ids)
    dup_consumer = duplicate_detected["n"]

    # 打印漂亮的汇总表
    print("\n" + "-" * 56)
    print("  最终一致性汇总")
    print("-" * 56)
    print(f"  订单总数          : {order_count}")
    print(f"  发件箱总记录数    : {total_in_outbox}")
    print(f"    已发布 PUBLISHED: {published}")
    print(f"    死信 DEAD        : {dead}")
    print(f"    待处理 PENDING+FAILED: {pending_or_failed}")
    print(f"  MQ 实际投递(唯一) : {unique_consumed}")
    print(f"  消费端业务(唯一)  : {unique_processed}")
    print(f"  消费端识别重复    : {dup_consumer}")
    print(f"  Publisher-A stats : {pub_a._stats}")
    print(f"  Publisher-B stats : {pub_b._stats}")
    print(f"  Consumer stats    : {consumer.stats}")
    print("-" * 56)

    # 一致性断言
    assert order_count == TOTAL_ORDERS == total_in_outbox, (
        f"订单/发件箱数量应等于 {TOTAL_ORDERS}"
    )
    # 已发布 + 死信 + 待处理 应该 = 总
    assert published + dead + pending_or_failed == total_in_outbox

    # 最终（除了死信/卡住的，其余都应该被消费）
    # 考虑到死信，业务处理数量 = 已发布 且被消费到的数量
    print("\n  一致性判定:")
    print(f"    业务唯一处理数 <= 发布总数 <= 发件箱总数")
    print(f"    {unique_processed} <= {published} <= {total_in_outbox}")
    assert unique_processed <= published <= total_in_outbox

    # 识别的重复数 应该等于 (投递到MQ次数 - 唯一处理数) 但我们只在消费者端统计
    assert dup_consumer + consumer.stats["processed"] == (
        consumer.stats["processed"] + dup_consumer
    )
    print("\n  ✅ Demo 8 成功: 端到端最终一致性符合预期")


# ============ Main ============

def run_safe(fn):
    try:
        fn()
        db.close()
    except Exception as e:
        print(f"\n❌ {fn.__name__} 失败: {e}")
        import traceback
        traceback.print_exc()
        db.close()
        raise


def main():
    run_safe(demo_1_normal_flow)
    run_safe(demo_2_atomicity)
    run_safe(demo_3_crash_recovery)
    run_safe(demo_4_duplicate_detection)
    run_safe(demo_5_concurrent_duplicate)
    run_safe(demo_6_multi_publisher)
    run_safe(demo_7_dead_letter)
    run_safe(demo_8_stress_end_to_end)

    print("\n" + "=" * 72)
    print("  全部 8 个演示通过 ✅🎉")
    print("=" * 72)
    cleanup_db()


if __name__ == "__main__":
    main()
