import json
import uuid
import threading
import time
from dataclasses import dataclass, field, asdict
from typing import Callable, Optional, List, Dict
from queue import Queue, Empty


@dataclass
class DomainEvent:
    event_id: str
    aggregate_type: str
    aggregate_id: str
    event_type: str
    payload: dict
    timestamp: str = field(default_factory=lambda: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_json(cls, s: str) -> "DomainEvent":
        data = json.loads(s)
        return cls(**data)


class MessageQueue:
    def publish(self, topic: str, event: DomainEvent) -> None:
        raise NotImplementedError

    def subscribe(self, topic: str, handler: Callable[[DomainEvent], None]) -> None:
        raise NotImplementedError


class InMemoryMessageQueue(MessageQueue):
    def __init__(self, simulate_network_failure_rate: float = 0.0):
        self._topics: Dict[str, Queue] = {}
        self._subscribers: Dict[str, List[Callable]] = {}
        self._lock = threading.RLock()
        self._running = False
        self._dispatch_thread: Optional[threading.Thread] = None
        self.simulate_network_failure_rate = simulate_network_failure_rate
        self.published_events: List[DomainEvent] = []

    def _ensure_topic(self, topic: str) -> Queue:
        with self._lock:
            if topic not in self._topics:
                self._topics[topic] = Queue()
                self._subscribers[topic] = []
            return self._topics[topic]

    def publish(self, topic: str, event: DomainEvent) -> None:
        import random
        if random.random() < self.simulate_network_failure_rate:
            raise ConnectionError("Simulated network failure while publishing to MQ")
        q = self._ensure_topic(topic)
        self.published_events.append(event)
        q.put(event)

    def subscribe(self, topic: str, handler: Callable[[DomainEvent], None]) -> None:
        with self._lock:
            self._ensure_topic(topic)
            self._subscribers[topic].append(handler)
            if not self._running:
                self._running = True
                self._dispatch_thread = threading.Thread(target=self._dispatch_loop, daemon=True)
                self._dispatch_thread.start()

    def _dispatch_loop(self):
        while self._running:
            with self._lock:
                topics = list(self._topics.keys())
            for topic in topics:
                try:
                    with self._lock:
                        q = self._topics[topic]
                        handlers = list(self._subscribers[topic])
                    while True:
                        try:
                            event = q.get_nowait()
                        except Empty:
                            break
                        for h in handlers:
                            try:
                                h(event)
                            except Exception as e:
                                print(f"[MQ] Handler error for event {event.event_id}: {e}")
                except Exception:
                    pass
            time.sleep(0.05)

    def stop(self):
        self._running = False
        if self._dispatch_thread:
            self._dispatch_thread.join(timeout=2)


mq = InMemoryMessageQueue()
