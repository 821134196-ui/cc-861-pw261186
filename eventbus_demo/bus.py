"""A small partitioned, append-only local event bus.

Checkpoints represent the *last successfully processed offset*.  A partition
starts at ``-1``, so the first record has offset ``0``.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from threading import RLock
from typing import Any


@dataclass(frozen=True)
class Event:
    topic: str
    partition: int
    offset: int
    event_id: str
    key: str
    order_id: str
    amount: int
    produced_at: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class EventBus:
    def __init__(self, partition_count: int = 3, topic: str = "orders") -> None:
        self.topic = topic
        self.partition_count = partition_count
        self.partitions: list[list[Event]] = [[] for _ in range(partition_count)]
        self.next_offsets = [0] * partition_count
        # Last processed offset, not next offset. -1 means nothing processed.
        self.checkpoints = [-1] * partition_count
        self.checkpoint_history: list[dict[str, Any]] = []
        self.lock = RLock()
        self._order_sequence = 2000

    def append(
        self,
        partition: int,
        *,
        produced_at: int = 0,
        order_id: str | None = None,
        amount: int | None = None,
        key: str | None = None,
    ) -> Event:
        if not 0 <= partition < self.partition_count:
            raise ValueError(f"partition must be in [0, {self.partition_count - 1}]")

        with self.lock:
            offset = self.next_offsets[partition]
            self.next_offsets[partition] += 1
            if order_id is None:
                order_id = f"ORD-{self._order_sequence}"
                self._order_sequence += 1
            if amount is None:
                amount = 4900 + partition * 700 + offset * 137
            event = Event(
                topic=self.topic,
                partition=partition,
                offset=offset,
                event_id=f"evt-{partition}-{offset}",
                key=key or order_id,
                order_id=order_id,
                amount=amount,
                produced_at=produced_at,
            )
            self.partitions[partition].append(event)
            return event

    def seed_demo_data(self, clock: int = 0) -> None:
        """Create deterministic records and some prior checkpoint progress."""
        with self.lock:
            for partition in range(self.partition_count):
                count = 5 if partition == 0 else 3
                for offset in range(count):
                    self.append(
                        partition,
                        produced_at=clock + offset,
                        order_id=f"ORD-{2100 + partition * 100 + offset}",
                        amount=5900 + partition * 1100 + offset * 250,
                    )
            # consumer-1 has already processed offsets 0 and 1 on partition 0.
            self.checkpoints[0] = 1
            self.checkpoint_history.append(
                {
                    "at": clock,
                    "partition": 0,
                    "requestedOffset": 1,
                    "previousOffset": -1,
                    "currentOffset": 1,
                    "member": "bootstrap",
                    "generation": 0,
                    "source": "seed",
                    "accepted": True,
                    "changed": True,
                    "reason": "seed_checkpoint",
                }
            )

    def latest_offset(self, partition: int) -> int:
        with self.lock:
            return self.next_offsets[partition] - 1

    def ensure_offset_exists(self, partition: int, offset: int, clock: int = 0) -> None:
        """Append records until ``offset`` exists. Used by the guided failover."""
        with self.lock:
            while self.next_offsets[partition] <= offset:
                self.append(partition, produced_at=clock)

    def pending_events(self, partition: int, limit: int = 20) -> list[Event]:
        with self.lock:
            checkpoint = self.checkpoints[partition]
            return [
                event
                for event in self.partitions[partition]
                if event.offset > checkpoint
            ][:limit]

    def first_pending(self, partition: int) -> Event | None:
        events = self.pending_events(partition, limit=1)
        return events[0] if events else None

    def commit_checkpoint(
        self,
        partition: int,
        offset: int,
        *,
        member: str,
        generation: int,
        source: str,
        clock: int,
    ) -> dict[str, Any]:
        """Conditional checkpoint write.

        The storage layer enforces monotonicity even if a stale/zombie caller
        bypasses group coordination. Equal offsets are treated as an
        idempotent recommit, not as an error.
        """
        with self.lock:
            previous = self.checkpoints[partition]
            latest = self.latest_offset(partition)
            result: dict[str, Any] = {
                "at": clock,
                "partition": partition,
                "requestedOffset": offset,
                "previousOffset": previous,
                "currentOffset": previous,
                "member": member,
                "generation": generation,
                "source": source,
                "accepted": False,
                "changed": False,
                "stage": "storage",
            }

            if offset < -1:
                result["reason"] = "invalid_offset"
                result["message"] = "检查点不能小于 -1。"
            elif latest >= 0 and offset > latest:
                result["reason"] = "offset_beyond_log_end"
                result["message"] = f"offset {offset} 超过日志末端 {latest}。"
            elif offset < previous:
                result["reason"] = "checkpoint_regression_rejected"
                result["message"] = (
                    f"拒绝把 p{partition} 检查点从 {previous} 倒退到 {offset}。"
                )
            elif offset == previous:
                result.update(
                    accepted=True,
                    changed=False,
                    currentOffset=previous,
                    reason="idempotent_equal_commit",
                    message="相同检查点重复提交，保持不变。",
                )
            else:
                self.checkpoints[partition] = offset
                result.update(
                    accepted=True,
                    changed=True,
                    currentOffset=offset,
                    reason="advanced",
                    message=f"p{partition} 检查点推进到 {offset}。",
                )

            self.checkpoint_history.append(result)
            if len(self.checkpoint_history) > 100:
                self.checkpoint_history = self.checkpoint_history[-100:]
            return dict(result)

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "topic": self.topic,
                "partitionCount": self.partition_count,
                "checkpoints": list(self.checkpoints),
                "latestOffsets": [o - 1 for o in self.next_offsets],
                "pendingCounts": [
                    max(0, self.next_offsets[p] - 1 - self.checkpoints[p])
                    if self.next_offsets[p]
                    else 0
                    for p in range(self.partition_count)
                ],
                "partitions": [
                    [event.to_dict() for event in events]
                    for events in self.partitions
                ],
                "checkpointHistory": list(reversed(self.checkpoint_history[-30:])),
            }
