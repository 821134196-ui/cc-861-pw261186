"""Deterministic orchestration for the event-bus/rebalancing demonstration."""

from __future__ import annotations

import itertools
from threading import RLock
from typing import Any

from .bus import EventBus
from .coordinator import GroupCoordinator
from .idempotency import AckLostError, ProcessorClient


class DemoActionError(Exception):
    def __init__(self, message: str, *, status: int = 409, code: str = "invalid_demo_action"):
        super().__init__(message)
        self.message = message
        self.status = status
        self.code = code


REBALANCE_STEPS: list[dict[str, str]] = [
    {
        "id": "detect",
        "title": "检测心跳超时",
        "detail": "协调器推进虚拟时钟；consumer-1 超过 10 秒未心跳，被标记为失联。",
    },
    {
        "id": "fence",
        "title": "递增 Generation / Fence",
        "detail": "消费组 generation 从 1 增加到 2，旧消费者持有的 g1 令牌随后会被拒绝。",
    },
    {
        "id": "assign",
        "title": "转交 p0、p1",
        "detail": "协调器把 consumer-1 负责的分区分配给健康的 consumer-2。",
    },
    {
        "id": "overlap",
        "title": "新旧实例重叠处理",
        "detail": "旧实例其实仍存活并处理同一 offset；新所有者从检查点重投，相同幂等键只产生一次副作用。",
    },
    {
        "id": "advance",
        "title": "新所有者推进检查点",
        "detail": "consumer-2 继续处理下一条并提交，检查点向前移动。",
    },
    {
        "id": "stale_commit",
        "title": "旧实例延迟提交",
        "detail": "旧 generation 的提交先被协调器 fence 拒绝；绕过协调器的倒退写入也被存储层单调检查拒绝。",
    },
    {
        "id": "complete",
        "title": "完成重平衡",
        "detail": "所有分区都有当前 generation 的活跃所有者，检查点没有倒退。",
    },
]


class DemoEngine:
    def __init__(
        self,
        processor_client: ProcessorClient,
        processor: Any | None = None,
        *,
        partition_count: int = 3,
    ) -> None:
        self.processor_client = processor_client
        self.processor = processor
        self.partition_count = partition_count
        self.lock = RLock()
        self._log_sequence = itertools.count(1)
        self._audit_sequence = itertools.count(1)
        self.reset()

    def reset(self) -> dict[str, Any]:
        with self.lock:
            self.clock = 0
            self.bus = EventBus(partition_count=self.partition_count)
            self.coordinator = GroupCoordinator(partition_count=self.partition_count)
            self.deliveries: dict[str, dict[str, Any]] = {}
            self.commit_audit: list[dict[str, Any]] = []
            self.logs: list[dict[str, Any]] = []
            self.rebalance: dict[str, Any] | None = None
            self.bus.seed_demo_data(clock=self.clock)
            if self.processor is not None:
                self.processor.reset()
            self._log(
                "success",
                "system",
                "演示已重置：g1 稳定分配为 consumer-1→p0/p1，consumer-2→p2；p0 检查点=1。",
            )
            return self.snapshot_unlocked()

    def _log(self, level: str, source: str, message: str, **detail: Any) -> dict[str, Any]:
        entry = {
            "id": next(self._log_sequence),
            "at": self.clock,
            "level": level,
            "source": source,
            "message": message,
            "detail": detail,
        }
        self.logs.append(entry)
        self.logs[:] = self.logs[-120:]
        return entry

    def _new_plan_unlocked(self, failed_member: str) -> dict[str, Any]:
        partitions = self.coordinator.partitions_owned_by(failed_member)
        return {
            "failedMember": failed_member,
            "partitions": partitions,
            "armed": True,
            "started": False,
            "finished": False,
            "stepIndex": -1,
            "overlap": False,
            "oldGeneration": self.coordinator.generation,
            "fenceGeneration": None,
            "overlapOffset": None,
            "advanceOffset": None,
            "staleResults": None,
            "steps": [
                {**step, "status": "pending", "result": None}
                for step in REBALANCE_STEPS
            ],
        }

    def stop_member(self, member_id: str) -> dict[str, Any]:
        with self.lock:
            if member_id != "consumer-1":
                raise DemoActionError("引导故障脚本当前只演示停止 consumer-1。")
            member = self.coordinator.members.get(member_id)
            if member is None:
                raise DemoActionError(f"未知消费者 {member_id}", status=404)
            if self.rebalance and self.rebalance.get("finished"):
                raise DemoActionError("本轮重平衡已完成，请先重置演示。")

            self.coordinator.stop_heartbeat(member_id, self.clock)
            if self.rebalance is None:
                self.rebalance = self._new_plan_unlocked(member_id)
            self._log(
                "critical",
                "consumer-1",
                "consumer-1 停止回应：心跳循环暂停，但它的本地处理线程仍可能在恢复后提交。",
            )
            return {"ok": True, "member": self.coordinator.snapshot(self.clock)["members"][0]}

    def produce_event(self, partition: int | None = None) -> dict[str, Any]:
        with self.lock:
            if partition is None:
                partition = 0
            if not 0 <= partition < self.partition_count:
                raise DemoActionError("分区编号无效。")
            event = self.bus.append(partition, produced_at=self.clock)
            self._log("info", "event-bus", f"新事件已追加到 p{partition} offset={event.offset}。")
            return {"ok": True, "event": event.to_dict()}

    def process_next(
        self,
        partition: int,
        *,
        lose_ack: bool = False,
        commit: bool = True,
    ) -> dict[str, Any]:
        with self.lock:
            event = self.bus.first_pending(partition)
            if event is None:
                raise DemoActionError(
                    f"p{partition} 没有晚于检查点 {self.bus.checkpoints[partition]} 的待处理事件；请先生产事件。"
                )
            return self.process_offset(
                partition,
                event.offset,
                lose_ack=lose_ack,
                commit=commit,
                source="ui",
            )

    def retry_delivery(self, key: str, *, commit: bool = True) -> dict[str, Any]:
        with self.lock:
            delivery = self.deliveries.get(key)
            if delivery is None:
                raise DemoActionError(f"找不到投递记录 {key}", status=404)
            # A redelivery is performed by the partition's *current* owner and
            # generation. After failover this intentionally changes actor; the
            # stable idempotency key still joins it to the original attempt.
            return self.process_offset(
                int(delivery["partition"]),
                int(delivery["offset"]),
                actor_id=None,
                generation=None,
                lose_ack=False,
                commit=commit,
                source="retry",
            )

    def process_offset(
        self,
        partition: int,
        offset: int,
        *,
        actor_id: str | None = None,
        generation: int | None = None,
        lose_ack: bool = False,
        commit: bool = True,
        source: str = "ui",
    ) -> dict[str, Any]:
        with self.lock:
            event = self.bus.partitions[partition][offset]
            if actor_id is None:
                actor_id = self.coordinator.owners[partition]
            if generation is None:
                generation = self.coordinator.generation

            key = f"idem:{event.topic}:{partition}:{offset}"
            delivery = self.deliveries.get(key)
            if delivery is None:
                delivery = {
                    "id": key,
                    "idempotencyKey": key,
                    "topic": event.topic,
                    "partition": partition,
                    "offset": offset,
                    "eventId": event.event_id,
                    "orderId": event.order_id,
                    "firstActor": actor_id,
                    "lastActor": actor_id,
                    "firstGeneration": generation,
                    "status": "created",
                    "attempts": [],
                    "commit": None,
                }
                self.deliveries[key] = delivery
            delivery["lastActor"] = actor_id
            attempt_number = len(delivery["attempts"]) + 1

            try:
                response = self.processor_client.process(
                    key=key,
                    event_id=event.event_id,
                    order_id=event.order_id,
                    amount=event.amount,
                    partition=partition,
                    offset=offset,
                    virtual_clock=self.clock,
                    lose_ack=lose_ack,
                )
            except AckLostError as exc:
                delivery["attempts"].append(
                    {
                        "attempt": attempt_number,
                        "at": self.clock,
                        "actor": actor_id,
                        "generation": generation,
                        "requestedLoseAck": lose_ack,
                        "outcome": "ack_lost",
                        "deduped": False,
                        "detail": str(exc),
                    }
                )
                delivery["status"] = "ack_lost"
                delivery["commit"] = None
                self._log(
                    "critical",
                    actor_id,
                    f"p{partition}@{offset} 外部处理成功但确认丢失；检查点未提交，等待重投。",
                    idempotencyKey=key,
                )
                return {"ok": False, "ackLost": True, "key": key, "error": str(exc)}

            deduped = bool(response.get("deduped"))
            outcome = "deduplicated" if deduped else "executed_once"
            delivery["attempts"].append(
                {
                    "attempt": attempt_number,
                    "at": self.clock,
                    "actor": actor_id,
                    "generation": generation,
                    "requestedLoseAck": lose_ack,
                    "outcome": outcome,
                    "deduped": deduped,
                    "detail": response.get("result", {}).get("message", ""),
                }
            )
            delivery["status"] = "deduplicated" if deduped else "processed"
            self._log(
                "warning" if deduped else "success",
                "idempotent-service",
                (
                    f"{actor_id} 重投 p{partition}@{offset}：命中幂等键，未重复执行副作用。"
                    if deduped
                    else f"{actor_id} 首次处理 p{partition}@{offset}：外部副作用执行一次。"
                ),
                idempotencyKey=key,
            )

            commit_result = None
            if commit:
                commit_result = self.commit_as_member(
                    partition,
                    offset,
                    actor_id,
                    generation,
                    source=f"{source}:commit",
                )
                delivery["commit"] = commit_result
                delivery["status"] = "committed" if commit_result["accepted"] else "commit_rejected"

            return {
                "ok": True,
                "ackLost": False,
                "key": key,
                "deduped": deduped,
                "response": response,
                "commit": commit_result,
            }

    def commit_as_member(
        self,
        partition: int,
        offset: int,
        member_id: str,
        generation: int,
        *,
        source: str,
    ) -> dict[str, Any]:
        with self.lock:
            audit_id = next(self._audit_sequence)
            authorization = self.coordinator.authorize_commit(
                member_id, partition, generation, self.clock
            )
            if not authorization["accepted"]:
                result = {
                    "auditId": audit_id,
                    **authorization,
                    "requestedOffset": offset,
                    "previousOffset": self.bus.checkpoints[partition],
                    "currentOffset": self.bus.checkpoints[partition],
                    "changed": False,
                    "source": source,
                }
                self.commit_audit.append(result)
                self._log(
                    "critical",
                    "coordinator",
                    f"拒绝 {member_id} 提交 p{partition}@{offset}：{authorization['reason']}；"
                    f"检查点保持 {self.bus.checkpoints[partition]}。",
                )
                return result

            storage_result = self.bus.commit_checkpoint(
                partition,
                offset,
                member=member_id,
                generation=generation,
                source=source,
                clock=self.clock,
            )
            storage_result["auditId"] = audit_id
            self.commit_audit.append(storage_result)
            if storage_result["accepted"] and storage_result["changed"]:
                self._log(
                    "success",
                    "checkpoint-store",
                    f"{member_id} 提交 p{partition} 检查点 → {offset}。",
                )
            elif storage_result["accepted"]:
                self._log(
                    "info",
                    "checkpoint-store",
                    f"{member_id} 重复提交 p{partition}@{offset}，存储层保持不变。",
                )
            else:
                self._log(
                    "critical",
                    "checkpoint-store",
                    storage_result["message"],
                )
            return storage_result

    def rebalance_next(self) -> dict[str, Any]:
        with self.lock:
            if self.rebalance is None:
                healthy = self.coordinator.members["consumer-1"]["heartbeatRunning"]
                if healthy:
                    raise DemoActionError(
                        "consumer-1 仍在正常心跳。请先点击“让 consumer-1 停止回应”。"
                    )
                self.rebalance = self._new_plan_unlocked("consumer-1")

            plan = self.rebalance
            if plan["finished"]:
                raise DemoActionError("重平衡已完成；可重置后再次演示。")

            next_index = int(plan["stepIndex"]) + 1
            if next_index >= len(plan["steps"]):
                raise DemoActionError("没有更多重平衡步骤。")

            step_id = plan["steps"][next_index]["id"]
            handler = getattr(self, f"_rebalance_step_{step_id}")
            result = handler(plan)
            plan["stepIndex"] = next_index
            plan["started"] = True
            plan["steps"][next_index]["status"] = "done"
            plan["steps"][next_index]["result"] = result
            return {"ok": True, "stepId": step_id, "stepIndex": next_index, "result": result}

    def _rebalance_step_detect(self, plan: dict[str, Any]) -> dict[str, Any]:
        self.clock = self.coordinator.tick(self.clock, self.coordinator.heartbeat_timeout_ms + 5_000)
        timed_out = self.coordinator.detect_timeouts(self.clock)
        plan["detectedTimedOut"] = timed_out
        self._log(
            "critical",
            "coordinator",
            f"虚拟时间推进到 {self.clock / 1000:.0f}s；consumer-1 心跳年龄 "
            f"{self.clock}ms，超过 10000ms，判定 dead 并触发 rebalance。",
        )
        return {"timedOut": timed_out, "clock": self.clock}

    def _rebalance_step_fence(self, plan: dict[str, Any]) -> dict[str, Any]:
        old_generation = self.coordinator.generation
        plan["oldGeneration"] = old_generation
        new_generation = self.coordinator.bump_generation(self.clock)
        plan["fenceGeneration"] = new_generation
        self.coordinator.begin_revoke(plan["partitions"])
        self._log(
            "warning",
            "coordinator",
            f"Generation {old_generation}→{new_generation}；p{plan['partitions']} 进入 revoking。"
            "consumer-1 无响应，撤销确认超时后继续，旧实例仍可能持有 g1 令牌。",
        )
        return {"oldGeneration": old_generation, "generation": new_generation}

    def _rebalance_step_assign(self, plan: dict[str, Any]) -> dict[str, Any]:
        generation = int(plan["fenceGeneration"])
        assignments: dict[int, str] = {}
        for partition in plan["partitions"]:
            self.coordinator.assign(partition, "consumer-2", generation)
            assignments[partition] = "consumer-2"
        self._log(
            "success",
            "coordinator",
            f"p{plan['partitions']} 已分配给 consumer-2（generation={generation}）。",
        )
        return {"assignments": {str(k): v for k, v in assignments.items()}}

    def _ensure_event(self, partition: int, offset: int) -> None:
        self.bus.ensure_offset_exists(partition, offset, clock=self.clock)

    def _rebalance_step_overlap(self, plan: dict[str, Any]) -> dict[str, Any]:
        partition = 0
        overlap_offset = self.bus.checkpoints[partition] + 1
        self._ensure_event(partition, overlap_offset + 1)
        plan["overlapOffset"] = overlap_offset
        plan["advanceOffset"] = overlap_offset + 1
        plan["overlap"] = True
        self.coordinator.members["consumer-1"]["processState"] = "zombie"

        old_generation = int(plan["oldGeneration"])
        new_generation = int(plan["fenceGeneration"])

        old_result = self.process_offset(
            partition,
            overlap_offset,
            actor_id="consumer-1",
            generation=old_generation,
            lose_ack=False,
            commit=False,
            source="rebalance:zombie-executes-before-delayed-commit",
        )
        new_result = self.process_offset(
            partition,
            overlap_offset,
            actor_id="consumer-2",
            generation=new_generation,
            lose_ack=False,
            commit=True,
            source="rebalance:new-owner-redelivery",
        )
        self._log(
            "warning",
            "simulator",
            "重叠窗口：consumer-1 已调用外部服务但把提交卡在半路；consumer-2 从旧检查点重投同一 offset。",
        )
        return {"old": old_result, "new": new_result, "offset": overlap_offset}

    def _rebalance_step_advance(self, plan: dict[str, Any]) -> dict[str, Any]:
        partition = 0
        offset = int(plan["advanceOffset"])
        self._ensure_event(partition, offset)
        result = self.process_offset(
            partition,
            offset,
            actor_id="consumer-2",
            generation=int(plan["fenceGeneration"]),
            lose_ack=False,
            commit=True,
            source="rebalance:new-owner-advance",
        )
        return {"offset": offset, "result": result}

    def _rebalance_step_stale_commit(self, plan: dict[str, Any]) -> dict[str, Any]:
        partition = 0
        offset = int(plan["overlapOffset"])
        old_generation = int(plan["oldGeneration"])

        coordinator_rejection = self.commit_as_member(
            partition,
            offset,
            "consumer-1",
            old_generation,
            source="rebalance:delayed-zombie-commit",
        )
        storage_rejection = self.bus.commit_checkpoint(
            partition,
            offset,
            member="consumer-1 [storage-bypass-test]",
            generation=old_generation,
            source="rebalance:direct-storage-write",
            clock=self.clock,
        )
        self.commit_audit.append(storage_rejection)
        plan["staleResults"] = {
            "coordinator": coordinator_rejection,
            "storage": storage_rejection,
        }
        plan["overlap"] = False
        self.coordinator.members["consumer-1"]["processState"] = "fenced"
        self._log(
            "success",
            "invariant",
            f"两层保护均生效：fence 拒绝旧 generation，单调存储拒绝倒退到 {offset}；"
            f"p0 检查点仍为 {self.bus.checkpoints[0]}。",
        )
        return plan["staleResults"]

    def _rebalance_step_complete(self, plan: dict[str, Any]) -> dict[str, Any]:
        plan["finished"] = True
        plan["armed"] = False
        plan["overlap"] = False
        self.coordinator.revoking.clear()
        self._log(
            "success",
            "coordinator",
            "重平衡完成。分区都有当前 generation 所有者；重投由幂等键去重，检查点保持单调。",
        )
        return {"checkpoints": list(self.bus.checkpoints)}

    def snapshot_unlocked(self) -> dict[str, Any]:
        external = self.processor.snapshot() if self.processor is not None else None
        return {
            "ok": True,
            "clock": self.clock,
            "clockSeconds": round(self.clock / 1000, 1),
            "heartbeatTimeoutMs": self.coordinator.heartbeat_timeout_ms,
            "bus": self.bus.snapshot(),
            "coordinator": self.coordinator.snapshot(self.clock),
            "rebalance": self.rebalance,
            "deliveries": list(reversed(list(self.deliveries.values()))),
            "commitAudit": list(reversed(self.commit_audit[-40:])),
            "logs": list(reversed(self.logs[-80:])),
            "external": external,
        }

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return self.snapshot_unlocked()
