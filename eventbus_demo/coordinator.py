"""Consumer-group membership, heartbeats, generation, and commit fencing."""

from __future__ import annotations

from threading import RLock
from typing import Any


class GroupCoordinator:
    def __init__(
        self,
        partition_count: int = 3,
        *,
        heartbeat_timeout_ms: int = 10_000,
    ) -> None:
        self.group_id = "order-processors"
        self.heartbeat_timeout_ms = heartbeat_timeout_ms
        self.generation = 1
        self.lock = RLock()
        self.members: dict[str, dict[str, Any]] = {
            "consumer-1": {
                "id": "consumer-1",
                "name": "consumer-1",
                "heartbeatRunning": True,
                "coordinatorState": "healthy",
                "processState": "live",
                "lastHeartbeat": 0,
                "knownGeneration": 1,
            },
            "consumer-2": {
                "id": "consumer-2",
                "name": "consumer-2",
                "heartbeatRunning": True,
                "coordinatorState": "healthy",
                "processState": "live",
                "lastHeartbeat": 0,
                "knownGeneration": 1,
            },
        }
        # Stable initial assignment: C1 owns p0/p1, C2 owns p2.
        self.owners: dict[int, str] = {0: "consumer-1", 1: "consumer-1", 2: "consumer-2"}
        self.revoking: list[int] = []
        self.events: list[dict[str, Any]] = []

    def tick(self, clock: int, milliseconds: int) -> int:
        """Advance the virtual clock and heartbeat live members."""
        with self.lock:
            new_clock = clock + milliseconds
            for member in self.members.values():
                if member["heartbeatRunning"]:
                    member["lastHeartbeat"] = new_clock
            return new_clock

    def stop_heartbeat(self, member_id: str, clock: int) -> dict[str, Any]:
        with self.lock:
            member = self.members[member_id]
            member["heartbeatRunning"] = False
            member["coordinatorState"] = "silent"
            member["processState"] = "paused"
            member["lastHeartbeat"] = clock
            return dict(member)

    def detect_timeouts(self, clock: int) -> list[str]:
        timed_out: list[str] = []
        with self.lock:
            for member_id, member in self.members.items():
                age = clock - int(member["lastHeartbeat"])
                if (
                    not member["heartbeatRunning"]
                    and age >= self.heartbeat_timeout_ms
                    and member["coordinatorState"] not in {"dead", "fenced"}
                ):
                    member["coordinatorState"] = "dead"
                    timed_out.append(member_id)
                    self.events.append(
                        {
                            "at": clock,
                            "type": "member_timeout",
                            "member": member_id,
                            "heartbeatAge": age,
                        }
                    )
            return timed_out

    def partitions_owned_by(self, member_id: str) -> list[int]:
        with self.lock:
            return sorted(p for p, owner in self.owners.items() if owner == member_id)

    def begin_revoke(self, partitions: list[int]) -> None:
        with self.lock:
            self.revoking = sorted(partitions)

    def bump_generation(self, clock: int) -> int:
        with self.lock:
            self.generation += 1
            self.events.append({"at": clock, "type": "generation_bump", "generation": self.generation})
            return self.generation

    def assign(self, partition: int, member_id: str, generation: int) -> None:
        with self.lock:
            if member_id not in self.members:
                raise KeyError(member_id)
            self.owners[partition] = member_id
            if partition in self.revoking:
                self.revoking.remove(partition)
            member = self.members[member_id]
            member["knownGeneration"] = generation
            if member["coordinatorState"] in {"silent", "dead"}:
                # Assignment to a healthy replacement should already be healthy.
                member["coordinatorState"] = "healthy"

    def authorize_commit(
        self,
        member_id: str,
        partition: int,
        generation: int,
        clock: int,
    ) -> dict[str, Any]:
        with self.lock:
            current_generation = self.generation
            owner = self.owners.get(partition)
            member = self.members.get(member_id)
            base = {
                "at": clock,
                "partition": partition,
                "member": member_id,
                "generation": generation,
                "currentGeneration": current_generation,
                "owner": owner,
                "stage": "coordinator",
            }
            if member is None:
                return {**base, "accepted": False, "reason": "unknown_member"}
            if generation != current_generation:
                return {
                    **base,
                    "accepted": False,
                    "reason": "fenced_stale_generation",
                    "message": (
                        f"{member_id} 持有旧 generation={generation}；"
                        f"当前 fence generation={current_generation}。"
                    ),
                }
            if owner != member_id:
                return {
                    **base,
                    "accepted": False,
                    "reason": "not_current_owner",
                    "message": f"{member_id} 不再拥有 p{partition}，当前所有者是 {owner}。",
                }
            if member["coordinatorState"] in {"dead", "fenced"}:
                return {
                    **base,
                    "accepted": False,
                    "reason": "member_fenced_or_dead",
                    "message": f"{member_id} 已被协调器标记为 {member['coordinatorState']}。",
                }
            return {**base, "accepted": True, "reason": "authorized"}

    def snapshot(self, clock: int) -> dict[str, Any]:
        with self.lock:
            members = []
            for member in self.members.values():
                item = dict(member)
                item["heartbeatAge"] = clock - int(member["lastHeartbeat"])
                item["ownedPartitions"] = sorted(
                    p for p, owner in self.owners.items() if owner == member["id"]
                )
                members.append(item)
            return {
                "groupId": self.group_id,
                "generation": self.generation,
                "heartbeatTimeoutMs": self.heartbeat_timeout_ms,
                "owners": {str(p): owner for p, owner in sorted(self.owners.items())},
                "revoking": list(self.revoking),
                "members": members,
                "events": list(reversed(self.events[-20:])),
            }
