"""Leader lease CAS、epoch fencing 与角色转换测试。"""

from __future__ import annotations

import asyncio
import hashlib
import threading
from datetime import UTC, datetime, timedelta

import pytest

from pystream.control import CoordinatorRole
from pystream.control.leader import (
    DEFAULT_LEASE_TTL,
    DEFAULT_RENEW_INTERVAL,
    DEFAULT_STANDBY_POLL_INTERVAL,
    LeaderCoordinator,
    LeaderLeaseLost,
    S3LeaderLeaseRepository,
)
from pystream.storage import (
    ObjectConflict,
    ObjectNotFound,
    ObjectStoreError,
    ObjectValue,
)


class MemoryObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, ObjectValue] = {}
        self.generation = 0
        self.lose_next_response = False

    def get(self, key: str) -> ObjectValue:
        try:
            return self.objects[key]
        except KeyError as exc:
            raise ObjectNotFound(key) from exc

    def put_if_absent(self, key: str, content: bytes) -> str:
        if key in self.objects:
            raise ObjectConflict(key)
        return self._put(key, content)

    def put_if_match(self, key: str, content: bytes, etag: str) -> str:
        current = self.objects.get(key)
        if current is None or current.etag != etag:
            raise ObjectConflict(key)
        return self._put(key, content)

    def list_keys(self, prefix: str) -> tuple[str, ...]:
        return tuple(sorted(key for key in self.objects if key.startswith(prefix)))

    def _put(self, key: str, content: bytes) -> str:
        self.generation += 1
        etag = f"etag-{self.generation}-{hashlib.sha256(content).hexdigest()}"
        self.objects[key] = ObjectValue(content, etag)
        if self.lose_next_response:
            self.lose_next_response = False
            raise ObjectStoreError("simulated response loss")
        return etag


def test_leader_lease_acquire_renew_release_and_epoch_takeover() -> None:
    store = MemoryObjectStore()
    repository = S3LeaderLeaseRepository(store)
    started = datetime(2026, 8, 4, 12, 0, tzinfo=UTC)

    first = repository.acquire("jobmanager-1", now=started, ttl=DEFAULT_LEASE_TTL)

    assert first is not None
    assert first.lease.coordinator_epoch == 1
    assert first.lease.expires_at == started + timedelta(seconds=10)
    assert (
        repository.acquire(
            "jobmanager-2",
            now=started + timedelta(seconds=1),
            ttl=DEFAULT_LEASE_TTL,
        )
        is None
    )

    renewed = repository.renew(
        first,
        now=started + timedelta(seconds=3),
        ttl=DEFAULT_LEASE_TTL,
    )
    assert renewed.lease.coordinator_epoch == 1
    assert renewed.lease.expires_at == started + timedelta(seconds=13)

    released = repository.release(
        renewed,
        now=started + timedelta(seconds=4),
    )
    second = repository.acquire(
        "jobmanager-2",
        now=started + timedelta(seconds=4),
        ttl=DEFAULT_LEASE_TTL,
    )

    assert released.lease.expires_at == started + timedelta(seconds=4)
    assert second is not None
    assert second.lease.holder_id == "jobmanager-2"
    assert second.lease.coordinator_epoch == 2


def test_leader_lease_two_contenders_have_exactly_one_winner() -> None:
    store = MemoryObjectStore()
    first_repository = S3LeaderLeaseRepository(store)
    second_repository = S3LeaderLeaseRepository(store)
    now = datetime(2026, 8, 4, 12, 0, tzinfo=UTC)

    first = first_repository.acquire("jobmanager-1", now=now, ttl=DEFAULT_LEASE_TTL)
    second = second_repository.acquire("jobmanager-2", now=now, ttl=DEFAULT_LEASE_TTL)

    assert first is not None
    assert second is None
    assert first_repository.read() == first


def test_leader_lease_reconciles_success_after_response_loss() -> None:
    store = MemoryObjectStore()
    repository = S3LeaderLeaseRepository(store)
    now = datetime(2026, 8, 4, 12, 0, tzinfo=UTC)
    store.lose_next_response = True

    acquired = repository.acquire("jobmanager-1", now=now, ttl=DEFAULT_LEASE_TTL)

    assert acquired is not None
    assert repository.read() == acquired


def test_leader_lease_stale_etag_cannot_renew() -> None:
    repository = S3LeaderLeaseRepository(MemoryObjectStore())
    now = datetime(2026, 8, 4, 12, 0, tzinfo=UTC)
    acquired = repository.acquire("jobmanager-1", now=now, ttl=DEFAULT_LEASE_TTL)
    assert acquired is not None
    renewed = repository.renew(
        acquired,
        now=now + timedelta(seconds=3),
        ttl=DEFAULT_LEASE_TTL,
    )

    with pytest.raises(LeaderLeaseLost, match="CAS"):
        repository.renew(
            acquired,
            now=now + timedelta(seconds=4),
            ttl=DEFAULT_LEASE_TTL,
        )

    assert repository.read() == renewed


class FailingRenewRepository:
    def __init__(self, repository: S3LeaderLeaseRepository) -> None:
        self.repository = repository
        self.fail_renew = False

    def read(self):
        return self.repository.read()

    def acquire(self, holder_id: str, *, now: datetime, ttl: timedelta):
        return self.repository.acquire(holder_id, now=now, ttl=ttl)

    def renew(self, current, *, now: datetime, ttl: timedelta):
        if self.fail_renew:
            raise ObjectStoreError("simulated lease outage")
        return self.repository.renew(current, now=now, ttl=ttl)

    def release(self, current, *, now: datetime):
        return self.repository.release(current, now=now)


class BlockingRenewRepository(FailingRenewRepository):
    def __init__(self, repository: S3LeaderLeaseRepository) -> None:
        super().__init__(repository)
        self.started = threading.Event()
        self.unblock = threading.Event()
        self.block_renew = False

    def renew(self, current, *, now: datetime, ttl: timedelta):
        if not self.block_renew:
            return self.repository.renew(current, now=now, ttl=ttl)
        self.started.set()
        self.unblock.wait(timeout=1)
        return self.repository.renew(current, now=now, ttl=ttl)


@pytest.mark.asyncio
async def test_leader_coordinator_enters_protective_on_renew_failure() -> None:
    now = [datetime(2026, 8, 4, 12, 0, tzinfo=UTC)]
    repository = FailingRenewRepository(S3LeaderLeaseRepository(MemoryObjectStore()))
    events: list[tuple[str, object]] = []

    async def acquired(epoch: int) -> None:
        events.append(("active", epoch))

    async def lost(reason: str) -> None:
        events.append(("lost", reason))

    async def standby() -> None:
        events.append(("standby", None))

    coordinator = LeaderCoordinator(
        "jobmanager-1",
        repository,
        on_acquired=acquired,
        on_lost=lost,
        on_standby=standby,
        clock=lambda: now[0],
    )

    assert await coordinator.poll_once() is CoordinatorRole.ACTIVE
    assert events == [("active", 1)]

    repository.fail_renew = True
    now[0] += DEFAULT_RENEW_INTERVAL
    assert await coordinator.poll_once() is CoordinatorRole.PROTECTIVE
    assert events[-1][0] == "lost"

    now[0] += DEFAULT_LEASE_TTL
    assert await coordinator.poll_once() is CoordinatorRole.STANDBY
    assert events[-1] == ("standby", None)


@pytest.mark.asyncio
async def test_leader_coordinator_renew阻塞不越过lease期限() -> None:
    repository = BlockingRenewRepository(S3LeaderLeaseRepository(MemoryObjectStore()))
    events: list[str] = []

    async def acquired(epoch: int) -> None:
        events.append(f"active:{epoch}")

    async def lost(reason: str) -> None:
        events.append(f"lost:{reason}")

    async def standby() -> None:
        events.append("standby")

    coordinator = LeaderCoordinator(
        "jobmanager-1",
        repository,
        on_acquired=acquired,
        on_lost=lost,
        on_standby=standby,
        ttl=timedelta(milliseconds=80),
        renew_interval=timedelta(milliseconds=10),
        poll_interval=timedelta(milliseconds=10),
    )
    assert await coordinator.poll_once() is CoordinatorRole.ACTIVE
    repository.block_renew = True
    await asyncio.sleep(0.02)
    started_at = asyncio.get_running_loop().time()
    try:
        assert await coordinator.poll_once() is CoordinatorRole.PROTECTIVE
    finally:
        repository.unblock.set()
    elapsed = asyncio.get_running_loop().time() - started_at

    assert repository.started.is_set()
    assert elapsed < 0.2
    assert events[0] == "active:1"
    assert events[-1].startswith("lost:Leader lease 续租失败: TimeoutError")


@pytest.mark.asyncio
async def test_leader_coordinator_激活阻塞不开放过期lease() -> None:
    repository = S3LeaderLeaseRepository(MemoryObjectStore())
    never = asyncio.Event()
    events: list[str] = []

    async def acquired(epoch: int) -> None:
        events.append(f"activating:{epoch}")
        await never.wait()

    async def lost(reason: str) -> None:
        events.append(f"lost:{reason}")

    async def standby() -> None:
        events.append("standby")

    coordinator = LeaderCoordinator(
        "jobmanager-1",
        repository,
        on_acquired=acquired,
        on_lost=lost,
        on_standby=standby,
        ttl=timedelta(milliseconds=80),
        renew_interval=timedelta(milliseconds=10),
        poll_interval=timedelta(milliseconds=10),
    )

    with pytest.raises(TimeoutError):
        await coordinator.poll_once()

    assert coordinator.role is CoordinatorRole.PROTECTIVE
    assert events[0] == "activating:1"
    assert events[-1].startswith("lost:Leader 激活失败: TimeoutError")


def test_leader_default_intervals_match_contract() -> None:
    assert timedelta(seconds=10) == DEFAULT_LEASE_TTL
    assert timedelta(seconds=3) == DEFAULT_RENEW_INTERVAL
    assert timedelta(seconds=1) == DEFAULT_STANDBY_POLL_INTERVAL
