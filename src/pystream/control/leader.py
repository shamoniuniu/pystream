"""基于对象存储 ETag CAS 的 JobManager lease 与选举循环。"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from pystream.control.models import CoordinatorRole
from pystream.observability import PyStreamMetrics, log_event
from pystream.storage import (
    ObjectConflict,
    ObjectNotFound,
    ObjectStore,
    ObjectStoreError,
)

LEADER_LEASE_SCHEMA_VERSION = 1
DEFAULT_LEASE_TTL = timedelta(seconds=10)
DEFAULT_RENEW_INTERVAL = timedelta(seconds=3)
DEFAULT_STANDBY_POLL_INTERVAL = timedelta(seconds=1)
_SAFE_HOLDER_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


class LeaderElectionError(RuntimeError):
    """Leader lease 内容、存储或生命周期操作失败。"""


class LeaderLeaseLost(LeaderElectionError):
    """当前 holder 已无法证明仍持有 lease。"""


@dataclass(frozen=True, slots=True)
class LeaderLease:
    """对象存储中的严格、带过期时间的 fencing lease。"""

    holder_id: str
    coordinator_epoch: int
    acquired_at: datetime
    renewed_at: datetime
    expires_at: datetime
    schema_version: int = LEADER_LEASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != LEADER_LEASE_SCHEMA_VERSION:
            raise LeaderElectionError("Leader lease schema 不兼容")
        if _SAFE_HOLDER_ID.fullmatch(self.holder_id) is None:
            raise LeaderElectionError("holder_id 只能包含字母、数字、下划线和连字符")
        if (
            isinstance(self.coordinator_epoch, bool)
            or not isinstance(self.coordinator_epoch, int)
            or self.coordinator_epoch < 1
        ):
            raise LeaderElectionError("coordinator_epoch 必须是正整数")
        acquired_at = _aware_utc(self.acquired_at, "acquired_at")
        renewed_at = _aware_utc(self.renewed_at, "renewed_at")
        expires_at = _aware_utc(self.expires_at, "expires_at")
        if acquired_at > renewed_at or renewed_at > expires_at:
            raise LeaderElectionError("Leader lease 时间顺序必须 acquired <= renewed <= expires")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "holder_id": self.holder_id,
            "coordinator_epoch": self.coordinator_epoch,
            "acquired_at": self.acquired_at.astimezone(UTC).isoformat(),
            "renewed_at": self.renewed_at.astimezone(UTC).isoformat(),
            "expires_at": self.expires_at.astimezone(UTC).isoformat(),
        }

    @classmethod
    def from_dict(cls, document: object) -> LeaderLease:
        expected = {
            "schema_version",
            "holder_id",
            "coordinator_epoch",
            "acquired_at",
            "renewed_at",
            "expires_at",
        }
        if not isinstance(document, dict) or set(document) != expected:
            raise LeaderElectionError("Leader lease 字段集合不匹配")
        try:
            acquired_at = datetime.fromisoformat(
                str(document["acquired_at"]).replace("Z", "+00:00")
            )
            renewed_at = datetime.fromisoformat(str(document["renewed_at"]).replace("Z", "+00:00"))
            expires_at = datetime.fromisoformat(str(document["expires_at"]).replace("Z", "+00:00"))
        except ValueError as exc:
            raise LeaderElectionError(f"Leader lease 时间非法: {exc}") from exc
        return cls(
            schema_version=document["schema_version"],
            holder_id=document["holder_id"],
            coordinator_epoch=document["coordinator_epoch"],
            acquired_at=acquired_at,
            renewed_at=renewed_at,
            expires_at=expires_at,
        )


@dataclass(frozen=True, slots=True)
class StoredLeaderLease:
    lease: LeaderLease
    etag: str

    def __post_init__(self) -> None:
        if not isinstance(self.etag, str) or not self.etag:
            raise LeaderElectionError("Leader lease ETag 必须是非空字符串")


class LeaderLeaseRepository(Protocol):
    def read(self) -> StoredLeaderLease | None: ...

    def acquire(
        self,
        holder_id: str,
        *,
        now: datetime,
        ttl: timedelta,
    ) -> StoredLeaderLease | None: ...

    def renew(
        self,
        current: StoredLeaderLease,
        *,
        now: datetime,
        ttl: timedelta,
    ) -> StoredLeaderLease: ...

    def release(
        self,
        current: StoredLeaderLease,
        *,
        now: datetime,
    ) -> StoredLeaderLease: ...


class S3LeaderLeaseRepository:
    """在单一 mutable 对象上实现 acquire/renew/takeover。"""

    def __init__(
        self,
        store: ObjectStore,
        *,
        key: str = "pystream/control/leader.json",
    ) -> None:
        if not key or key.startswith("/"):
            raise ValueError("Leader lease key 必须是非空相对路径")
        self.store = store
        self.key = key

    def read(self) -> StoredLeaderLease | None:
        try:
            value = self.store.get(self.key)
        except ObjectNotFound:
            return None
        except ObjectStoreError as exc:
            raise LeaderElectionError(f"读取 Leader lease 失败: {exc}") from exc
        return StoredLeaderLease(_decode_lease(value.content), value.etag)

    def acquire(
        self,
        holder_id: str,
        *,
        now: datetime,
        ttl: timedelta,
    ) -> StoredLeaderLease | None:
        observed_at = _aware_utc(now, "now")
        duration = _positive_duration(ttl, "ttl")
        current = self.read()
        if current is None:
            candidate = LeaderLease(
                holder_id=holder_id,
                coordinator_epoch=1,
                acquired_at=observed_at,
                renewed_at=observed_at,
                expires_at=observed_at + duration,
            )
            return self._put_candidate(candidate, expected_etag=None, conflict_is_none=True)
        if current.lease.expires_at > observed_at:
            return None
        candidate = LeaderLease(
            holder_id=holder_id,
            coordinator_epoch=current.lease.coordinator_epoch + 1,
            acquired_at=observed_at,
            renewed_at=observed_at,
            expires_at=observed_at + duration,
        )
        return self._put_candidate(
            candidate,
            expected_etag=current.etag,
            conflict_is_none=True,
        )

    def renew(
        self,
        current: StoredLeaderLease,
        *,
        now: datetime,
        ttl: timedelta,
    ) -> StoredLeaderLease:
        observed_at = _aware_utc(now, "now")
        duration = _positive_duration(ttl, "ttl")
        if observed_at < current.lease.renewed_at:
            raise LeaderElectionError("Leader lease 续租时间不能回退")
        if observed_at >= current.lease.expires_at:
            raise LeaderLeaseLost("Leader lease 已过期, 禁止续租复活")
        candidate = LeaderLease(
            holder_id=current.lease.holder_id,
            coordinator_epoch=current.lease.coordinator_epoch,
            acquired_at=current.lease.acquired_at,
            renewed_at=observed_at,
            expires_at=observed_at + duration,
        )
        stored = self._put_candidate(
            candidate,
            expected_etag=current.etag,
            conflict_is_none=False,
        )
        if stored is None:  # pragma: no cover - conflict_is_none=False 保证
            raise LeaderLeaseLost("Leader lease 续租 CAS 冲突")
        return stored

    def release(
        self,
        current: StoredLeaderLease,
        *,
        now: datetime,
    ) -> StoredLeaderLease:
        observed_at = _aware_utc(now, "now")
        if observed_at < current.lease.renewed_at:
            raise LeaderElectionError("Leader lease 释放时间不能回退")
        candidate = LeaderLease(
            holder_id=current.lease.holder_id,
            coordinator_epoch=current.lease.coordinator_epoch,
            acquired_at=current.lease.acquired_at,
            renewed_at=observed_at,
            expires_at=observed_at,
        )
        stored = self._put_candidate(
            candidate,
            expected_etag=current.etag,
            conflict_is_none=False,
        )
        if stored is None:  # pragma: no cover - conflict_is_none=False 保证
            raise LeaderLeaseLost("Leader lease 释放 CAS 冲突")
        return stored

    def _put_candidate(
        self,
        candidate: LeaderLease,
        *,
        expected_etag: str | None,
        conflict_is_none: bool,
    ) -> StoredLeaderLease | None:
        content = _canonical_json(candidate.to_dict())
        try:
            if expected_etag is None:
                etag = self.store.put_if_absent(self.key, content)
            else:
                etag = self.store.put_if_match(self.key, content, expected_etag)
            return StoredLeaderLease(candidate, etag)
        except ObjectStoreError as exc:
            observed = self._reconcile_candidate(candidate)
            if observed is not None:
                return observed
            if isinstance(exc, ObjectConflict):
                if conflict_is_none:
                    return None
                raise LeaderLeaseLost("Leader lease CAS 冲突") from exc
            raise LeaderElectionError(f"写入 Leader lease 失败: {exc}") from exc

    def _reconcile_candidate(
        self,
        candidate: LeaderLease,
    ) -> StoredLeaderLease | None:
        try:
            observed = self.read()
        except LeaderElectionError:
            return None
        if observed is not None and observed.lease == candidate:
            return observed
        return None


class LeaderCoordinator:
    """轮询 lease 并把角色转换映射到 JobManager 回调。"""

    def __init__(
        self,
        holder_id: str,
        repository: LeaderLeaseRepository,
        *,
        on_acquired: Callable[[int], Awaitable[None]],
        on_lost: Callable[[str], Awaitable[None]],
        on_standby: Callable[[], Awaitable[None]],
        ttl: timedelta = DEFAULT_LEASE_TTL,
        renew_interval: timedelta = DEFAULT_RENEW_INTERVAL,
        poll_interval: timedelta = DEFAULT_STANDBY_POLL_INTERVAL,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        metrics: PyStreamMetrics | None = None,
    ) -> None:
        if _SAFE_HOLDER_ID.fullmatch(holder_id) is None:
            raise ValueError("holder_id 只能包含字母、数字、下划线和连字符")
        self.holder_id = holder_id
        self.repository = repository
        self.on_acquired = on_acquired
        self.on_lost = on_lost
        self.on_standby = on_standby
        self.ttl = _positive_duration(ttl, "ttl")
        self.renew_interval = _positive_duration(renew_interval, "renew_interval")
        self.poll_interval = _positive_duration(poll_interval, "poll_interval")
        if self.renew_interval >= self.ttl:
            raise ValueError("renew_interval 必须小于 ttl")
        self.clock = clock
        self.sleep = sleep
        self.metrics = metrics
        self.role = CoordinatorRole.STANDBY
        self.current: StoredLeaderLease | None = None
        self._next_renew_at: datetime | None = None
        self._protect_until: datetime | None = None
        self._task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        if self._task is not None:
            return
        try:
            await self.poll_once()
        except Exception as exc:
            self._log_failure("leader_initial_poll_failed", exc)
        self._task = asyncio.create_task(
            self._run(),
            name=f"pystream-leader-{self.holder_id}",
        )

    async def close(self) -> None:
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        async with self._lock:
            if self.role is CoordinatorRole.ACTIVE and self.current is not None:
                with suppress(LeaderElectionError):
                    await asyncio.to_thread(
                        self.repository.release,
                        self.current,
                        now=self.clock(),
                    )
                await self.on_lost("Leader coordinator 正在关闭")
            self.current = None
            self.role = CoordinatorRole.STANDBY
            self._next_renew_at = None
            self._protect_until = None
            await self.on_standby()

    async def poll_once(self) -> CoordinatorRole:
        async with self._lock:
            now = _aware_utc(self.clock(), "clock")
            if self.role is CoordinatorRole.STANDBY:
                candidate = await asyncio.to_thread(
                    self.repository.acquire,
                    self.holder_id,
                    now=now,
                    ttl=self.ttl,
                )
                if candidate is None:
                    return self.role
                try:
                    await asyncio.wait_for(
                        self.on_acquired(candidate.lease.coordinator_epoch),
                        timeout=(candidate.lease.expires_at - now).total_seconds(),
                    )
                    activated_at = _aware_utc(self.clock(), "clock")
                    remaining = (candidate.lease.expires_at - activated_at).total_seconds()
                    if remaining <= 0:
                        raise LeaderLeaseLost("Leader lease 在激活期间过期")
                    candidate = await asyncio.wait_for(
                        asyncio.to_thread(
                            self.repository.renew,
                            candidate,
                            now=activated_at,
                            ttl=self.ttl,
                        ),
                        timeout=remaining,
                    )
                except Exception as exc:
                    if self.metrics is not None:
                        self.metrics.lease_renew_failures.inc()
                    with suppress(LeaderElectionError):
                        await asyncio.to_thread(
                            self.repository.release,
                            candidate,
                            now=_aware_utc(self.clock(), "clock"),
                        )
                    self.current = candidate
                    self.role = CoordinatorRole.PROTECTIVE
                    self._protect_until = candidate.lease.expires_at
                    await self.on_lost(f"Leader 激活失败: {type(exc).__name__}: {exc}")
                    raise
                self.current = candidate
                self.role = CoordinatorRole.ACTIVE
                self._next_renew_at = activated_at + self.renew_interval
                self._protect_until = None
                return self.role

            if self.role is CoordinatorRole.ACTIVE:
                if self._next_renew_at is not None and now < self._next_renew_at:
                    return self.role
                current = self.current
                if current is None:  # pragma: no cover - 状态不变量
                    raise LeaderElectionError("ACTIVE 缺少当前 lease")
                remaining = (current.lease.expires_at - now).total_seconds()
                try:
                    renewed = await asyncio.wait_for(
                        asyncio.to_thread(
                            self.repository.renew,
                            current,
                            now=now,
                            ttl=self.ttl,
                        ),
                        timeout=remaining,
                    )
                except Exception as exc:
                    if self.metrics is not None:
                        self.metrics.lease_renew_failures.inc()
                    self.role = CoordinatorRole.PROTECTIVE
                    self._protect_until = current.lease.expires_at
                    self._next_renew_at = None
                    await self.on_lost(f"Leader lease 续租失败: {type(exc).__name__}: {exc}")
                    return self.role
                self.current = renewed
                self._next_renew_at = now + self.renew_interval
                return self.role

            if self._protect_until is None or now >= self._protect_until:
                self.current = None
                self.role = CoordinatorRole.STANDBY
                self._protect_until = None
                await self.on_standby()
            return self.role

    async def _run(self) -> None:
        while True:
            await self.sleep(self.poll_interval.total_seconds())
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._log_failure("leader_poll_failed", exc)

    def _log_failure(self, event: str, exc: Exception) -> None:
        log_event(
            logging.getLogger(__name__),
            logging.ERROR,
            event,
            "JobManager leader 轮询失败",
            component="leader_coordinator",
            error=f"{type(exc).__name__}: {exc}",
            exc_info=exc,
            holder_id=self.holder_id,
            role=self.role.value,
        )


def _positive_duration(value: timedelta, field: str) -> timedelta:
    if not isinstance(value, timedelta) or value.total_seconds() <= 0:
        raise ValueError(f"{field} 必须是正 timedelta")
    return value


def _aware_utc(value: datetime, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise LeaderElectionError(f"{field} 必须是带时区 datetime")
    return value.astimezone(UTC)


def _canonical_json(document: object) -> bytes:
    try:
        return json.dumps(
            document,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError) as exc:
        raise LeaderElectionError(f"Leader lease 必须可严格 JSON 序列化: {exc}") from exc


def _decode_lease(content: bytes) -> LeaderLease:
    try:
        document = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LeaderElectionError("Leader lease 不是合法 UTF-8 JSON") from exc
    return LeaderLease.from_dict(document)


__all__ = [
    "DEFAULT_LEASE_TTL",
    "DEFAULT_RENEW_INTERVAL",
    "DEFAULT_STANDBY_POLL_INTERVAL",
    "LEADER_LEASE_SCHEMA_VERSION",
    "LeaderCoordinator",
    "LeaderElectionError",
    "LeaderLease",
    "LeaderLeaseLost",
    "LeaderLeaseRepository",
    "S3LeaderLeaseRepository",
    "StoredLeaderLease",
]
