"""仅供 Docker 验收使用的确定性 Checkpoint gate。"""

from __future__ import annotations

import asyncio
from typing import Final

from pystream.common import JsonValue

CHECKPOINT_TEST_HOOKS: Final = frozenset(
    {
        "before_barrier",
        "before_decision",
        "after_decision",
    }
)
CHECKPOINT_TEST_HOOK_ACTIONS: Final = frozenset({"continue", "fail"})


class CheckpointTestHookError(RuntimeError):
    """A deterministic acceptance hook requested checkpoint failure."""


class CheckpointTestHooks:
    """Arm one checkpoint gate, expose when it is reached, and release it once."""

    def __init__(self) -> None:
        self._generation = 0
        self._target: str | None = None
        self._action: str | None = None
        self._reached = asyncio.Event()
        self._released = asyncio.Event()
        self._state: dict[str, JsonValue] = {
            "generation": 0,
            "hook": None,
            "status": "IDLE",
            "action": None,
            "context": {},
        }

    def arm(self, hook: str) -> dict[str, JsonValue]:
        """Arm a supported hook when no previous gate is active."""
        _require_hook(hook)
        if self._target is not None:
            raise CheckpointTestHookError(f"Checkpoint test hook {self._target!r} is still active")
        self._generation += 1
        self._target = hook
        self._action = None
        self._reached.clear()
        self._released.clear()
        self._state = {
            "generation": self._generation,
            "hook": hook,
            "status": "ARMED",
            "action": None,
            "context": {},
        }
        return self.view()

    async def reach(
        self,
        hook: str,
        *,
        context: dict[str, JsonValue],
    ) -> None:
        """Wait at an armed gate; unarmed hook calls are no-ops."""
        _require_hook(hook)
        if self._target != hook:
            return
        self._state = {
            "generation": self._generation,
            "hook": hook,
            "status": "REACHED",
            "action": None,
            "context": dict(context),
        }
        self._reached.set()
        try:
            await self._released.wait()
        except asyncio.CancelledError:
            self._state["status"] = "CANCELLED"
            self._target = None
            raise
        action = self._action
        self._state["status"] = "RELEASED"
        self._state["action"] = action
        self._target = None
        if action == "fail":
            raise CheckpointTestHookError(
                f"Checkpoint test hook {hook!r} requested deterministic failure"
            )

    def release(self, hook: str, action: str) -> dict[str, JsonValue]:
        """Release a reached gate with either normal continuation or failure."""
        _require_hook(hook)
        if action not in CHECKPOINT_TEST_HOOK_ACTIONS:
            raise ValueError(
                f"Checkpoint test hook action must be one of {sorted(CHECKPOINT_TEST_HOOK_ACTIONS)}"
            )
        if self._target != hook or not self._reached.is_set():
            raise CheckpointTestHookError(f"Checkpoint test hook {hook!r} has not been reached")
        if self._released.is_set():
            raise CheckpointTestHookError(
                f"Checkpoint test hook {hook!r} has already been released"
            )
        self._action = action
        self._state["status"] = "RELEASING"
        self._state["action"] = action
        self._released.set()
        return self.view()

    def view(self) -> dict[str, JsonValue]:
        """Return a detached JSON-compatible state snapshot."""
        context = self._state["context"]
        return {
            **self._state,
            "context": dict(context) if isinstance(context, dict) else {},
        }


def _require_hook(hook: str) -> None:
    if hook not in CHECKPOINT_TEST_HOOKS:
        raise ValueError(f"Checkpoint test hook must be one of {sorted(CHECKPOINT_TEST_HOOKS)}")


__all__ = [
    "CHECKPOINT_TEST_HOOKS",
    "CheckpointTestHookError",
    "CheckpointTestHooks",
]
