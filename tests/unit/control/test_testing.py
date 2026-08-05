"""Deterministic acceptance-only checkpoint gate tests."""

from __future__ import annotations

import asyncio

import pytest

from pystream.control.testing import CheckpointTestHookError, CheckpointTestHooks


@pytest.mark.asyncio
async def test_checkpoint_test_hook_reaches_and_continues_once() -> None:
    hooks = CheckpointTestHooks()
    armed = hooks.arm("before_barrier")
    assert armed["status"] == "ARMED"

    reached = asyncio.create_task(
        hooks.reach(
            "before_barrier",
            context={
                "checkpoint_id": 3,
                "attempt_id": 1,
                "coordinator_epoch": 4,
            },
        )
    )
    await asyncio.wait_for(_wait_for_status(hooks, "REACHED"), timeout=1)
    assert hooks.view()["context"] == {
        "checkpoint_id": 3,
        "attempt_id": 1,
        "coordinator_epoch": 4,
    }

    hooks.release("before_barrier", "continue")
    await asyncio.wait_for(reached, timeout=1)
    assert hooks.view()["status"] == "RELEASED"
    assert hooks.view()["action"] == "continue"


@pytest.mark.asyncio
async def test_checkpoint_test_hook_failure_and_validation() -> None:
    hooks = CheckpointTestHooks()
    with pytest.raises(ValueError, match="must be one of"):
        hooks.arm("unknown")
    hooks.arm("before_decision")
    with pytest.raises(CheckpointTestHookError, match="still active"):
        hooks.arm("after_decision")
    with pytest.raises(CheckpointTestHookError, match="has not been reached"):
        hooks.release("before_decision", "fail")

    reached = asyncio.create_task(
        hooks.reach(
            "before_decision",
            context={
                "checkpoint_id": 5,
                "attempt_id": 0,
                "coordinator_epoch": 1,
            },
        )
    )
    await asyncio.wait_for(_wait_for_status(hooks, "REACHED"), timeout=1)
    hooks.release("before_decision", "fail")
    with pytest.raises(CheckpointTestHookError, match="deterministic failure"):
        await reached


async def _wait_for_status(hooks: CheckpointTestHooks, status: str) -> None:
    while hooks.view()["status"] != status:
        await asyncio.sleep(0)
