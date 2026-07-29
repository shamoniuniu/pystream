"""基础算子和处理时间窗口的确定性测试。"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from pystream.common import ChangeKind, RecordEnvelope
from pystream.operators import (
    KeyByOperator,
    ManualClock,
    MapOperator,
    OperatorContext,
    OperatorLifecycleError,
    OperatorState,
    RecordValidationError,
    ReduceWindowOperator,
    TumblingProcessingTimeWindowAssigner,
    UnsupportedStateOperation,
)


def at(hour: int, minute: int = 0, second: int = 0) -> datetime:
    """构造固定测试日的 UTC 时间。"""
    return datetime(2026, 7, 26, hour, minute, second, tzinfo=UTC)


def record(
    payload: Any,
    *,
    processing_time: datetime | None = None,
    event_time: datetime | None = None,
    change_kind: ChangeKind = ChangeKind.INSERT,
    key: Any = None,
    record_id: str = "words:0:1",
) -> RecordEnvelope:
    """构造一条测试记录。"""
    return RecordEnvelope(
        record_id=record_id,
        payload=payload,
        processing_time=processing_time or at(12),
        event_time=event_time,
        change_kind=change_kind,
        key=key,
    )


def context(clock: ManualClock | None = None) -> OperatorContext:
    """构造带可控 Clock 的算子上下文。"""
    return OperatorContext("test-operator", clock=clock or ManualClock(at(12)))


def add_counts(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    """WordCount Reduce UDF。"""
    return {"word": left["word"], "count": left["count"] + right["count"]}


def test_lifecycle_rejects_processing_before_open_and_after_close() -> None:
    operator = MapOperator(context(), lambda value: value)

    with pytest.raises(OperatorLifecycleError):
        operator.process(record({"value": 1}))

    operator.open()
    assert operator.state is OperatorState.OPEN
    operator.close()
    operator.close()
    assert operator.state is OperatorState.CLOSED

    with pytest.raises(OperatorLifecycleError):
        operator.process(record({"value": 1}))
    with pytest.raises(OperatorLifecycleError):
        operator.open()


def test_first_phase_snapshot_and_restore_are_explicitly_unsupported() -> None:
    operator = MapOperator(context(), lambda value: value)

    with pytest.raises(UnsupportedStateOperation):
        operator.snapshot_state()
    with pytest.raises(UnsupportedStateOperation):
        operator.restore_state(b"snapshot")


def test_map_normalizes_word_without_mutating_input() -> None:
    operator = MapOperator(
        context(),
        lambda payload: {"word": payload["word"].lower(), "count": payload["count"]},
    )
    operator.open()
    source = record({"word": "APPLE", "count": 1})

    outputs = operator.process(source)

    assert outputs[0].payload == {"word": "apple", "count": 1}
    assert source.payload == {"word": "APPLE", "count": 1}
    assert outputs[0].record_id == source.record_id


def test_map_and_keyby_preserve_changelog_kind() -> None:
    mapper = MapOperator(context(), lambda payload: {**payload, "normalized": True})
    key_by = KeyByOperator(context(), lambda payload: payload["word"])
    mapper.open()
    key_by.open()
    source = record(
        {"word": "apple", "count": 1},
        change_kind=ChangeKind.UPDATE_BEFORE,
    )

    mapped = mapper.process(source)[0]
    keyed = key_by.process(mapped)[0]

    assert mapped.change_kind is ChangeKind.UPDATE_BEFORE
    assert keyed.change_kind is ChangeKind.UPDATE_BEFORE
    assert keyed.key == "apple"


def test_map_none_explicitly_drops_record() -> None:
    operator = MapOperator(context(), lambda _payload: None)
    operator.open()

    assert operator.process(record({"word": "skip"})) == []


@pytest.mark.parametrize("bad_result", [{1, 2}, float("nan"), float("inf")])
def test_map_rejects_non_json_result(bad_result: Any) -> None:
    operator = MapOperator(context(), lambda _payload: bad_result)
    operator.open()

    with pytest.raises(RecordValidationError, match="JSON"):
        operator.process(record({"word": "apple"}))


def test_key_by_sets_json_key_and_preserves_payload() -> None:
    operator = KeyByOperator(context(), lambda payload: payload["word"])
    operator.open()
    source = record({"word": "apple", "count": 1})

    output = operator.process(source)[0]

    assert output.key == "apple"
    assert output.payload == source.payload
    assert source.key is None


@pytest.mark.parametrize("bad_key", [None, {"not-json"}])
def test_key_by_rejects_null_or_non_json_key(bad_key: Any) -> None:
    operator = KeyByOperator(context(), lambda _payload: bad_key)
    operator.open()

    with pytest.raises(RecordValidationError):
        operator.process(record({"word": "apple"}))


def test_window_assigner_is_epoch_aligned_and_left_closed_right_open() -> None:
    assigner = TumblingProcessingTimeWindowAssigner.from_seconds(300)

    first = assigner.assign(at(12, 4, 59))
    boundary = assigner.assign(at(12, 5))

    assert first.start == at(12)
    assert first.end == at(12, 5)
    assert first.contains(at(12))
    assert first.contains(at(12, 4, 59))
    assert not first.contains(at(12, 5))
    assert boundary.start == at(12, 5)
    assert boundary.end == at(12, 10)


def test_default_window_size_is_five_minutes() -> None:
    operator = ReduceWindowOperator(context(), add_counts)

    assert operator.window_size == timedelta(seconds=300)


def test_reduce_isolates_keys_and_emits_at_window_end() -> None:
    clock = ManualClock(at(12))
    operator = ReduceWindowOperator(context(clock), add_counts)
    operator.open()

    operator.process(record({"word": "apple", "count": 1}, key="apple"))
    operator.process(
        record(
            {"word": "apple", "count": 2},
            key="apple",
            record_id="words:0:2",
        )
    )
    operator.process(
        record(
            {"word": "pie", "count": 1},
            key="pie",
            record_id="words:0:3",
        )
    )

    assert operator.state_metrics == {"state_entries": 2, "active_windows": 1}
    assert operator.on_timer() == []

    clock.set(at(12, 5))
    outputs = operator.on_timer()

    assert [(item.key, item.payload) for item in outputs] == [
        ("apple", {"word": "apple", "count": 3}),
        ("pie", {"word": "pie", "count": 1}),
    ]
    assert {item.headers["window_end"] for item in outputs} == {"2026/07/26T12:05:00"}
    assert all(item.processing_time == at(12, 5) for item in outputs)
    assert operator.state_size == 0
    assert operator.active_window_count == 0


def test_event_time_window_accepts_bounded_out_of_order_and_fires_on_watermark() -> None:
    clock = ManualClock(at(13))
    operator = ReduceWindowOperator(
        context(clock),
        add_counts,
        window_size_seconds=5,
        time_characteristic="event",
    )
    operator.open()

    operator.process(
        record(
            {"word": "apple", "count": 1},
            key="apple",
            event_time=at(12, 0, 1),
        )
    )
    operator.process(
        record(
            {"word": "apple", "count": 1},
            key="apple",
            event_time=at(12, 0, 4),
            record_id="words:0:2",
        )
    )
    operator.process(
        record(
            {"word": "apple", "count": 1},
            key="apple",
            event_time=at(12, 0, 3),
            record_id="words:0:3",
        )
    )

    assert operator.on_timer() == []
    assert operator.on_watermark(at(12, 0, 4)) == []
    outputs = operator.on_watermark(at(12, 0, 5))

    assert [item.payload["count"] for item in outputs] == [3]
    assert outputs[0].headers["window_end"] == "2026/07/26T12:00:05"
    assert operator.state_metrics == {
        "state_entries": 0,
        "active_windows": 0,
        "late_records": 0,
    }


def test_event_time_record_at_or_before_watermark_is_late() -> None:
    operator = ReduceWindowOperator(
        context(),
        add_counts,
        window_size_seconds=5,
        time_characteristic="event",
    )
    operator.open()
    operator.on_watermark(at(12, 0, 2))

    assert (
        operator.process(
            record(
                {"word": "apple", "count": 1},
                key="apple",
                event_time=at(12, 0, 2),
            )
        )
        == []
    )
    assert operator.state_metrics["late_records"] == 1
    with pytest.raises(RecordValidationError, match="不能回退"):
        operator.on_watermark(at(12, 0, 1))


def test_event_time_window_requires_event_time() -> None:
    operator = ReduceWindowOperator(
        context(),
        add_counts,
        time_characteristic="event",
    )
    operator.open()

    with pytest.raises(RecordValidationError, match="event_time"):
        operator.process(record({"word": "apple", "count": 1}, key="apple"))


def test_changelog_reduce_emits_insert_then_before_after_and_cleans_on_window_end() -> None:
    clock = ManualClock(at(12))
    operator = ReduceWindowOperator(
        context(clock),
        add_counts,
        window_size_seconds=5,
        emit_mode="changelog",
    )
    operator.open()

    first = operator.process(record({"word": "apple", "count": 1}, key="apple"))
    update = operator.process(
        record(
            {"word": "apple", "count": 1},
            key="apple",
            record_id="words:0:2",
        )
    )

    assert [(item.change_kind, item.payload) for item in first] == [
        (ChangeKind.INSERT, {"word": "apple", "count": 1})
    ]
    assert [(item.change_kind, item.payload) for item in update] == [
        (ChangeKind.UPDATE_BEFORE, {"word": "apple", "count": 1}),
        (ChangeKind.UPDATE_AFTER, {"word": "apple", "count": 2}),
    ]
    assert operator.state_metrics["changelog_records"] == 3
    clock.set(at(12, 0, 5))
    assert operator.on_timer() == []
    assert operator.state_size == 0


def test_retract_reduce_updates_old_bucket_and_new_bucket() -> None:
    clock = ManualClock(at(12))

    def add_bucket(left: dict[str, int], right: dict[str, int]) -> dict[str, int]:
        return {"count": left["count"], "word_count": left["word_count"] + right["word_count"]}

    def remove_bucket(left: dict[str, int], right: dict[str, int]):
        remaining = left["word_count"] - right["word_count"]
        return None if remaining == 0 else {"count": left["count"], "word_count": remaining}

    operator = ReduceWindowOperator(
        context(clock),
        add_bucket,
        window_size_seconds=5,
        retract_function=remove_bucket,
    )
    operator.open()
    operator.process(record({"count": 1, "word_count": 1}, key=1))
    operator.process(
        record(
            {"count": 1, "word_count": 1},
            key=1,
            record_id="words:0:2",
        )
    )
    operator.process(
        record(
            {"count": 1, "word_count": 1},
            key=1,
            record_id="words:0:3",
            change_kind=ChangeKind.UPDATE_BEFORE,
        )
    )
    operator.process(
        record(
            {"count": 2, "word_count": 1},
            key=2,
            record_id="words:0:3",
            change_kind=ChangeKind.UPDATE_AFTER,
        )
    )

    assert operator.state_metrics["retractions_applied"] == 1
    clock.set(at(12, 0, 5))
    outputs = operator.on_timer()
    assert [(item.key, item.payload) for item in outputs] == [
        (1, {"count": 1, "word_count": 1}),
        (2, {"count": 2, "word_count": 1}),
    ]


def test_retract_to_null_deletes_state_and_missing_state_fails() -> None:
    operator = ReduceWindowOperator(
        context(),
        add_counts,
        retract_function=lambda _left, _right: None,
    )
    operator.open()

    with pytest.raises(RecordValidationError, match="不存在"):
        operator.process(
            record(
                {"word": "apple", "count": 1},
                key="apple",
                change_kind=ChangeKind.DELETE,
            )
        )

    operator.process(record({"word": "apple", "count": 1}, key="apple"))
    assert (
        operator.process(
            record(
                {"word": "apple", "count": 1},
                key="apple",
                change_kind=ChangeKind.UPDATE_BEFORE,
            )
        )
        == []
    )
    assert operator.state_size == 0
    assert operator.state_metrics["retract_state_deletes"] == 1


def test_records_on_window_boundary_do_not_accumulate_across_windows() -> None:
    clock = ManualClock(at(12))
    operator = ReduceWindowOperator(context(clock), add_counts)
    operator.open()

    operator.process(
        record(
            {"word": "apple", "count": 1},
            key="apple",
            processing_time=at(12, 4, 59),
        )
    )
    operator.process(
        record(
            {"word": "apple", "count": 5},
            key="apple",
            processing_time=at(12, 5),
            record_id="words:0:2",
        )
    )

    clock.set(at(12, 5))
    first = operator.on_timer()
    assert [item.payload["count"] for item in first] == [1]
    assert operator.state_size == 1

    clock.set(at(12, 10))
    second = operator.on_timer()
    assert [item.payload["count"] for item in second] == [5]
    assert operator.state_size == 0


def test_empty_and_repeated_timer_produce_no_output() -> None:
    clock = ManualClock(at(12, 5))
    operator = ReduceWindowOperator(context(clock), add_counts)
    operator.open()

    assert operator.on_timer() == []
    operator.process(
        record(
            {"word": "apple", "count": 1},
            key="apple",
            processing_time=at(12),
        )
    )
    assert len(operator.on_timer()) == 1
    assert operator.on_timer() == []


def test_close_clears_active_window_state() -> None:
    operator = ReduceWindowOperator(context(), add_counts)
    operator.open()
    operator.process(record({"word": "apple", "count": 1}, key="apple"))

    operator.close()

    assert operator.state_size == 0
    assert operator.active_window_count == 0


def test_reduce_rejects_unkeyed_record_and_invalid_udf_result() -> None:
    operator = ReduceWindowOperator(context(), lambda _left, _right: {"bad"})
    operator.open()

    with pytest.raises(RecordValidationError, match="KeyBy"):
        operator.process(record({"word": "apple", "count": 1}))

    operator.process(record({"word": "apple", "count": 1}, key="apple"))
    with pytest.raises(RecordValidationError, match="JSON"):
        operator.process(
            record(
                {"word": "apple", "count": 1},
                key="apple",
                record_id="words:0:2",
            )
        )


def test_reduce_udf_failure_does_not_mutate_existing_accumulator() -> None:
    def mutating_failure(left: dict[str, Any], _right: dict[str, Any]) -> dict[str, Any]:
        left["count"] = 999
        raise RuntimeError("UDF failed")

    clock = ManualClock(at(12))
    operator = ReduceWindowOperator(context(clock), mutating_failure)
    operator.open()
    operator.process(record({"word": "apple", "count": 1}, key="apple"))

    with pytest.raises(RuntimeError, match="UDF failed"):
        operator.process(
            record(
                {"word": "apple", "count": 1},
                key="apple",
                record_id="words:0:2",
            )
        )

    clock.set(at(12, 5))
    assert operator.on_timer()[0].payload["count"] == 1


def test_manual_clock_rejects_naive_time_and_backward_movement() -> None:
    with pytest.raises(ValueError, match="时区"):
        ManualClock(datetime(2026, 7, 26, 12))

    clock = ManualClock(at(12))
    with pytest.raises(ValueError, match="向后"):
        clock.set(at(11, 59))
    with pytest.raises(ValueError, match="负"):
        clock.advance(-1)
