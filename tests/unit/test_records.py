"""公共 RecordEnvelope 契约测试。"""

from datetime import UTC, datetime, timedelta, timezone

import pytest

from pystream.common import (
    ChangeKind,
    MessageType,
    RecordEnvelope,
    RecordValidationError,
)


def make_record(**overrides) -> RecordEnvelope:
    """构造一条固定 UTC 时间的测试记录。"""
    values = {
        "record_id": "words:0:7",
        "payload": {"word": "apple", "count": 1},
        "processing_time": datetime(2026, 7, 26, 12, 0, tzinfo=UTC),
    }
    values.update(overrides)
    return RecordEnvelope(**values)


def test_记录信封完整往返并保留后续阶段字段():
    original = make_record(
        key={"tenant": "demo", "word": "apple"},
        event_time=datetime(2026, 7, 26, 11, 59, tzinfo=UTC),
        message_type=MessageType.BARRIER,
        change_kind=ChangeKind.UPDATE_AFTER,
        checkpoint_id=3,
        headers={"trace_id": "abc"},
    )

    restored = RecordEnvelope.from_dict(original.to_dict())

    assert restored == original
    assert restored.message_type is MessageType.BARRIER
    assert restored.change_kind is ChangeKind.UPDATE_AFTER


def test_with_helpers_返回新信封并保留系统元数据():
    original = make_record()

    keyed = original.with_key("apple")
    mapped = keyed.with_payload({"word": "apple", "count": 2})

    assert original.key is None
    assert keyed.key == "apple"
    assert mapped.payload["count"] == 2
    assert mapped.record_id == original.record_id
    assert mapped.processing_time == original.processing_time


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("processing_time", datetime(2026, 7, 26, 12, 0)),
        (
            "processing_time",
            datetime(2026, 7, 26, 20, 0, tzinfo=timezone(timedelta(hours=8))),
        ),
        ("event_time", datetime(2026, 7, 26, 12, 0)),
    ],
)
def test_时间字段必须显式使用_utc(field_name, value):
    with pytest.raises(RecordValidationError, match=f"{field_name} 必须"):
        make_record(**{field_name: value})


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("payload", object()),
        ("payload", float("nan")),
        ("key", {1, 2}),
        ("headers", {"invalid": object()}),
    ],
)
def test_payload_key_headers_必须严格可_json_序列化(field_name, value):
    with pytest.raises(RecordValidationError, match=field_name):
        make_record(**{field_name: value})


@pytest.mark.parametrize("record_id", ["", None, 1])
def test_record_id_必须是非空字符串(record_id):
    with pytest.raises(RecordValidationError, match="record_id"):
        make_record(record_id=record_id)


@pytest.mark.parametrize("checkpoint_id", [-1, True, "1"])
def test_checkpoint_id_必须是非负整数(checkpoint_id):
    with pytest.raises(RecordValidationError, match="checkpoint_id"):
        make_record(checkpoint_id=checkpoint_id)


def test_barrier_必须包含checkpoint_id():
    with pytest.raises(RecordValidationError, match="BARRIER 必须包含"):
        make_record(message_type=MessageType.BARRIER)


def test_from_dict_拒绝缺失和未知字段():
    document = make_record().to_dict()
    document.pop("key")

    with pytest.raises(RecordValidationError, match="缺少字段: key"):
        RecordEnvelope.from_dict(document)

    document = make_record().to_dict()
    document["unexpected"] = 1
    with pytest.raises(RecordValidationError, match="未知字段: unexpected"):
        RecordEnvelope.from_dict(document)


def test_from_dict_拒绝未知枚举和非法时间():
    document = make_record().to_dict()
    document["message_type"] = "UNKNOWN"
    with pytest.raises(RecordValidationError, match="message_type"):
        RecordEnvelope.from_dict(document)

    document = make_record().to_dict()
    document["processing_time"] = "not-a-time"
    with pytest.raises(RecordValidationError, match="ISO-8601"):
        RecordEnvelope.from_dict(document)


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("message_type", "DATA"),
        ("change_kind", "INSERT"),
        ("headers", {1: "not-a-string-key"}),
    ],
)
def test_直接构造也拒绝错误枚举和_headers(field_name, value):
    with pytest.raises(RecordValidationError, match=field_name):
        make_record(**{field_name: value})
