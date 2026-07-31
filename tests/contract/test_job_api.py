"""YAML v1 契约、DataStream 属性和 DAG 校验测试。"""

from __future__ import annotations

from copy import deepcopy

import pytest
import yaml

from pystream.api import (
    API_VERSION,
    DeliveryGuarantee,
    FileSinkConfig,
    JobConfigError,
    OperatorType,
    Partitioning,
    build_stream_graph,
    load_job_yaml,
    load_stream_graph,
    parse_job_yaml,
    parse_stream_graph,
)
from pystream.api.errors import ConfigIssue, format_path
from pystream.api.models import TumblingWindowConfig


def operator(operator_id: str, operator_type: str, **overrides):
    """构造最小合法算子字典。"""
    result = {
        "id": operator_id,
        "type": operator_type,
        "parallelism": 1,
    }
    if operator_type == "source":
        result["config"] = {
            "connector": "kafka",
            "topic": "words",
            "value_format": "json",
        }
    elif operator_type == "sink":
        result["config"] = {"connector": "file", "format": "csv"}
    elif operator_type in {"map", "key_by", "reduce"}:
        result["udf"] = f"wordcount_udfs:{operator_type}"
    if operator_type == "reduce":
        result["window"] = {
            "type": "tumbling",
            "time_characteristic": "processing",
            "size": "300s",
        }
    result.update(overrides)
    return result


def linear_job() -> dict:
    """返回覆盖全部第一阶段算子的作业。"""
    return {
        "api_version": API_VERSION,
        "job": {"name": "wordcount"},
        "operators": [
            operator("words", "source", parallelism=2),
            operator("normalize", "map", parallelism=2),
            operator("by_word", "key_by", parallelism=2),
            operator("totals", "reduce", parallelism=3),
            operator("output", "sink", parallelism=1),
        ],
        "edges": [
            {"from": "words", "to": "normalize"},
            {"from": "normalize", "to": "by_word"},
            {"from": "by_word", "to": "totals"},
            {"from": "totals", "to": "output"},
        ],
    }


def as_yaml(document: dict) -> str:
    """用稳定顺序生成测试 YAML。"""
    return yaml.safe_dump(document, sort_keys=False, allow_unicode=True)


def assert_config_error(document: dict, path: str, message: str) -> JobConfigError:
    """断言完整解析在指定路径失败。"""
    with pytest.raises(JobConfigError) as exc_info:
        parse_stream_graph(as_yaml(document))
    assert path in str(exc_info.value)
    assert message in str(exc_info.value)
    return exc_info.value


def test_合法线性作业保留配置并生成稳定拓扑和分区():
    graph = parse_stream_graph(as_yaml(linear_job()))

    assert graph.definition.api_version == API_VERSION
    assert graph.definition.job.name == "wordcount"
    assert graph.topological_order == ("words", "normalize", "by_word", "totals", "output")
    assert graph.total_parallelism == 10
    assert graph.operator("totals").type is OperatorType.REDUCE
    assert graph.operator("totals").window.size_seconds == 300
    assert [edge.partitioning for edge in graph.edges] == [
        Partitioning.FORWARD,
        Partitioning.FORWARD,
        Partitioning.HASH,
        Partitioning.HASH,
    ]
    assert graph.data_stream("words").keyed is False
    assert graph.data_stream("by_word").keyed is True
    assert graph.data_stream("totals").partitioning is Partitioning.HASH


def test_普通边并发度不一致时使用_rebalance():
    document = linear_job()
    document["operators"][1]["parallelism"] = 3

    graph = parse_stream_graph(as_yaml(document))

    assert graph.edges[0].partitioning is Partitioning.REBALANCE


def test_keyed_stream_经过_map_仍保持_hash_分区():
    document = linear_job()
    document["operators"].insert(3, operator("decorate", "map", parallelism=4))
    document["edges"] = [
        {"from": "words", "to": "normalize"},
        {"from": "normalize", "to": "by_word"},
        {"from": "by_word", "to": "decorate"},
        {"from": "decorate", "to": "totals"},
        {"from": "totals", "to": "output"},
    ]

    graph = parse_stream_graph(as_yaml(document))

    assert graph.data_stream("decorate").keyed is True
    assert graph.outgoing_edges("decorate")[0].partitioning is Partitioning.HASH


def test_分支_dag_保留每条独立下游边():
    document = linear_job()
    document["operators"].append(operator("audit", "sink"))
    document["edges"].append({"from": "normalize", "to": "audit"})

    graph = parse_stream_graph(as_yaml(document))

    assert {edge.target_id for edge in graph.outgoing_edges("normalize")} == {
        "by_word",
        "audit",
    }
    assert graph.topological_order.index("normalize") < graph.topological_order.index("audit")


def test_多上游合流到普通算子():
    document = {
        "api_version": API_VERSION,
        "job": {"name": "merge"},
        "operators": [
            operator("left", "source"),
            operator("right", "source"),
            operator("normalize", "map"),
            operator("output", "sink"),
        ],
        "edges": [
            {"from": "left", "to": "normalize"},
            {"from": "right", "to": "normalize"},
            {"from": "normalize", "to": "output"},
        ],
    }

    graph = parse_stream_graph(as_yaml(document))

    assert len(graph.incoming_edges("normalize")) == 2
    assert graph.data_stream("normalize").keyed is False


@pytest.mark.parametrize(
    ("size", "milliseconds"),
    [("1ms", 1), ("2s", 2_000), ("3m", 180_000), ("4h", 14_400_000)],
)
def test_窗口持续时间支持四种单位(size, milliseconds):
    window = TumblingWindowConfig(size=size)

    assert window.size_milliseconds == milliseconds
    assert window.size_seconds == milliseconds / 1_000


@pytest.mark.parametrize("size", ["0s", "-1s", "1d", "1.5s", "seconds"])
def test_非法窗口持续时间给出_window_路径(size):
    document = linear_job()
    document["operators"][3]["window"]["size"] = size

    assert_config_error(document, "operators[3].window", "size 必须是正整数")


def test_未知字段被拒绝并包含准确路径():
    document = linear_job()
    document["operators"][0]["paralellism"] = 2

    assert_config_error(document, "operators[0].paralellism", "Extra inputs are not permitted")


def test_source_允许配置_payload_validator():
    document = linear_job()
    document["operators"][0]["config"]["validator"] = "wordcount_udfs:validate_input"

    graph = parse_stream_graph(as_yaml(document))

    assert graph.operator("words").config.validator == "wordcount_udfs:validate_input"


def test_source_validator_必须使用_module_function_格式():
    document = linear_job()
    document["operators"][0]["config"]["validator"] = "missing_separator"

    assert_config_error(
        document,
        "operators[0].config.kafka.validator",
        "String should match pattern",
    )


def test_布尔值不能冒充并发度整数():
    document = linear_job()
    document["operators"][0]["parallelism"] = True

    assert_config_error(document, "operators[0].parallelism", "valid integer")


@pytest.mark.parametrize("parallelism", [0, -1, 1025])
def test_并发度必须位于允许范围(parallelism):
    document = linear_job()
    document["operators"][0]["parallelism"] = parallelism

    assert_config_error(document, "operators[0].parallelism", "than or equal")


@pytest.mark.parametrize("operator_type", ["map", "key_by", "reduce"])
def test_计算算子必须配置_udf(operator_type):
    document = linear_job()
    target = next(item for item in document["operators"] if item["type"] == operator_type)
    target.pop("udf")

    assert_config_error(document, "operators", f"{operator_type} 算子必须配置 udf")


@pytest.mark.parametrize("operator_type", ["source", "sink"])
def test_连接器算子禁止配置_udf(operator_type):
    document = linear_job()
    target = next(item for item in document["operators"] if item["type"] == operator_type)
    target["udf"] = "mod:func"

    assert_config_error(document, "operators", f"{operator_type} 算子不能配置 udf")


def test_source_必须使用_kafka_connector():
    document = linear_job()
    document["operators"][0]["config"] = {"connector": "file"}

    assert_config_error(document, "operators", "source 算子必须配置 Kafka connector")


def test_sink_必须使用_file_connector():
    document = linear_job()
    document["operators"][-1]["config"] = {
        "connector": "kafka",
        "topic": "invalid",
    }

    assert_config_error(document, "operators", "sink 算子必须配置 File connector")


def test_普通计算算子禁止连接器配置():
    document = linear_job()
    document["operators"][1]["config"] = {
        "connector": "file",
    }

    assert_config_error(document, "operators", "map 算子不能配置 connector")


def test_reduce_必须配置窗口():
    document = linear_job()
    document["operators"][3].pop("window")

    assert_config_error(document, "operators", "reduce 算子必须配置处理时间滚动窗口")


def test_非_reduce_禁止窗口配置():
    document = linear_job()
    document["operators"][1]["window"] = {
        "type": "tumbling",
        "time_characteristic": "processing",
        "size": "10s",
    }

    assert_config_error(document, "operators", "map 算子不能配置 window")


def test_udf_引用格式必须是_module_function():
    document = linear_job()
    document["operators"][1]["udf"] = "missing_separator"

    assert_config_error(document, "operators[1].udf", "String should match pattern")


def test_算子_id_格式错误被拒绝():
    document = linear_job()
    document["operators"][0]["id"] = "0-invalid"

    assert_config_error(document, "operators[0].id", "String should match pattern")


def test_不支持的_api_version_被拒绝():
    document = linear_job()
    document["api_version"] = "pystream/v2"

    assert_config_error(document, "api_version", "pystream/v1")


def test_重复_yaml_key_在模型校验前失败():
    content = """\
api_version: pystream/v1
job:
  name: first
  name: second
operators: []
edges: []
"""

    with pytest.raises(JobConfigError, match=r"\$: YAML 语法错误.*重复字段 'name'"):
        parse_job_yaml(content)


@pytest.mark.parametrize("content", ["[]", "null", "hello"])
def test_yaml_根节点必须是_mapping(content):
    with pytest.raises(JobConfigError, match="根节点必须是 mapping"):
        parse_job_yaml(content)


def test_yaml_语法错误包含行列():
    with pytest.raises(JobConfigError, match=r"第 3 行.*第 1 列"):
        parse_job_yaml("job:\n  - bad: [\n")


def test_从_utf8_文件加载_definition_和_graph(tmp_path):
    path = tmp_path / "job.yaml"
    path.write_text(as_yaml(linear_job()), encoding="utf-8")

    definition = load_job_yaml(path)
    graph = load_stream_graph(path)

    assert definition.job.name == "wordcount"
    assert graph.topological_order[-1] == "output"


def test_读取不存在文件时转换为路径化配置错误(tmp_path):
    with pytest.raises(JobConfigError, match=r"\$: 无法读取作业文件"):
        load_job_yaml(tmp_path / "missing.yaml")


def test_重复算子_id_指出首次位置():
    document = linear_job()
    document["operators"][1]["id"] = "words"

    assert_config_error(document, "operators[1].id", "首次出现在 operators[0].id")


@pytest.mark.parametrize(
    ("field", "value"),
    [("from", "missing_source"), ("to", "missing_target")],
)
def test_边端点必须引用已存在算子(field, value):
    document = linear_job()
    document["edges"][0][field] = value

    assert_config_error(document, f"edges[0].{field}", "不存在的算子")


def test_重复边被拒绝():
    document = linear_job()
    document["edges"].append(deepcopy(document["edges"][0]))

    assert_config_error(document, "edges[4]", "重复边")


def test_自环被拒绝():
    document = linear_job()
    document["edges"][0] = {"from": "words", "to": "words"}

    assert_config_error(document, "edges[0]", "连接到自身")


def test_source_不能有上游():
    document = linear_job()
    document["edges"].append({"from": "output", "to": "words"})

    assert_config_error(document, "operators[0].type", "source 算子的入度必须为 0")


def test_sink_不能有下游():
    document = linear_job()
    document["operators"].append(operator("second_output", "sink"))
    document["edges"].append({"from": "output", "to": "second_output"})

    assert_config_error(document, "operators[4].type", "sink 算子的出度必须为 0")


def test_非_source_必须有上游():
    document = linear_job()
    document["edges"] = document["edges"][1:]

    assert_config_error(document, "operators[1].id", "必须至少有一个上游")


def test_非_sink_必须有下游():
    document = linear_job()
    document["edges"] = document["edges"][:-1]

    assert_config_error(document, "operators[3].id", "必须至少有一个下游")


def test_图必须同时包含_source_与_sink():
    no_source = linear_job()
    no_source["operators"][0] = operator("words", "map")
    no_sink = linear_job()
    no_sink["operators"][-1] = operator("output", "map")

    assert_config_error(no_source, "operators", "至少包含一个 source")
    assert_config_error(no_sink, "operators", "至少包含一个 sink")


def test_独立子图中的环被_dag_校验捕获():
    document = {
        "api_version": API_VERSION,
        "job": {"name": "cycle"},
        "operators": [
            operator("source", "source"),
            operator("sink", "sink"),
            operator("a", "map"),
            operator("b", "map"),
        ],
        "edges": [
            {"from": "source", "to": "sink"},
            {"from": "a", "to": "b"},
            {"from": "b", "to": "a"},
        ],
    }

    assert_config_error(document, "edges", "检测到环涉及: a, b")


def test_reduce_拒绝未_keyby_的输入():
    document = linear_job()
    document["edges"] = [
        {"from": "words", "to": "normalize"},
        {"from": "normalize", "to": "totals"},
        {"from": "totals", "to": "output"},
    ]
    document["operators"] = [item for item in document["operators"] if item["id"] != "by_word"]

    assert_config_error(document, "operators[2].type", "只能消费 keyed stream")


def test_reduce_的所有合流上游都必须_keyed():
    document = linear_job()
    document["operators"].insert(1, operator("raw", "source"))
    document["edges"].insert(0, {"from": "raw", "to": "totals"})

    assert_config_error(document, "type", "所有上游路径中先使用 key_by")


def test_图查询_helpers_和未知_id():
    graph = build_stream_graph(parse_job_yaml(as_yaml(linear_job())))

    assert graph.incoming_edges("words") == ()
    assert graph.outgoing_edges("output") == ()
    assert graph.incoming_edges("totals")[0].source_id == "by_word"
    with pytest.raises(KeyError):
        graph.operator("missing")
    with pytest.raises(KeyError):
        graph.data_stream("missing")


def test_错误对象格式和空问题保护():
    issue = ConfigIssue("operators[2].id", "错误")

    assert str(issue) == "operators[2].id: 错误"
    assert format_path(("operators", 2, "id")) == "operators[2].id"
    assert format_path(()) == "$"
    with pytest.raises(ValueError, match="至少需要一条问题"):
        JobConfigError([])


def event_time_job() -> dict:
    """返回在现有线性 DAG 上启用事件时间的兼容作业。"""
    document = linear_job()
    document["execution"] = {
        "event_time": {
            "max_out_of_orderness": "2s",
            "idle_timeout": "30s",
        },
        "checkpoint": {
            "interval": "10s",
            "timeout": "30s",
            "max_consecutive_failures": 3,
        },
        "restart": {"max_attempts": 3, "delay": "2s"},
    }
    document["operators"][0]["config"]["event_time"] = {
        "pointer": "/event_time",
        "format": "rfc3339",
    }
    document["operators"][3]["window"]["time_characteristic"] = "event"
    return document


def test_旧作业不配置_execution_时保持第一阶段默认() -> None:
    graph = parse_stream_graph(as_yaml(linear_job()))

    assert graph.definition.execution is None
    assert graph.operator("totals").emit_mode == "final"
    assert graph.data_stream("totals").changelog is False
    assert graph.operator("output").config.columns is None


def test_事件时间作业解析默认值和毫秒属性() -> None:
    graph = parse_stream_graph(as_yaml(event_time_job()))
    execution = graph.definition.execution

    assert execution is not None
    assert execution.delivery_guarantee is DeliveryGuarantee.EXACTLY_ONCE
    assert execution.event_time is not None
    assert execution.event_time.max_out_of_orderness_milliseconds == 2_000
    assert execution.event_time.idle_timeout_seconds == 30
    assert execution.checkpoint.interval_seconds == 10
    assert execution.checkpoint.timeout_seconds == 30
    assert execution.restart.delay_seconds == 2
    assert graph.operator("totals").window.time_characteristic == "event"


def test_execution_可显式回退at_least_once() -> None:
    document = event_time_job()
    document["execution"]["delivery_guarantee"] = "at_least_once"

    graph = parse_stream_graph(as_yaml(document))

    assert graph.definition.execution.delivery_guarantee is DeliveryGuarantee.AT_LEAST_ONCE


def test_exactly_once_拒绝无事务能力sink(monkeypatch) -> None:
    monkeypatch.setattr(FileSinkConfig, "supports_exactly_once", False)

    assert_config_error(
        event_time_job(),
        "operators[4].config",
        "不支持 exactly_once",
    )

    compatible = event_time_job()
    compatible["execution"]["delivery_guarantee"] = "at_least_once"
    parse_stream_graph(as_yaml(compatible))


def test_事件时间窗口要求_execution策略和所有source提取器() -> None:
    missing_execution = event_time_job()
    missing_execution.pop("execution")
    assert_config_error(
        missing_execution,
        "execution.event_time",
        "事件时间窗口必须配置",
    )

    missing_extractor = event_time_job()
    missing_extractor["operators"][0]["config"].pop("event_time")
    assert_config_error(
        missing_extractor,
        "operators[0].config.event_time",
        "必须配置 event_time",
    )


def test_处理时间作业拒绝无用途的事件时间配置() -> None:
    document = event_time_job()
    document["operators"][3]["window"]["time_characteristic"] = "processing"

    assert_config_error(
        document,
        "execution.event_time",
        "只有事件时间窗口作业",
    )


@pytest.mark.parametrize("pointer", ["event_time", "/bad~2escape"])
def test_source_event_time_pointer_必须是合法_rfc6901(pointer: str) -> None:
    document = event_time_job()
    document["operators"][0]["config"]["event_time"]["pointer"] = pointer

    assert_config_error(
        document,
        "operators[0].config.kafka.event_time.pointer",
        "JSON Pointer",
    )


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("execution", "event_time", "max_out_of_orderness"), "-1s"),
        (("execution", "event_time", "idle_timeout"), "0s"),
        (("execution", "checkpoint", "interval"), "0s"),
        (("execution", "checkpoint", "timeout"), "seconds"),
        (("execution", "restart", "delay"), "-1s"),
    ],
)
def test_execution_持续时间边界(path: tuple[str, ...], value: str) -> None:
    document = event_time_job()
    target = document
    for segment in path[:-1]:
        target = target[segment]
    target[path[-1]] = value

    assert_config_error(document, ".".join(path), "持续时间")


def changelog_job() -> dict:
    """返回带二级 retract Reduce 的处理时间作业。"""
    document = linear_job()
    document["operators"][3]["emit_mode"] = "changelog"
    document["operators"].insert(
        4,
        operator("by_count", "key_by", udf="wordcount_udfs:count_key"),
    )
    document["operators"].insert(
        5,
        operator(
            "distribution",
            "reduce",
            udf="wordcount_udfs:add_bucket",
            retract_udf="wordcount_udfs:remove_bucket",
            window={
                "type": "tumbling",
                "time_characteristic": "processing",
                "size": "300s",
            },
        ),
    )
    document["edges"] = [
        {"from": "words", "to": "normalize"},
        {"from": "normalize", "to": "by_word"},
        {"from": "by_word", "to": "totals"},
        {"from": "totals", "to": "by_count"},
        {"from": "by_count", "to": "distribution"},
        {"from": "distribution", "to": "output"},
    ]
    return document


def test_changelog属性传播且消费reduce要求_retract_udf() -> None:
    document = changelog_job()
    graph = parse_stream_graph(as_yaml(document))

    assert graph.data_stream("totals").changelog is True
    assert graph.data_stream("by_count").changelog is True
    assert graph.data_stream("distribution").changelog is False

    document["operators"][5].pop("retract_udf")
    assert_config_error(
        document,
        "operators[5].retract_udf",
        "必须配置 retract_udf",
    )


def test_只消费insert的reduce拒绝无用途_retract_udf() -> None:
    document = linear_job()
    document["operators"][3]["retract_udf"] = "wordcount_udfs:remove"

    assert_config_error(
        document,
        "operators[3].retract_udf",
        "只消费 INSERT",
    )


def test_file_sink_columns_校验_pointer和重复() -> None:
    valid = linear_job()
    valid["operators"][-1]["config"]["columns"] = [
        "/headers/window_end",
        "/payload/count",
    ]
    graph = parse_stream_graph(as_yaml(valid))
    assert graph.operator("output").config.columns == [
        "/headers/window_end",
        "/payload/count",
    ]

    duplicate = linear_job()
    duplicate["operators"][-1]["config"]["columns"] = ["/payload/count", "/payload/count"]
    assert_config_error(duplicate, "operators[4].config.file.columns", "重复")
