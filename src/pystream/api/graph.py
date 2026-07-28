"""逻辑 DAG、DataStream 属性传播和边分区决策。"""

from __future__ import annotations

import heapq
from collections import defaultdict
from dataclasses import dataclass

from pystream.api.errors import ConfigIssue, JobConfigError
from pystream.api.models import (
    EdgeSpec,
    JobDefinition,
    OperatorSpec,
    OperatorType,
    Partitioning,
)


@dataclass(frozen=True, slots=True)
class DataStream:
    """一个逻辑算子的输出流及其 key/分区属性。"""

    operator_id: str
    keyed: bool
    partitioning: Partitioning | None


@dataclass(frozen=True, slots=True)
class StreamEdge:
    """带确定分区策略的规范化逻辑边。"""

    source_id: str
    target_id: str
    partitioning: Partitioning


class StreamGraph:
    """经过完整关系校验且可确定性遍历的逻辑执行图。"""

    def __init__(self, definition: JobDefinition) -> None:
        self.definition = definition
        self._operators = self._index_operators(definition.operators)
        self._validate_edges(definition.edges)
        self._topological_order = self._sort_topologically()
        self._streams = self._propagate_stream_properties()
        self._edges = tuple(self._normalize_edge(edge) for edge in definition.edges)

    @staticmethod
    def _index_operators(operators: list[OperatorSpec]) -> dict[str, OperatorSpec]:
        indexed: dict[str, OperatorSpec] = {}
        first_position: dict[str, int] = {}
        issues: list[ConfigIssue] = []
        for position, operator in enumerate(operators):
            if operator.id in indexed:
                issues.append(
                    ConfigIssue(
                        f"operators[{position}].id",
                        f"算子 ID {operator.id!r} 重复, 首次出现在 operators"
                        f"[{first_position[operator.id]}].id",
                    )
                )
            else:
                indexed[operator.id] = operator
                first_position[operator.id] = position
        if issues:
            raise JobConfigError(issues)
        return indexed

    def _validate_edges(self, edges: list[EdgeSpec]) -> None:
        issues: list[ConfigIssue] = []
        seen: dict[tuple[str, str], int] = {}
        incoming: dict[str, list[int]] = defaultdict(list)
        outgoing: dict[str, list[int]] = defaultdict(list)

        for position, edge in enumerate(edges):
            pair = (edge.from_, edge.to)
            if pair in seen:
                issues.append(
                    ConfigIssue(
                        f"edges[{position}]",
                        f"重复边 {edge.from_!r} -> {edge.to!r}, 首次出现在 edges[{seen[pair]}]",
                    )
                )
            else:
                seen[pair] = position

            if edge.from_ not in self._operators:
                issues.append(
                    ConfigIssue(
                        f"edges[{position}].from",
                        f"引用了不存在的算子 {edge.from_!r}",
                    )
                )
            if edge.to not in self._operators:
                issues.append(
                    ConfigIssue(
                        f"edges[{position}].to",
                        f"引用了不存在的算子 {edge.to!r}",
                    )
                )
            if edge.from_ == edge.to:
                issues.append(ConfigIssue(f"edges[{position}]", "不允许算子连接到自身"))

            if edge.from_ in self._operators and edge.to in self._operators:
                outgoing[edge.from_].append(position)
                incoming[edge.to].append(position)

        has_missing_endpoint = any(
            edge.from_ not in self._operators or edge.to not in self._operators for edge in edges
        )
        if has_missing_endpoint and issues:
            raise JobConfigError(issues)

        source_count = 0
        sink_count = 0
        for position, operator in enumerate(self.definition.operators):
            if operator.type is OperatorType.SOURCE:
                source_count += 1
                if incoming[operator.id]:
                    issues.append(
                        ConfigIssue(
                            f"operators[{position}].type",
                            "source 算子的入度必须为 0",
                        )
                    )
            elif not incoming[operator.id]:
                issues.append(
                    ConfigIssue(
                        f"operators[{position}].id",
                        f"{operator.type.value} 算子必须至少有一个上游",
                    )
                )

            if operator.type is OperatorType.SINK:
                sink_count += 1
                if outgoing[operator.id]:
                    issues.append(
                        ConfigIssue(
                            f"operators[{position}].type",
                            "sink 算子的出度必须为 0",
                        )
                    )
            elif not outgoing[operator.id]:
                issues.append(
                    ConfigIssue(
                        f"operators[{position}].id",
                        f"{operator.type.value} 算子必须至少有一个下游",
                    )
                )

        if source_count == 0:
            issues.append(ConfigIssue("operators", "作业必须至少包含一个 source 算子"))
        if sink_count == 0:
            issues.append(ConfigIssue("operators", "作业必须至少包含一个 sink 算子"))
        if issues:
            raise JobConfigError(issues)

    def _sort_topologically(self) -> tuple[str, ...]:
        declaration_order = {
            operator.id: position for position, operator in enumerate(self.definition.operators)
        }
        indegree = {operator_id: 0 for operator_id in self._operators}
        outgoing: dict[str, list[str]] = defaultdict(list)
        for edge in self.definition.edges:
            indegree[edge.to] += 1
            outgoing[edge.from_].append(edge.to)

        ready = [
            (declaration_order[operator_id], operator_id)
            for operator_id, degree in indegree.items()
            if degree == 0
        ]
        heapq.heapify(ready)
        ordered: list[str] = []
        while ready:
            _, operator_id = heapq.heappop(ready)
            ordered.append(operator_id)
            for target_id in sorted(outgoing[operator_id], key=declaration_order.__getitem__):
                indegree[target_id] -= 1
                if indegree[target_id] == 0:
                    heapq.heappush(ready, (declaration_order[target_id], target_id))

        if len(ordered) != len(self._operators):
            cycle_nodes = [
                operator_id for operator_id in self._operators if indegree[operator_id] > 0
            ]
            raise JobConfigError(
                [
                    ConfigIssue(
                        "edges",
                        "作业图必须是 DAG, 检测到环涉及: " + ", ".join(cycle_nodes),
                    )
                ]
            )
        return tuple(ordered)

    def _propagate_stream_properties(self) -> dict[str, DataStream]:
        upstream_ids: dict[str, list[str]] = defaultdict(list)
        for edge in self.definition.edges:
            upstream_ids[edge.to].append(edge.from_)

        streams: dict[str, DataStream] = {}
        issues: list[ConfigIssue] = []
        positions = {
            operator.id: position for position, operator in enumerate(self.definition.operators)
        }
        for operator_id in self._topological_order:
            operator = self._operators[operator_id]
            input_streams = [streams[source_id] for source_id in upstream_ids[operator_id]]

            if operator.type is OperatorType.SOURCE:
                keyed = False
            elif operator.type is OperatorType.KEY_BY:
                keyed = True
            else:
                keyed = bool(input_streams) and all(stream.keyed for stream in input_streams)

            if operator.type is OperatorType.REDUCE and not keyed:
                issues.append(
                    ConfigIssue(
                        f"operators[{positions[operator_id]}].type",
                        "reduce 算子只能消费 keyed stream; 请在所有上游路径中先使用 key_by",
                    )
                )

            streams[operator_id] = DataStream(
                operator_id=operator_id,
                keyed=keyed,
                partitioning=Partitioning.HASH if keyed else None,
            )

        if issues:
            raise JobConfigError(issues)
        return streams

    def _normalize_edge(self, edge: EdgeSpec) -> StreamEdge:
        source = self._operators[edge.from_]
        target = self._operators[edge.to]
        stream = self._streams[edge.from_]
        if stream.keyed:
            partitioning = Partitioning.HASH
        elif source.parallelism == target.parallelism:
            partitioning = Partitioning.FORWARD
        else:
            partitioning = Partitioning.REBALANCE
        return StreamEdge(
            source_id=edge.from_,
            target_id=edge.to,
            partitioning=partitioning,
        )

    @property
    def operators(self) -> tuple[OperatorSpec, ...]:
        """按 YAML 声明顺序返回算子。"""
        return tuple(self.definition.operators)

    @property
    def edges(self) -> tuple[StreamEdge, ...]:
        """按 YAML 声明顺序返回规范化边。"""
        return self._edges

    @property
    def topological_order(self) -> tuple[str, ...]:
        """返回稳定的拓扑顺序。"""
        return self._topological_order

    @property
    def total_parallelism(self) -> int:
        """返回后续调度器需要展开的物理任务总数。"""
        return sum(operator.parallelism for operator in self.definition.operators)

    def operator(self, operator_id: str) -> OperatorSpec:
        """按 ID 获取算子；未知 ID 保留标准 KeyError 语义。"""
        return self._operators[operator_id]

    def data_stream(self, operator_id: str) -> DataStream:
        """获取算子输出的 DataStream 属性。"""
        return self._streams[operator_id]

    def incoming_edges(self, operator_id: str) -> tuple[StreamEdge, ...]:
        """返回指定算子的所有入边。"""
        return tuple(edge for edge in self._edges if edge.target_id == operator_id)

    def outgoing_edges(self, operator_id: str) -> tuple[StreamEdge, ...]:
        """返回指定算子的所有出边。"""
        return tuple(edge for edge in self._edges if edge.source_id == operator_id)


def build_stream_graph(definition: JobDefinition) -> StreamGraph:
    """校验跨算子关系并构建规范化逻辑图。"""
    return StreamGraph(definition)


__all__ = ["DataStream", "StreamEdge", "StreamGraph", "build_stream_graph"]
