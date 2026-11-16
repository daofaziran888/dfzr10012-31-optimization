"""告警判定结果的兼容读取规则。

复核、静默判断和消费入口都从这里读取同一份判定结果：旧字段名、旧批次别名和
判定结论的归一化只写一次，避免三个入口各写一遍兼容分支。
"""
from __future__ import annotations

from typing import Any, Iterable

RULE_ID_KEYS = ("规则编号", "rule_id", "ruleId", "rule")
VERSION_KEYS = (
    "指标版本",
    "metric_version",
    "metricVersion",
    "批次编号",
    "batch_id",
    "batchId",
    "batch",
    "版本",
)
CONCLUSION_KEYS = (
    "复核结论",
    "review_conclusion",
    "reviewConclusion",
    "review",
    "conclusion",
    "结论",
)
PREVIOUS_KEYS = (
    "上一条有效结果",
    "previous_valid_result",
    "previousValidResult",
    "previous",
    "last_valid",
    "上一条",
)
SNAPSHOT_KEYS = ("指标快照", "metric_snapshot", "metricSnapshot", "snapshot", "快照")
EVENT_KEYS = ("通知事件", "events", "event_ids", "event_id", "event")
SILENCE_KEYS = ("静默截止", "silence_until", "silenced_until", "silenceUntil")
VALID_KEYS = ("是否有效", "is_valid", "valid", "有效")
CURRENT_KEYS = ("当前有效", "is_current", "current")
FAILURE_KEYS = ("失败原因", "failure_reason", "failureReason", "reason")
CONSUME_KEYS = ("消费状态", "consume_status", "consumeStatus")
REVIEW_TIME_KEYS = ("复核时间", "reviewed_at", "reviewedAt", "updated_at")

VALUE_KEYS = (
    "avg-sample",
    "avg_sample",
    "avgSample",
    "采样值",
    "样本均值",
    "指标值",
    "value",
    "数值",
)
METRIC_NAME_KEYS = ("指标名称", "metric_name", "metricName", "监控指标", "指标")
COLLECTOR_KEYS = ("采集器编号", "collector_id", "collectorId", "采集器")
AGGREGATION_KEYS = ("聚合方式", "aggregation", "agg")
SAMPLE_COUNT_KEYS = ("样本数", "sample_count", "sampleCount", "count")
COLLECTED_AT_KEYS = ("采集时间", "collected_at", "collectedAt", "时间")


def pick(source: Any, keys: Iterable[str], default: Any = None) -> Any:
    """按别名顺序取第一个非空字段，兼容新旧两种结果结构。"""
    if not isinstance(source, dict):
        return default
    for key in keys:
        if key in source and source[key] is not None:
            return source[key]
    return default


def normalize_events(value: Any) -> list[str]:
    if value is None or value == "":
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if item not in (None, "")]
    return [str(value)]


def compatible_decision(row: Any) -> dict[str, Any] | None:
    """把一条判定结果归一成统一结构；三个入口只认这一种写法。"""
    if not isinstance(row, dict):
        return None

    version = pick(row, VERSION_KEYS)
    conclusion = pick(row, CONCLUSION_KEYS)
    previous = pick(row, PREVIOUS_KEYS)
    snapshot = pick(row, SNAPSHOT_KEYS)
    events = normalize_events(pick(row, EVENT_KEYS))

    is_valid = pick(row, VALID_KEYS, True)
    is_current = pick(row, CURRENT_KEYS, False)

    return {
        "判定编号": pick(row, ("判定编号", "decision_id", "decisionId", "id")),
        "规则编号": pick(row, RULE_ID_KEYS),
        "指标版本": version,
        "复核结论": conclusion,
        "上一条有效结果": previous,
        "指标快照": snapshot if isinstance(snapshot, dict) else {},
        "通知事件": events,
        "静默截止": pick(row, SILENCE_KEYS),
        "复核状态": pick(row, ("复核状态", "review_status", "reviewStatus")),
        "是否有效": bool(is_valid),
        "当前有效": bool(is_current),
        "失败原因": pick(row, FAILURE_KEYS),
        "消费状态": pick(row, CONSUME_KEYS),
        "复核时间": pick(row, REVIEW_TIME_KEYS),
        "冲突判定": pick(row, ("冲突判定", "conflict_decision", "conflictDecision")),
        "原始结果": row,
    }


def public_decision(row: Any) -> dict[str, Any] | None:
    """对外的判定结果：统一字段名 + 兼容别名，复核/静默/消费读到的完全一致。"""
    decision = compatible_decision(row)
    if decision is None:
        return None
    result = {
        "判定编号": decision["判定编号"],
        "规则编号": decision["规则编号"],
        "指标版本": decision["指标版本"],
        "批次编号": pick(row, ("批次编号", "batch_id", "batchId", "batch"), decision["指标版本"]),
        "复核结论": decision["复核结论"],
        "复核状态": decision["复核状态"],
        "判定状态": pick(row, ("status", "判定状态"), decision["复核状态"]),
        "上一条有效结果": decision["上一条有效结果"],
        "指标快照": decision["指标快照"],
        "通知事件": list(decision["通知事件"]),
        "沿用事件": normalize_events(pick(row, ("沿用事件", "carried_events", "carriedEvents"))),
        "静默截止": decision["静默截止"],
        "是否有效": decision["是否有效"],
        "当前有效": decision["当前有效"],
        "失败原因": decision["失败原因"],
        "消费状态": decision["消费状态"],
        "复核时间": decision["复核时间"],
        "冲突判定": pick(row, ("冲突判定", "conflict_decision", "conflictDecision")),
        "重算次数": int(pick(row, ("重算次数", "recompute_count", "recomputeCount"), 0) or 0),
    }
    # 兼容旧读取方：同一份结果同时暴露下划线写法与原始记录引用。
    result.update(
        {
            "decision_id": decision["判定编号"],
            "rule_id": decision["规则编号"],
            "metric_version": decision["指标版本"],
            "review_conclusion": decision["复核结论"],
            "previous_valid_result": decision["上一条有效结果"],
            "metric_snapshot": decision["指标快照"],
            "events": list(decision["通知事件"]),
            "silence_until": decision["静默截止"],
            "is_valid": decision["是否有效"],
            "is_current": decision["当前有效"],
        }
    )
    return result


def compatible_metric_row(row: Any) -> dict[str, Any] | None:
    """把老批次字段名和新批次字段名收口成一份指标快照。"""
    if not isinstance(row, dict):
        return None

    value = pick(row, VALUE_KEYS)
    version = pick(row, VERSION_KEYS)
    return {
        "指标版本": version,
        "批次编号": pick(row, ("批次编号", "batch_id", "batchId", "batch"), version),
        "规则编号": pick(row, RULE_ID_KEYS),
        "采集器编号": pick(row, COLLECTOR_KEYS),
        "指标名称": pick(row, METRIC_NAME_KEYS),
        "聚合方式": pick(row, AGGREGATION_KEYS),
        "avg-sample": value,
        "采样值": value,
        "样本数": pick(row, SAMPLE_COUNT_KEYS),
        "采集时间": pick(row, COLLECTED_AT_KEYS),
        "原始指标": row,
    }


def _normalize_metric_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    for prefix in ("metric:", "metrics/", "指标："):
        if text.startswith(prefix):
            text = text[len(prefix):]
    return "".join(ch for ch in text if ch.isalnum())


def metric_names_match(left: Any, right: Any) -> bool:
    """告警规则里的监控指标与批次里的指标名称是否为同一个指标。"""
    left_text = _normalize_metric_name(left)
    right_text = _normalize_metric_name(right)
    if not left_text or not right_text:
        return False
    return left_text == right_text
