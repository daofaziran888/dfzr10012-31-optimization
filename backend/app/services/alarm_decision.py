"""流告警判定业务规则：批次校验、复核、静默判断与消费共用一份判定结果。

处理链路：

1. 读取 ALAR 告警规则与 METR 聚合指标批次；
2. 校验批次（规则、指标、版本、数值、条件），通过后写入告警判定数据结果；
3. 判定结果固定保留「指标版本」「复核结论」「上一条有效结果」；
4. 复核、静默判断、消费入口都从 ``alarm_compat`` 读取同一份归一化结果；
5. 同一告警并发重算按规则加锁并幂等复用，只生成一个通知事件；
6. 批次校验失败或复核失败时保留上一条有效判定，不覆盖当前有效结果。
"""
from __future__ import annotations

import math
import re
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

from app.store import store
from app.services import alarm_compat as compat

RULE_MODULE = "alarm"
METRIC_MODULE = "metric"
BATCH_MODULE = "metric_batch"
DECISION_MODULE = "alarm_decision"
EVENT_MODULE = "alarm_event"

# 一次判定事务涉及的内部表，异常回滚时一起恢复。
TRANSACTION_MODULES = (BATCH_MODULE, DECISION_MODULE, EVENT_MODULE, METRIC_MODULE, RULE_MODULE)

BATCH_STATUSES = ["待校验", "已校验", "校验失败"]
DECISION_STATUSES = ["待复核", "已复核", "复核失败", "已消费"]
EVENT_STATUSES = ["待通知", "已通知", "已消费", "已撤回"]

REVIEW_PENDING = "待复核"
REVIEW_DONE = "已复核"
REVIEW_FAILED = "复核失败"
CONSUME_DONE = "已消费"

CONCLUSION_TRIGGERED = "触发告警"
CONCLUSION_NORMAL = "未触发"
CONCLUSION_CONFIRMED = "确认告警"
CONCLUSION_FAILED = "复核失败"

FAILURE_CONCLUSIONS = {"复核失败", "失败", "rejected", "failed", "reject", "fail"}
FALSE_CONCLUSIONS = {"误报", "无告警", "false", "dismiss"}

_DURATION_PATTERN = re.compile(r"(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)?", re.IGNORECASE)
_NUMBER_PATTERN = re.compile(r"-?\d+(?:\.\d+)?")
_OPERATOR_PATTERN = re.compile(
    r"(?:avg[-_ ]?sample|采样值|样本均值|指标值|value)?\s*(>=|<=|==|!=|>|<)\s*(-?\d+(?:\.\d+)?)",
    re.IGNORECASE,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def _parse_time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _to_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
    else:
        text = str(value).strip().replace("%", "").replace(",", "")
        if not text:
            return None
        try:
            number = float(text)
        except ValueError:
            match = _NUMBER_PATTERN.search(text)
            if match is None:
                return None
            number = float(match.group(0))
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _version_rank(value: Any) -> int | None:
    numbers = re.findall(r"\d+", str(value or ""))
    return int(numbers[-1]) if numbers else None


def _parse_duration_seconds(value: Any, default: float = 0.0) -> float:
    if value is None or isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return max(float(value), 0.0)
    match = _DURATION_PATTERN.search(str(value))
    if not match:
        return default
    amount = float(match.group(1))
    unit = (match.group(2) or "s").lower()
    factor = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}[unit]
    return max(amount * factor, 0.0)


def _next_id(module: str) -> int:
    return max((int(row.get("id", 0)) for row in store.rows(module)), default=0) + 1


def _decision_ref(source: Any) -> str:
    value = compat.pick(source, ("判定编号", "decision_id", "decisionId"))
    if value is None and isinstance(source, dict):
        value = source.get("id")
    return str(value).strip() if value is not None else ""


def summarize_decision(row: Any) -> dict[str, Any] | None:
    """给「上一条有效结果」用的精简快照，保证只引用一份结果。"""
    decision = compat.compatible_decision(row)
    if decision is None:
        return None
    return {
        "判定编号": decision["判定编号"],
        "规则编号": decision["规则编号"],
        "指标版本": decision["指标版本"],
        "复核结论": decision["复核结论"],
        "复核状态": decision["复核状态"],
        "是否有效": decision["是否有效"],
        "当前有效": decision["当前有效"],
        "通知事件": decision["通知事件"],
        "复核时间": decision["复核时间"],
    }


class AlarmDecisionService:
    """告警判定入口：重算、复核、静默判断、消费共用一套兼容规则。"""

    _locks: dict[str, threading.RLock] = {}
    _locks_guard = threading.Lock()

    # ------------------------------------------------------------------ 查询
    @classmethod
    def _lock_for(cls, key: str) -> threading.RLock:
        with cls._locks_guard:
            return cls._locks.setdefault(key, threading.RLock())

    @contextmanager
    def _transaction(self, rule_ref: str) -> Iterator[None]:
        """同一告警串行处理；中途异常回滚，保证判定结果只有一份一致快照。"""
        modules = list(TRANSACTION_MODULES)
        with self._lock_for(rule_ref):
            backup = store.snapshot(modules)
            try:
                yield
            except Exception:
                store.restore(backup)
                raise

    def list_decisions(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = []
        for raw in store.rows(DECISION_MODULE):
            decision = compat.public_decision(raw)
            if decision is None:
                continue
            if keyword and keyword not in str(decision.get("判定编号", "")) and keyword not in str(
                decision.get("规则编号", "")
            ):
                continue
            if status and status not in (decision.get("复核状态"), decision.get("复核结论")):
                continue
            rows.append(decision)
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def list_events(self, *, rule: str | None = None) -> list[dict[str, Any]]:
        rows = []
        for row in store.rows(EVENT_MODULE):
            if rule and str(row.get("规则编号")) != str(rule):
                continue
            rows.append(dict(row))
        return rows

    def get_decision(self, decision_ref: Any) -> dict[str, Any] | None:
        raw = self._find_decision(decision_ref)
        return compat.public_decision(raw)

    def rule_for_metric(self, metric_name: Any) -> str | None:
        for row in store.rows(RULE_MODULE):
            if compat.metric_names_match(compat.pick(row, compat.METRIC_NAME_KEYS), metric_name):
                return self._rule_ref(row)
        return None

    # ------------------------------------------------------------------ 入口一：批次重算
    def recompute(self, values: dict[str, Any] | None = None) -> dict[str, Any]:
        """校验新批次并写入告警判定数据结果；并发与重复批次只保留一份通知。"""
        values = dict(values or {})
        rule = self._resolve_rule(values)
        if rule is None:
            return self._result(False, "告警规则不存在或已归档，未写入判定结果")

        rule_ref = self._rule_ref(rule)
        with self._transaction(rule_ref):
            version_hint = self._batch_ref(values)
            existing = self._find_decision_for(rule_ref, version_hint) if version_hint else None

            # 先校验批次：同一版本换了指标也必须在写入前拦下，不能走复用捷径。
            snapshot, failure = self._build_snapshot(rule, values)
            now = _now()
            if failure:
                # 失败批次不能把之前有效的那份结果改成失败，否则「上一条有效结果」会指向自己。
                overwrite = existing if existing is not None and not self._is_reusable(existing, values) else None
                previous = self._effective_decision(rule_ref, exclude=overwrite)
                row = self._write_failed_decision(
                    rule_ref=rule_ref,
                    snapshot=snapshot,
                    failure=failure,
                    previous=previous,
                    now=now,
                    existing=overwrite,
                )
                return self._result(
                    False,
                    f"批次校验失败：{failure}；已保留上一条有效判定，未生成通知",
                    compat.public_decision(row),
                    reused=False,
                )

            version = snapshot["指标版本"]
            existing = self._find_decision_for(rule_ref, version)
            if existing is not None and self._is_reusable(existing, values):
                decision = compat.public_decision(existing)
                return self._result(
                    True,
                    "同一告警同一指标版本已重算，复用已有判定，不重复通知",
                    decision,
                    self._decision_event(decision),
                    notified=False,
                    reused=True,
                )

            previous = self._effective_decision(rule_ref)
            conflict = self._pending_decision(rule_ref, exclude=existing)
            triggered, threshold, operator = self._condition_hit(rule, snapshot)
            silenced, new_deadline, previous_event = self._silence_state(rule, previous, now)
            previous_deadline = compat.pick(previous or {}, compat.SILENCE_KEYS)

            notified = False
            event: dict[str, Any] | None = None
            events: list[str] = []
            carried_events: list[str] = []

            if conflict is not None:
                carried_events = previous_event
                deadline = previous_deadline
                message = (
                    f"同一告警正在复核（{conflict.get('判定编号')}），"
                    f"新批次只记录不重复通知；静默判断沿用 "
                    f"{previous.get('指标版本') if previous else conflict.get('指标版本')} 这一份判定结果"
                )
            elif triggered and silenced:
                carried_events = previous_event
                deadline = previous_deadline
                message = "静默期内不重复通知，沿用上一条有效判定的通知事件"
            elif triggered:
                event = self._ensure_event(rule, snapshot, now)
                notified = bool(event.pop("_notified", False))
                events = [str(event.get("事件编号"))]
                if not notified:
                    carried_events = events
                deadline = new_deadline
                if notified:
                    message = f"新批次已校验，通知事件 {events[0]} 只生成一次"
                else:
                    message = f"新批次已校验，沿用已有通知事件 {events[0]}，不重复生成"
            else:
                deadline = previous_deadline
                message = "新批次已校验，未触发告警"

            row = self._write_valid_decision(
                rule=rule,
                rule_ref=rule_ref,
                snapshot=snapshot,
                conclusion=CONCLUSION_TRIGGERED if triggered else CONCLUSION_NORMAL,
                previous=previous,
                event_ids=events,
                carried_events=carried_events,
                deadline=deadline,
                triggered=triggered,
                conflict=conflict,
                now=now,
                existing=existing,
            )
            self._record_batch(rule_ref, snapshot, now)
            return self._result(
                True,
                message,
                compat.public_decision(row),
                event,
                notified=notified,
                reused=False,
                silence=self.silence_status(rule_ref),
                extra={"阈值": threshold, "比较符": operator},
            )

    # ------------------------------------------------------------------ 入口二：复核
    def review(self, decision_ref: Any, values: dict[str, Any] | None = None) -> dict[str, Any]:
        """复核指定判定；失败时回退并保留上一条有效结果，通知不重复生成。"""
        values = dict(values or {})
        raw = self._find_decision(decision_ref)
        if raw is None:
            return self._result(False, "告警判定结果不存在")

        decision = compat.compatible_decision(raw)
        rule_ref = str(decision.get("规则编号") or "")
        with self._transaction(rule_ref):
            raw = self._find_decision(decision_ref)
            if raw is None:
                return self._result(False, "告警判定结果不存在")
            decision = compat.compatible_decision(raw)
            conclusion = str(
                compat.pick(values, compat.CONCLUSION_KEYS) or CONCLUSION_CONFIRMED
            ).strip()
            force_failure = bool(
                compat.pick(values, ("复核失败", "force_failure", "forceFailure", "failed"), False)
            ) or conclusion in FAILURE_CONCLUSIONS or (
                bool(compat.pick(values, ("failed_batch", "批次失败", "batch_failed"), False))
            )
            reason = compat.pick(values, compat.FAILURE_KEYS)
            now = _now()
            before_review = summarize_decision(raw)

            if force_failure:
                previous = self._previous_valid(rule_ref, exclude=raw)
                # 没有更早的有效判定时，保留本次复核前的状态快照，保证失败结果仍可核对。
                preserved = (
                    summarize_decision(previous)
                    or compat.compatible_decision(raw).get("上一条有效结果")
                    or before_review
                )
                raw["复核结论"] = CONCLUSION_FAILED
                raw["复核状态"] = REVIEW_FAILED
                raw["status"] = REVIEW_FAILED
                raw["是否有效"] = False
                raw["当前有效"] = False
                raw["失败原因"] = str(reason or "复核未通过，保留上一条有效结果")
                raw["复核时间"] = _iso(now)
                raw["pending"] = False
                raw["abnormal"] = True
                raw["上一条有效结果"] = preserved
                self._mark_events(raw, "已撤回")
                if previous is not None:
                    self._promote(rule_ref, previous)
                self._resume_conflict(rule_ref, raw, now)
                return self._result(
                    False,
                    "复核失败，已保留上一条有效结果，未生成重复通知",
                    compat.public_decision(raw),
                    reused=False,
                    silence=self.silence_status(rule_ref),
                )

            previous = self._previous_valid(rule_ref, exclude=raw)
            raw["复核结论"] = conclusion
            raw["复核状态"] = REVIEW_DONE
            raw["status"] = REVIEW_DONE
            raw["是否有效"] = True
            raw["失败原因"] = None
            raw["复核时间"] = _iso(now)
            raw["pending"] = False
            raw["abnormal"] = conclusion in FALSE_CONCLUSIONS
            if raw.get("上一条有效结果") is None and previous is not None:
                raw["上一条有效结果"] = summarize_decision(previous)
            self._promote(rule_ref, raw)
            self._resume_conflict(rule_ref, raw, now)

            if conclusion in FALSE_CONCLUSIONS:
                self._mark_events(raw, "已撤回")
            else:
                self._mark_events(raw, "已通知")

            event = self._decision_event(compat.public_decision(raw))
            return self._result(
                True,
                f"复核完成：{conclusion}；后续静默判断读取同一份判定结果",
                compat.public_decision(raw),
                event,
                reused=False,
                silence=self.silence_status(rule_ref),
            )

    # ------------------------------------------------------------------ 入口三：静默判断
    def silence_status(
        self,
        rule_ref: Any,
        version: Any = None,
        *,
        at: Any = None,
    ) -> dict[str, Any]:
        """静默判断：始终只读取一个统一版本的当前有效判定，不自行拼装快照。"""
        rule = self._resolve_rule(rule_ref)
        if rule is None:
            return {
                "规则编号": str(rule_ref),
                "是否静默": False,
                "静默截止": None,
                "判定编号": None,
                "指标版本": None,
                "复核结论": None,
                "reason": "告警规则不存在",
            }

        canonical = self._rule_ref(rule)
        if version is not None and str(version).strip():
            raw = self._find_decision_for(canonical, str(version).strip())
        else:
            raw = self._effective_decision(canonical)
        decision = compat.compatible_decision(raw)
        now = _parse_time(at) or _now()

        deadline = _parse_time(decision.get("静默截止")) if decision else None
        silenced = bool(deadline and now < deadline)
        pending = self._pending_decision(canonical)
        return {
            "规则编号": canonical,
            "判定编号": decision.get("判定编号") if decision else None,
            "指标版本": decision.get("指标版本") if decision else None,
            "复核结论": decision.get("复核结论") if decision else None,
            "复核状态": decision.get("复核状态") if decision else None,
            "是否静默": silenced,
            "静默截止": decision.get("静默截止") if decision else None,
            "通知事件": decision.get("通知事件") if decision else [],
            "待复核判定": pending.get("判定编号") if pending else None,
            "统一版本": decision.get("指标版本") if decision else None,
        }

    # ------------------------------------------------------------------ 入口四：消费
    def consume(
        self,
        decision_ref: Any = None,
        values: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """消费入口：读取同一份判定结果，更新判定、事件、批次与规则记录。"""
        values = dict(values or {})
        raw = self._find_decision(decision_ref) if decision_ref is not None else None
        if raw is None:
            rule = self._resolve_rule(values)
            if rule is None:
                return self._result(False, "消费入口未找到对应的告警判定结果")
            rule_ref = self._rule_ref(rule)
            version = self._batch_ref(values)
            raw = self._find_decision_for(rule_ref, version) if version else self._effective_decision(rule_ref)
        if raw is None:
            return self._result(False, "消费入口未找到对应的告警判定结果")

        decision = compat.compatible_decision(raw)
        rule_ref = str(decision.get("规则编号") or "")
        with self._transaction(rule_ref):
            raw = self._locate_raw(decision)
            if raw is None:
                return self._result(False, "消费入口未找到对应的告警判定结果")
            decision = compat.compatible_decision(raw)
            if not decision.get("是否有效"):
                return self._result(
                    False,
                    "判定结果无效，保留上一条有效结果，消费侧未更新记录",
                    compat.public_decision(raw),
                )

            now = _now()
            updated: list[dict[str, Any]] = []
            for event_id in decision.get("通知事件") or []:
                event = self._find_event(str(event_id))
                if event is None:
                    continue
                event["消费状态"] = CONSUME_DONE
                event["status"] = CONSUME_DONE
                event["pending"] = False
                event["消费时间"] = _iso(now)
                updated.append({"事件编号": event.get("事件编号"), "消费状态": event.get("消费状态")})

            batch = self._find_batch(decision.get("指标版本"), rule_ref)
            if batch is not None:
                batch["消费状态"] = CONSUME_DONE
                batch["判定编号"] = decision.get("判定编号")
                batch["消费时间"] = _iso(now)
                updated.append({"批次编号": batch.get("批次编号"), "消费状态": CONSUME_DONE})

            rule = self._resolve_rule(rule_ref)
            if rule is not None:
                rule["最近判定"] = decision.get("判定编号")
                rule["最近判定版本"] = decision.get("指标版本")
                rule["最近消费时间"] = _iso(now)

            raw["消费状态"] = CONSUME_DONE
            raw["status"] = CONSUME_DONE
            raw["pending"] = False
            raw["消费时间"] = _iso(now)
            raw["关联记录"] = updated
            self._touch_collector(
                decision.get("指标快照") or {},
                now,
                valid=True,
                decision=compat.public_decision(raw),
            )
            return self._result(
                True,
                "判定结果已消费，消费侧同步更新对应记录",
                compat.public_decision(raw),
                updated=updated,
                silence=self.silence_status(rule_ref),
            )

    # ------------------------------------------------------------------ 内部：定位
    def _resolve_rule(self, source: Any) -> dict[str, Any] | None:
        if isinstance(source, dict):
            ref = compat.pick(source, compat.RULE_ID_KEYS)
            if ref is None:
                ref = source.get("id", source.get("entry_id"))
        else:
            ref = source
        if ref is None:
            return None
        text = str(ref).strip()
        if not text:
            return None
        for row in store.rows(RULE_MODULE):
            if str(row.get("规则编号")) == text:
                return row
        if text.lstrip("-").isdigit():
            return store.find(RULE_MODULE, int(text))
        for row in store.rows(RULE_MODULE):
            if text in str(row.get("规则编号", "")):
                return row
        return None

    def _rule_ref(self, rule: dict[str, Any]) -> str:
        return str(rule.get("规则编号") or f"alarm-{rule.get('id')}")

    def _batch_ref(self, values: dict[str, Any]) -> str:
        value = compat.pick(
            values,
            ("批次编号", "batch_id", "batchId", "batch", "指标版本", "metric_version", "metricVersion", "version"),
        )
        return str(value).strip() if value is not None and str(value).strip() else ""

    def _find_decision(self, decision_ref: Any) -> dict[str, Any] | None:
        if isinstance(decision_ref, dict):
            ref = _decision_ref(decision_ref)
            if not ref:
                rule = self._resolve_rule(decision_ref)
                if rule is None:
                    return None
                return self._effective_decision(self._rule_ref(rule))
        else:
            ref = str(decision_ref or "").strip()
        if not ref or ref in ("None", "null"):
            return None
        for row in store.rows(DECISION_MODULE):
            if str(compat.pick(row, ("判定编号", "decision_id", "decisionId"))) == ref:
                return row
        if ref.lstrip("-").isdigit():
            found = store.find(DECISION_MODULE, int(ref))
            if found is not None:
                return found
        rule = self._resolve_rule(ref)
        if rule is not None:
            return self._effective_decision(self._rule_ref(rule))
        return None

    def _locate_raw(self, decision: dict[str, Any]) -> dict[str, Any] | None:
        ref = decision.get("判定编号")
        for row in store.rows(DECISION_MODULE):
            if str(compat.pick(row, ("判定编号", "decision_id", "decisionId"))) == str(ref):
                return row
        return None

    def _find_decision_for(self, rule_ref: str, version: Any) -> dict[str, Any] | None:
        target = str(version or "").strip()
        if not target:
            return None
        found: list[dict[str, Any]] = []
        for row in store.rows(DECISION_MODULE):
            decision = compat.compatible_decision(row)
            if decision is None:
                continue
            if str(decision.get("规则编号")) != rule_ref:
                continue
            if str(decision.get("指标版本")) == target:
                found.append(row)
        return found[-1] if found else None

    def _find_event(self, event_id: str) -> dict[str, Any] | None:
        for row in store.rows(EVENT_MODULE):
            if str(row.get("事件编号")) == str(event_id):
                return row
        return None

    def _find_batch(self, batch_ref: Any, rule_ref: str | None = None) -> dict[str, Any] | None:
        target = str(batch_ref or "").strip()
        if not target:
            return None
        candidates = []
        for row in store.rows(BATCH_MODULE):
            version = compat.pick(row, compat.VERSION_KEYS)
            if str(version) == target or str(row.get("批次编号")) == target:
                candidates.append(row)
        if not candidates:
            return None
        if rule_ref:
            for row in candidates:
                if str(compat.pick(row, compat.RULE_ID_KEYS)) == rule_ref:
                    return row
        return candidates[-1]

    def _effective_decision(
        self, rule_ref: Any, *, exclude: Any = None
    ) -> dict[str, Any] | None:
        canonical = str(rule_ref or "")
        rule = self._resolve_rule(canonical) if canonical else None
        if rule is not None:
            canonical = self._rule_ref(rule)
        rows: list[dict[str, Any]] = []
        for raw in store.rows(DECISION_MODULE):
            if exclude is not None and raw is exclude:
                continue
            decision = compat.compatible_decision(raw)
            if decision is None:
                continue
            if str(decision.get("规则编号")) != canonical:
                continue
            rows.append(raw)
        if not rows:
            return None

        current = [row for row in rows if compat.compatible_decision(row).get("当前有效")]
        if current:
            return sorted(current, key=lambda row: int(row.get("id", 0)))[-1]
        valid = [row for row in rows if compat.compatible_decision(row).get("是否有效")]
        if valid:
            return sorted(valid, key=lambda row: int(row.get("id", 0)))[-1]
        # 只有失败结果时不当作有效判定，避免把失败批次再当成「上一条有效结果」。
        return None

    def _previous_valid(self, rule_ref: str, *, exclude: Any = None) -> dict[str, Any] | None:
        rows = [
            row
            for row in store.rows(DECISION_MODULE)
            if row is not exclude
            and str(compat.compatible_decision(row).get("规则编号")) == str(rule_ref)
        ]
        current = [
            row for row in rows if compat.compatible_decision(row).get("当前有效")
        ]
        if current:
            return sorted(current, key=lambda row: int(row.get("id", 0)))[-1]
        exclude_id = int(exclude.get("id", 0)) if isinstance(exclude, dict) else None
        valid = [
            row
            for row in rows
            if compat.compatible_decision(row).get("是否有效")
            and (exclude_id is None or int(row.get("id", 0)) < exclude_id)
        ]
        if not valid:
            return None
        return sorted(valid, key=lambda row: int(row.get("id", 0)))[-1]

    def _pending_decision(self, rule_ref: str, *, exclude: Any = None) -> dict[str, Any] | None:
        pending = []
        for raw in store.rows(DECISION_MODULE):
            if exclude is not None and raw is exclude:
                continue
            decision = compat.compatible_decision(raw)
            if decision is None:
                continue
            if str(decision.get("规则编号")) != str(rule_ref):
                continue
            if decision.get("复核状态") == REVIEW_PENDING and decision.get("当前有效"):
                pending.append(raw)
        if not pending:
            return None
        return compat.compatible_decision(sorted(pending, key=lambda row: int(row.get("id", 0)))[-1])

    def _resume_conflict(
        self, rule_ref: str, reviewed: dict[str, Any], now: datetime
    ) -> None:
        """复核结束后，让之前因复核冲突而挂起的新批次转为唯一有效判定。

        新批次已经记录过指标快照，这里只切换「哪一份结果被静默判断采用」，
        不重新生成通知，避免同一告警在复核收尾时再通知一次。
        """
        for raw in sorted(store.rows(DECISION_MODULE), key=lambda row: int(row.get("id", 0))):
            if raw is reviewed:
                continue
            decision = compat.compatible_decision(raw)
            if decision is None or str(decision.get("规则编号")) != str(rule_ref):
                continue
            if decision.get("冲突判定") != reviewed.get("判定编号"):
                continue
            raw["冲突判定"] = None
            raw["复核状态"] = REVIEW_PENDING if decision.get("复核结论") == CONCLUSION_TRIGGERED else REVIEW_DONE
            raw["status"] = raw["复核状态"]
            raw["复核时间"] = _iso(now)
            raw["静默截止"] = compat.pick(reviewed, compat.SILENCE_KEYS) or decision.get("静默截止")
            self._promote(rule_ref, raw)
            break

    def _promote(self, rule_ref: str, target: dict[str, Any]) -> None:
        for raw in store.rows(DECISION_MODULE):
            decision = compat.compatible_decision(raw)
            if decision is None or str(decision.get("规则编号")) != str(rule_ref):
                continue
            raw["当前有效"] = raw is target
        target["当前有效"] = True

    def _mark_events(self, raw: dict[str, Any], status: str) -> None:
        decision = compat.compatible_decision(raw)
        if decision is None:
            return
        for event_id in decision.get("通知事件") or []:
            event = self._find_event(str(event_id))
            if event is None:
                continue
            event["通知状态"] = status
            event["status"] = status
            event["pending"] = status == "待通知"
            event["abnormal"] = status == "已撤回"

    # ------------------------------------------------------------------ 内部：校验
    def _build_snapshot(
        self, rule: dict[str, Any], values: dict[str, Any]
    ) -> tuple[dict[str, Any], str | None]:
        batch_ref = self._batch_ref(values)
        rule_ref = self._rule_ref(rule)
        stored = self._find_batch(batch_ref, rule_ref) if batch_ref else None
        if stored is None and batch_ref:
            stored = self._find_batch(batch_ref)

        merged: dict[str, Any] = dict(stored or {})
        for key in (
            compat.VALUE_KEYS
            + compat.METRIC_NAME_KEYS
            + compat.COLLECTOR_KEYS
            + compat.AGGREGATION_KEYS
            + compat.SAMPLE_COUNT_KEYS
            + compat.COLLECTED_AT_KEYS
        ):
            if key in values and values[key] is not None:
                merged[key] = values[key]

        if not batch_ref:
            batch_ref = str(compat.pick(merged, compat.VERSION_KEYS) or "")
        if not batch_ref:
            batch_ref = f"batch-{_next_id(BATCH_MODULE)}"

        metric_name = compat.pick(merged, compat.METRIC_NAME_KEYS) or compat.pick(
            rule, compat.METRIC_NAME_KEYS
        )
        collector = compat.pick(merged, compat.COLLECTOR_KEYS)
        if not collector:
            collector = self._collector_for(metric_name)
        value = _to_number(compat.pick(merged, compat.VALUE_KEYS))
        version = str(compat.pick(merged, compat.VERSION_KEYS) or batch_ref)

        snapshot = compat.compatible_metric_row(merged) or {}
        snapshot.update(
            {
                "批次编号": batch_ref,
                "指标版本": version,
                "规则编号": rule_ref,
                "采集器编号": collector,
                "指标名称": metric_name,
                "聚合方式": snapshot.get("聚合方式") or "avg",
                "avg-sample": value,
                "采样值": value,
                "样本数": compat.pick(merged, compat.SAMPLE_COUNT_KEYS),
                "采集时间": compat.pick(merged, compat.COLLECTED_AT_KEYS),
                "原始指标": merged,
            }
        )

        status = str(rule.get("status") or rule.get("规则状态") or "")
        if status == "已关闭":
            return snapshot, "告警规则已关闭，不能重算"

        rule_metric = compat.pick(rule, compat.METRIC_NAME_KEYS)
        if metric_name and rule_metric and not compat.metric_names_match(rule_metric, metric_name):
            return snapshot, f"监控指标不匹配：规则为 {rule_metric}，批次为 {metric_name}"

        if value is None:
            return snapshot, "批次缺少可用的数值指标（avg-sample）"

        stored_status = str(stored.get("status") or "") if stored else ""
        explicit_value = any(key in values and values[key] is not None for key in compat.VALUE_KEYS)
        if stored_status == "校验失败" and not explicit_value:
            return snapshot, "批次已标记为校验失败"

        current = self._effective_decision(rule_ref)
        current_version = compat.pick(current or {}, compat.VERSION_KEYS)
        current_rank = _version_rank(current_version)
        new_rank = _version_rank(version)
        if current_rank is not None and new_rank is not None and new_rank < current_rank:
            return snapshot, f"指标版本回退：{version} 早于当前有效版本 {current_version}"

        return snapshot, None

    def _collector_for(self, metric_name: Any) -> str | None:
        if not metric_name:
            return None
        for row in store.rows(METRIC_MODULE):
            if compat.metric_names_match(compat.pick(row, compat.METRIC_NAME_KEYS), metric_name):
                return compat.pick(row, compat.COLLECTOR_KEYS)
        return None

    def _condition_hit(
        self, rule: dict[str, Any], snapshot: dict[str, Any]
    ) -> tuple[bool, float | None, str | None]:
        value = _to_number(compat.pick(snapshot, compat.VALUE_KEYS))
        text = str(rule.get("告警条件") or "").strip()
        operator = None
        threshold = None

        match = _OPERATOR_PATTERN.search(text)
        if match:
            operator, threshold = match.group(1), _to_number(match.group(2))
        else:
            number = _NUMBER_PATTERN.search(text)
            if number:
                operator, threshold = ">", _to_number(number.group(0))
            else:
                threshold = _to_number(
                    compat.pick(rule, ("阈值", "threshold", "告警阈值"))
                )
                operator = ">" if threshold is not None else None

        if value is None:
            return False, threshold, operator
        if threshold is None or operator is None:
            return value > 0, threshold, operator

        comparisons = {
            ">": value > threshold,
            ">=": value >= threshold,
            "<": value < threshold,
            "<=": value <= threshold,
            "==": value == threshold,
            "!=": value != threshold,
        }
        return bool(comparisons.get(operator, False)), threshold, operator

    # ------------------------------------------------------------------ 内部：写入
    def _decision_id_for(self, rule_ref: str, snapshot: dict[str, Any], row: dict[str, Any]) -> str:
        """判定编号按版本生成；同版本发生二次判定时补序号，保证每条结果可单独读取。"""
        if row.get("判定编号"):
            return str(row["判定编号"])
        base = f"decision-{rule_ref}-{snapshot.get('指标版本') or _next_id(DECISION_MODULE)}"
        taken = {
            str(compat.pick(other, ("判定编号", "decision_id", "decisionId")))
            for other in store.rows(DECISION_MODULE)
            if other is not row
        }
        if base not in taken:
            return base
        index = 2
        while f"{base}-{index}" in taken:
            index += 1
        return f"{base}-{index}"

    def _write_failed_decision(
        self,
        *,
        rule_ref: str,
        snapshot: dict[str, Any],
        failure: str,
        previous: dict[str, Any] | None,
        now: datetime,
        existing: dict[str, Any] | None,
    ) -> dict[str, Any]:
        row = existing if existing is not None else self._new_decision_row()
        row.update(
            {
                "status": REVIEW_FAILED,
                "pending": False,
                "abnormal": True,
                "判定编号": self._decision_id_for(rule_ref, snapshot, row),
                "规则编号": rule_ref,
                "指标版本": snapshot.get("指标版本"),
                "批次编号": snapshot.get("批次编号"),
                "复核结论": CONCLUSION_FAILED,
                "复核状态": REVIEW_FAILED,
                "上一条有效结果": summarize_decision(previous),
                "指标快照": snapshot,
                "通知事件": [],
                "沿用事件": summarize_decision(previous).get("通知事件") if previous else [],
                "静默截止": compat.pick(previous or {}, compat.SILENCE_KEYS),
                "是否有效": False,
                "当前有效": False,
                "失败原因": failure,
                "复核时间": _iso(now),
                "消费状态": "未消费",
                "重算次数": int(row.get("重算次数", 0)) + 1,
            }
        )
        if existing is None:
            row["id"] = _next_id(DECISION_MODULE)
            store.rows(DECISION_MODULE).append(row)
        self._mark_batch_failed(rule_ref, snapshot)
        return row

    def _mark_batch_failed(self, rule_ref: str, snapshot: dict[str, Any]) -> None:
        """校验失败的批次只记失败状态，不覆盖判定结果里的上一条有效快照。"""
        version = snapshot.get("指标版本")
        batch_ref = snapshot.get("批次编号")
        batch = self._find_batch(version, rule_ref) or self._find_batch(batch_ref, rule_ref)
        if batch is None:
            if not (batch_ref or version):
                return
            batch = {
                "id": _next_id(BATCH_MODULE),
                "批次编号": batch_ref or version,
                "指标版本": version or batch_ref,
                "规则编号": rule_ref,
            }
            store.rows(BATCH_MODULE).append(batch)
        batch.update(
            {
                "status": "校验失败",
                "pending": False,
                "abnormal": True,
                "批次状态": "校验失败",
                "规则编号": rule_ref,
                "指标快照": snapshot,
                "avg-sample": snapshot.get("avg-sample"),
                "采样值": snapshot.get("采样值"),
                "消费状态": "未消费",
            }
        )
        self._touch_collector(snapshot, _now(), valid=False)

    def _write_valid_decision(
        self,
        *,
        rule: dict[str, Any],
        rule_ref: str,
        snapshot: dict[str, Any],
        conclusion: str,
        previous: dict[str, Any] | None,
        event_ids: list[str],
        carried_events: list[str],
        deadline: str | None,
        triggered: bool,
        conflict: dict[str, Any] | None,
        now: datetime,
        existing: dict[str, Any] | None,
    ) -> dict[str, Any]:
        row = existing if existing is not None else self._new_decision_row()
        if existing is None:
            row["id"] = _next_id(DECISION_MODULE)

        previous_summary = summarize_decision(previous)
        pending = bool(triggered and conflict is None)
        if conflict is not None:
            pending = True

        row.update(
            {
                "status": REVIEW_PENDING if pending else REVIEW_DONE,
                "pending": pending,
                "abnormal": False,
                "判定编号": self._decision_id_for(rule_ref, snapshot, row),
                "规则编号": rule_ref,
                "指标版本": snapshot.get("指标版本"),
                "批次编号": snapshot.get("批次编号"),
                "复核结论": conclusion,
                "复核状态": REVIEW_PENDING if pending else REVIEW_DONE,
                "上一条有效结果": previous_summary,
                "指标快照": snapshot,
                "通知事件": event_ids,
                "沿用事件": carried_events,
                "静默截止": deadline,
                "是否有效": True,
                "当前有效": conflict is None,
                "失败原因": None,
                "复核时间": _iso(now) if not pending else None,
                "消费状态": "未消费",
                "冲突判定": conflict.get("判定编号") if conflict else None,
                "重算次数": int(row.get("重算次数", 0)) + 1,
            }
        )
        if existing is None:
            store.rows(DECISION_MODULE).append(row)
        if conflict is None:
            self._promote(rule_ref, row)
        return row

    def _decision_stub(self) -> dict[str, Any]:
        """判定结果的统一骨架：四个写入路径都从这里起步，字段只写一次。"""
        return {
            "id": 0,
            "判定编号": None,
            "规则编号": None,
            "指标版本": None,
            "批次编号": None,
            "复核结论": None,
            "复核状态": REVIEW_PENDING,
            "上一条有效结果": None,
            "指标快照": {},
            "通知事件": [],
            "沿用事件": [],
            "静默截止": None,
            "是否有效": True,
            "当前有效": False,
            "失败原因": None,
            "复核时间": None,
            "消费状态": "未消费",
            "冲突判定": None,
            "重算次数": 0,
        }

    def _new_decision_row(self) -> dict[str, Any]:
        return self._decision_stub()

    def _record_batch(
        self, rule_ref: str, snapshot: dict[str, Any], now: datetime
    ) -> None:
        """把校验通过的批次落库，后续消费入口按版本找到同一份指标快照。"""
        batch_ref = snapshot.get("批次编号")
        version = snapshot.get("指标版本")
        batch = self._find_batch(version, rule_ref) or self._find_batch(batch_ref, rule_ref)
        if batch is None:
            batch = {
                "id": _next_id(BATCH_MODULE),
                "批次编号": batch_ref or version,
                "指标版本": version,
                "规则编号": rule_ref,
            }
            store.rows(BATCH_MODULE).append(batch)
        batch.update(
            {
                "status": "已校验",
                "pending": False,
                "abnormal": False,
                "批次状态": "已校验",
                "规则编号": rule_ref,
                "指标快照": snapshot,
                "avg-sample": snapshot.get("avg-sample"),
                "采样值": snapshot.get("采样值"),
                "校验时间": _iso(now),
                "消费状态": batch.get("消费状态", "未消费"),
            }
        )
        self._touch_collector(snapshot, now, valid=True)

    def _touch_collector(
        self,
        snapshot: dict[str, Any],
        now: datetime,
        *,
        valid: bool,
        decision: dict[str, Any] | None = None,
    ) -> None:
        """把批次结果回写到 METR 聚合指标记录，沿用存量字段映射。"""
        collector = compat.pick(snapshot, compat.COLLECTOR_KEYS)
        metric_name = compat.pick(snapshot, compat.METRIC_NAME_KEYS)
        target = None
        for row in store.rows(METRIC_MODULE):
            if collector and str(compat.pick(row, compat.COLLECTOR_KEYS)) == str(collector):
                target = row
                break
            if not collector and metric_name and compat.metric_names_match(
                compat.pick(row, compat.METRIC_NAME_KEYS), metric_name
            ):
                target = row
                break
        if target is None:
            return
        target["最近采集"] = _iso(now)
        target["最新批次"] = snapshot.get("批次编号")
        target["最新指标版本"] = snapshot.get("指标版本")
        target["最新均值"] = snapshot.get("avg-sample")
        target["批次校验"] = "通过" if valid else "失败"
        if decision is not None:
            target["最近判定"] = decision.get("判定编号")
            target["最近判定版本"] = decision.get("指标版本")
            target["最近复核结论"] = decision.get("复核结论")
            target["最近判定消费"] = decision.get("消费状态")

    def _ensure_event(
        self, rule: dict[str, Any], snapshot: dict[str, Any], now: datetime
    ) -> dict[str, Any]:
        event_id = self._event_id(rule)
        event = self._find_event(event_id)
        created = False
        if event is None:
            event = {
                "id": _next_id(EVENT_MODULE),
                "status": "待通知",
                "pending": True,
                "abnormal": False,
                "事件编号": event_id,
                "规则编号": self._rule_ref(rule),
                "指标版本": snapshot.get("指标版本"),
                "指标快照": snapshot,
                "通知渠道": rule.get("通知渠道"),
                "通知次数": 1,
                "首次通知时间": _iso(now),
                "最后通知时间": _iso(now),
                "消费状态": "未消费",
            }
            store.rows(EVENT_MODULE).append(event)
            created = True
        else:
            event["指标版本"] = snapshot.get("指标版本")
            event["指标快照"] = snapshot
            event["最后通知时间"] = _iso(now)
            # 同一告警同一版本只保留一个通知事件，重算只记复用次数。
            event["重算复用次数"] = int(event.get("重算复用次数", 0)) + 1

        event["通知状态"] = "已通知"
        event["status"] = "已通知"
        event["pending"] = False
        event["消费状态"] = event.get("消费状态", "未消费")
        payload = dict(event)
        payload["_notified"] = created
        return payload

    def _event_id(self, rule: dict[str, Any]) -> str:
        rule_ref = self._rule_ref(rule)
        suffix = re.search(r"(\d+)$", rule_ref)
        if suffix:
            return f"event-n{suffix.group(1)}"
        return f"event-n{rule.get('id')}"

    def _decision_event(self, decision: dict[str, Any] | None) -> dict[str, Any] | None:
        if not decision:
            return None
        for event_id in decision.get("通知事件") or []:
            event = self._find_event(str(event_id))
            if event is not None:
                return dict(event)
        return None

    def _silence_state(
        self, rule: dict[str, Any], previous: dict[str, Any] | None, now: datetime
    ) -> tuple[bool, str | None, list[str]]:
        """静默判断只认上一条有效判定的静默截止，不再按批次单独计算。"""
        deadline_text = compat.pick(previous or {}, compat.SILENCE_KEYS)
        deadline = _parse_time(deadline_text)
        previous_events = [str(item) for item in (previous or {}).get("通知事件") or []]
        if deadline and now < deadline:
            return True, deadline_text, previous_events
        seconds = _parse_duration_seconds(rule.get("静默周期"))
        return False, _iso(now + timedelta(seconds=seconds)), previous_events

    # ------------------------------------------------------------------ 内部：结果
    def _is_reusable(self, raw: dict[str, Any], values: dict[str, Any] | None = None) -> bool:
        decision = compat.compatible_decision(raw)
        if decision is None:
            return False
        if decision.get("复核状态") == REVIEW_FAILED:
            return False
        reused = bool(decision.get("是否有效")) or decision.get("复核状态") == REVIEW_PENDING
        if not reused:
            return False
        # 同一批次重发但采样值被改动时按新结果重算，仍然不新增通知事件。
        incoming = _to_number(compat.pick(values or {}, compat.VALUE_KEYS))
        snapshot_value = _to_number(compat.pick(decision.get("指标快照") or {}, compat.VALUE_KEYS))
        if incoming is not None and snapshot_value is not None and incoming != snapshot_value:
            return False
        return True

    def _result(
        self,
        ok: bool,
        message: str,
        decision: dict[str, Any] | None = None,
        event: dict[str, Any] | None = None,
        *,
        notified: bool = False,
        reused: bool = False,
        updated: list[dict[str, Any]] | None = None,
        silence: dict[str, Any] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """可复用的处理结果：四个入口返回同一种结构，兼容字段只在这里收口。"""
        result: dict[str, Any] = {
            "ok": ok,
            "message": message,
            "decision": decision,
            "event": event,
            "notified": notified,
            "reused": reused,
            "updated": updated or [],
            "silence": silence,
            "判定编号": decision.get("判定编号") if decision else None,
            "指标版本": decision.get("指标版本") if decision else None,
            "复核结论": decision.get("复核结论") if decision else None,
            "上一条有效结果": decision.get("上一条有效结果") if decision else None,
        }
        if extra:
            result.update(extra)
        # 兼容旧读取方：处理结果同时暴露下划线别名。
        result.update(
            {
                "decision_id": result["判定编号"],
                "metric_version": result["指标版本"],
                "review_conclusion": result["复核结论"],
                "previous_valid_result": result["上一条有效结果"],
            }
        )
        return result
