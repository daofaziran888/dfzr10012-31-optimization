"""告警判定链路的验收用例：只用标准库 unittest，不依赖额外测试包。

运行：cd backend && .venv/bin/python -m unittest discover -s tests -v
"""
from __future__ import annotations

import threading
import unittest

from app.services.alarm_decision import AlarmDecisionService
from app.services.metric import MetricService
from app.store import store

RULE = "rule-window-latency-31"


class AlarmDecisionTest(unittest.TestCase):
    def setUp(self) -> None:
        store.reset()
        self.service = AlarmDecisionService()

    def batch(self, ref: str, value: float, **extra) -> dict:
        values = {"规则编号": RULE, "批次编号": ref, "avg-sample": value}
        values.update(extra)
        return values

    def test_acceptance_batch_31_only_one_event(self) -> None:
        result = self.service.recompute(self.batch("batch-31", 860))

        self.assertTrue(result["ok"])
        self.assertTrue(result["notified"])
        decision = result["decision"]
        self.assertEqual(decision["指标版本"], "batch-31")
        self.assertEqual(decision["复核结论"], "触发告警")
        self.assertIn("上一条有效结果", decision)
        self.assertEqual([event["事件编号"] for event in self.service.list_events(rule=RULE)], ["event-n31"])

    def test_repeated_and_concurrent_recompute_keeps_single_notification(self) -> None:
        results = []

        def run() -> None:
            results.append(self.service.recompute(self.batch("batch-31", 860)))

        threads = [threading.Thread(target=run) for _ in range(16)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(len(results), 16)
        self.assertEqual(sum(1 for item in results if item["notified"]), 1)
        events = self.service.list_events(rule=RULE)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["通知次数"], 1)
        self.assertEqual(len(store.rows("alarm_decision")), 1)

    def test_failed_review_keeps_previous_valid_result(self) -> None:
        first = self.service.recompute(self.batch("batch-31", 860))
        decision_id = first["decision"]["判定编号"]
        self.service.review(decision_id, {"复核结论": "确认告警"})

        failed = self.service.review(decision_id, {"复核结论": "复核失败"})

        self.assertFalse(failed["ok"])
        decision = failed["decision"]
        self.assertEqual(decision["指标版本"], "batch-31")
        self.assertEqual(decision["复核结论"], "复核失败")
        self.assertEqual(decision["上一条有效结果"]["判定编号"], decision_id)
        self.assertEqual(
            [event["事件编号"] for event in self.service.list_events(rule=RULE)], ["event-n31"]
        )

    def test_failed_review_falls_back_to_earlier_valid_decision(self) -> None:
        old = self.service.recompute(self.batch("batch-30", 700))
        self.service.review(old["decision"]["判定编号"], {"复核结论": "未触发"})
        new = self.service.recompute(self.batch("batch-31", 860))

        self.service.review(new["decision"]["判定编号"], {"复核结论": "复核失败"})

        silence = self.service.silence_status(RULE)
        self.assertEqual(silence["指标版本"], "batch-30")
        self.assertEqual(silence["复核结论"], "未触发")
        consumed = self.service.consume(None, {"规则编号": RULE})
        self.assertTrue(consumed["ok"])
        self.assertEqual(consumed["metric_version"], "batch-30")

    def test_conflicting_batch_during_review_does_not_notify_again(self) -> None:
        first = self.service.recompute(self.batch("batch-30", 900))
        self.assertTrue(first["notified"])

        conflict = self.service.recompute(self.batch("batch-31", 860))

        self.assertTrue(conflict["ok"])
        self.assertFalse(conflict["notified"])
        self.assertEqual(conflict["decision"]["冲突判定"], first["decision"]["判定编号"])
        self.assertEqual(len(self.service.list_events(rule=RULE)), 1)
        self.assertEqual(self.service.silence_status(RULE)["指标版本"], "batch-30")

        # 复核结束后，静默判断只采用一个统一版本的新判定，通知不重复生成。
        self.service.review(first["decision"]["判定编号"], {"复核结论": "误报"})
        silence = self.service.silence_status(RULE)
        self.assertEqual(silence["指标版本"], "batch-31")
        self.assertEqual(silence["判定编号"], conflict["decision"]["判定编号"])
        self.assertEqual(len(self.service.list_events(rule=RULE)), 1)

    def test_consume_from_metric_entry_updates_records(self) -> None:
        self.service.recompute(self.batch("batch-31", 860))
        collector = MetricService().get_entry(4)

        result = self.service.consume(
            None, {"规则编号": self.service.rule_for_metric("window-latency")}
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["指标版本"], "batch-31")
        self.assertIn({"事件编号": "event-n31", "消费状态": "已消费"}, result["updated"])
        self.assertEqual(collector["最近判定"], result["判定编号"])
        self.assertEqual(collector["最近判定消费"], "已消费")
        self.assertEqual(store.rows("alarm_event")[0]["消费状态"], "已消费")
        self.assertEqual(store.rows("metric_batch")[0]["消费状态"], "已消费")

    def test_metric_decision_route_entry_shares_the_same_decision(self) -> None:
        from app.routers.metric import consume_decision

        self.service.recompute(self.batch("batch-31", 860))
        result = consume_decision(4, None)

        self.assertTrue(result.ok)
        entry = MetricService().get_entry(4)
        self.assertEqual(entry["最近判定"], "decision-rule-window-latency-31-batch-31")
        self.assertEqual(entry["最近复核结论"], "触发告警")

    def test_invalid_batch_keeps_previous_valid_and_skips_notification(self) -> None:
        good = self.service.recompute(self.batch("batch-31", 860))
        self.service.review(good["decision"]["判定编号"], {"复核结论": "确认告警"})

        bad = self.service.recompute({"规则编号": RULE, "批次编号": "batch-32"})

        self.assertFalse(bad["ok"])
        self.assertIn("保留上一条有效判定", bad["message"])
        self.assertIsNotNone(bad["decision"]["上一条有效结果"])
        self.assertEqual(bad["decision"]["上一条有效结果"]["指标版本"], "batch-31")
        self.assertEqual(self.service.silence_status(RULE)["指标版本"], "batch-31")
        self.assertEqual(len(self.service.list_events(rule=RULE)), 1)

    def test_stale_metric_version_is_rejected(self) -> None:
        self.service.recompute(self.batch("batch-31", 860))
        stale = self.service.recompute(self.batch("batch-30", 900))

        self.assertFalse(stale["ok"])
        self.assertIn("指标版本回退", stale["message"])
        self.assertEqual(self.service.silence_status(RULE)["指标版本"], "batch-31")

    def test_changed_sample_recomputes_without_new_event(self) -> None:
        first = self.service.recompute(self.batch("batch-31", 860))
        self.assertTrue(first["notified"])

        reused = self.service.recompute(self.batch("batch-31", 860))
        self.assertTrue(reused["reused"])

        self.service.review(first["decision"]["判定编号"], {"复核结论": "确认告警"})
        changed = self.service.recompute(self.batch("batch-31", 999))

        self.assertFalse(changed["reused"])
        self.assertEqual(changed["decision"]["指标快照"]["avg-sample"], 999.0)
        self.assertEqual(len(self.service.list_events(rule=RULE)), 1)

    def test_failed_batch_retry_recovers_single_notification(self) -> None:
        failed = self.service.recompute({"规则编号": RULE, "批次编号": "batch-31"})
        self.assertFalse(failed["ok"])

        retried = self.service.recompute(self.batch("batch-31", 860))

        self.assertTrue(retried["ok"])
        self.assertTrue(retried["notified"])
        self.assertEqual(retried["decision"]["复核结论"], "触发告警")
        self.assertEqual(self.service.get_decision("decision-rule-window-latency-31-batch-31")["复核状态"], "待复核")
        self.assertEqual(len(store.rows("alarm_decision")), 1)
        self.assertEqual(len(self.service.list_events(rule=RULE)), 1)

    def test_same_version_with_conflicting_metric_keeps_previous_valid(self) -> None:
        good = self.service.recompute(self.batch("batch-31", 860))
        self.service.review(good["decision"]["判定编号"], {"复核结论": "确认告警"})

        bad = self.service.recompute(
            {"规则编号": RULE, "批次编号": "batch-31", "指标名称": "another-metric", "avg-sample": 860}
        )

        self.assertFalse(bad["ok"])
        failed_id = bad["decision"]["判定编号"]
        self.assertNotEqual(failed_id, good["decision"]["判定编号"])
        self.assertEqual(
            bad["decision"]["上一条有效结果"]["判定编号"], good["decision"]["判定编号"]
        )
        self.assertEqual(self.service.get_decision(failed_id)["复核结论"], "复核失败")
        self.assertEqual(self.service.get_decision(good["decision"]["判定编号"])["复核状态"], "已复核")
        self.assertEqual(self.service.silence_status(RULE)["判定编号"], good["decision"]["判定编号"])

    def test_compat_reads_legacy_alias_fields(self) -> None:
        from app.services.alarm_compat import compatible_decision, public_decision

        legacy = {
            "decision_id": "decision-legacy",
            "rule_id": RULE,
            "metric_version": "batch-31",
            "review_conclusion": "触发告警",
            "previous_valid_result": {"metric_version": "batch-30"},
            "events": ["event-n31"],
            "silence_until": "2026-01-01T00:10:00+00:00",
        }
        decision = compatible_decision(legacy)
        self.assertEqual(decision["指标版本"], "batch-31")
        self.assertEqual(decision["复核结论"], "触发告警")
        self.assertEqual(decision["上一条有效结果"]["metric_version"], "batch-30")
        self.assertEqual(public_decision(legacy)["metric_version"], "batch-31")

    def test_review_exception_rolls_back_to_single_consistent_result(self) -> None:
        from unittest.mock import patch

        first = self.service.recompute(self.batch("batch-31", 860))

        def boom(raw, status):
            raise RuntimeError("复核写回失败")

        with patch.object(self.service, "_mark_events", side_effect=boom):
            with self.assertRaises(RuntimeError):
                self.service.review(first["decision"]["判定编号"], {"复核结论": "确认告警"})

        silence = self.service.silence_status(RULE)
        self.assertEqual(silence["指标版本"], "batch-31")
        self.assertEqual(silence["复核状态"], "待复核")
        self.assertEqual(self.service.get_decision(first["decision"]["判定编号"])["复核状态"], "待复核")
        self.assertEqual(
            [event["通知状态"] for event in self.service.list_events(rule=RULE)], ["已通知"]
        )

        retried = self.service.review(first["decision"]["判定编号"], {"复核结论": "确认告警"})
        self.assertTrue(retried["ok"])
        self.assertEqual(retried["decision"]["复核结论"], "确认告警")

    def test_batch_validation_failure_does_not_overwrite_current_decision(self) -> None:
        good = self.service.recompute(self.batch("batch-31", 860))
        self.service.review(good["decision"]["判定编号"], {"复核结论": "确认告警"})

        bad = self.service.recompute(
            {"规则编号": RULE, "批次编号": "batch-32", "指标名称": "another-metric", "avg-sample": 900}
        )

        self.assertFalse(bad["ok"])
        self.assertEqual(bad["decision"]["复核结论"], "复核失败")
        self.assertEqual(bad["decision"]["指标版本"], "batch-32")
        self.assertFalse(bad["decision"]["是否有效"])
        # 当前有效判定保持不变，静默判断仍读 batch-31。
        current = self.service.silence_status(RULE)
        self.assertEqual(current["指标版本"], "batch-31")
        self.assertEqual(current["复核结论"], "确认告警")
        self.assertEqual(len(self.service.list_events(rule=RULE)), 1)

    def test_disabled_rule_rejects_new_batch(self) -> None:
        store.find("alarm", 4)["status"] = "已关闭"

        result = self.service.recompute(self.batch("batch-31", 860))

        self.assertFalse(result["ok"])
        self.assertIn("已关闭", result["message"])
        self.assertEqual(self.service.list_events(rule=RULE), [])


if __name__ == "__main__":
    unittest.main()
