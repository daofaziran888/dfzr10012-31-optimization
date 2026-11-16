"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
"""
from __future__ import annotations

import copy
from typing import Any, Iterable

from app.seed import SEED_ROWS

# 告警判定链路的内部表只服务判定结果，不计入运营概览的业务模块统计。
INTERNAL_MODULES = {"metric_batch", "alarm_decision", "alarm_event"}


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }

    def module_names(self) -> list[str]:
        return sorted(name for name in self._tables if name not in INTERNAL_MODULES)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def reset(self) -> None:
        """恢复到种子数据，测试和演示脚本用，避免上一次写入影响下一次核对。"""
        self._tables = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def snapshot(self, modules: Iterable[str]) -> dict[str, list[dict[str, Any]]]:
        """给一次判定事务留底，中途抛异常时回滚到上一份一致结果。"""
        return {
            name: [copy.deepcopy(row) for row in self.rows(name)]
            for name in modules
        }

    def restore(self, snapshot: dict[str, list[dict[str, Any]]]) -> None:
        for name, rows in snapshot.items():
            self._tables[name] = rows

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()
