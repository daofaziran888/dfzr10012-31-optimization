"""流作业指标采集接口：维护指标采集器，覆盖启动采集、暂停采集、关闭采集等动作。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.alarm_decision import AlarmDecisionService
from app.services.metric import MetricService

router = APIRouter(prefix="/api/metric", tags=["流作业指标采集"])

service = MetricService()
decision_service = AlarmDecisionService()

LIST_FIELDS = ["采集器编号", "作业名称", "指标名称", "采集间隔", "聚合方式", "保留天数", "最近采集", "采集器状态"]
STATUSES = ["草稿", "采集中", "已暂停", "已关闭"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按采集器编号检索"),
    status: str | None = Query(default=None, description="草稿、采集中、已暂停、已关闭"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按采集器编号与状态过滤流作业指标采集列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条指标采集器明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"指标采集器 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条指标采集器，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="指标采集器已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条指标采集器执行启动采集、暂停采集、关闭采集；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出流作业指标采集清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "metric", "total": total, "items": items}


@router.post("/{entry_id}/decision", response_model=ActionResult)
def consume_decision(entry_id: int, payload: EntryPayload | None = None) -> ActionResult:
    """另一个处理入口：从同一份告警判定结果里取当前有效判定，同步更新采集侧记录。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"指标采集器 {entry_id} 不存在或已归档")
    values: dict[str, Any] = dict(payload.values if payload else {})
    metric_name = str(entry.get("指标名称") or "")
    values.setdefault("指标名称", metric_name)
    rule = values.pop("规则编号", None) or decision_service.rule_for_metric(metric_name)
    if rule is None:
        return ActionResult(ok=False, message="该指标尚未关联告警规则，未更新采集侧记录")
    result = decision_service.consume(None, {"规则编号": rule})
    if result["ok"]:
        entry["最近判定"] = result["判定编号"]
        entry["最近判定版本"] = result["指标版本"]
        entry["最近复核结论"] = result["复核结论"]
    return ActionResult(
        ok=bool(result["ok"]),
        message=str(result["message"]),
        entry=result["decision"],
    )
