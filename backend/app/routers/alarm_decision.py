"""流告警判定接口：批次重算、复核、静默判断与消费四个入口共用一份判定结果。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.alarm_decision import AlarmDecisionService
from app.store import store

router = APIRouter(prefix="/api/alarm", tags=["流告警判定"])

service = AlarmDecisionService()


def _fail(message: str) -> ActionResult:
    return ActionResult(ok=False, message=message)


@router.get("/decisions", response_model=PageResult[dict])
def list_decisions(
    keyword: str | None = Query(default=None, description="按判定编号或规则编号检索"),
    status: str | None = Query(default=None, description="待复核、已复核、复核失败、已消费"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """读取告警判定数据结果列表；每条都带指标版本、复核结论与上一条有效结果。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_decisions(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/decisions/{decision_ref}", response_model=dict)
def get_decision(decision_ref: str) -> dict:
    """读取单条判定结果；找不到时给出可读说明。"""
    decision = service.get_decision(decision_ref)
    if decision is None:
        raise HTTPException(status_code=404, detail=f"告警判定结果 {decision_ref} 不存在")
    return decision


@router.post("/decisions/recompute", response_model=ActionResult)
def recompute(payload: EntryPayload) -> ActionResult:
    """校验新指标批次并写入告警判定结果；同一告警并发重算只生成一次通知。"""
    result = service.recompute(payload.values)
    return ActionResult(
        ok=bool(result["ok"]),
        message=str(result["message"]),
        entry=result["decision"],
    )


@router.post("/decisions/{decision_ref}/review", response_model=ActionResult)
def review(decision_ref: str, payload: EntryPayload) -> ActionResult:
    """复核判定结果；复核失败时保留上一条有效结果，不重复通知。"""
    result = service.review(decision_ref, payload.values)
    return ActionResult(
        ok=bool(result["ok"]),
        message=str(result["message"]),
        entry=result["decision"],
    )


@router.post("/decisions/{decision_ref}/consume", response_model=ActionResult)
def consume_decision(decision_ref: str, payload: EntryPayload | None = None) -> ActionResult:
    """消费入口：读取同一份判定结果，并同步更新事件、批次与规则记录。"""
    result = service.consume(decision_ref, (payload.values if payload else {}))
    return ActionResult(
        ok=bool(result["ok"]),
        message=str(result["message"]),
        entry=result["decision"],
    )


@router.post("/consume", response_model=ActionResult)
def consume(payload: EntryPayload) -> ActionResult:
    """另一个处理入口：按规则编号或批次编号读取同一份判定结果后消费。"""
    result = service.consume(None, payload.values)
    return ActionResult(
        ok=bool(result["ok"]),
        message=str(result["message"]),
        entry=result["decision"],
    )


@router.get("/silence", response_model=dict)
def silence(
    rule: str = Query(description="告警规则编号"),
    version: str | None = Query(default=None, description="可选，指定指标版本"),
) -> dict:
    """静默判断：只读取一份统一版本的当前有效判定结果。"""
    return service.silence_status(rule, version)


@router.get("/events", response_model=PageResult[dict])
def list_events(
    rule: str | None = Query(default=None, description="按规则编号检索通知事件"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """查看通知事件，用于核对同一告警是否只生成了一次通知。"""
    items = service.list_events(rule=rule)
    total = len(items)
    start = max(page - 1, 0) * size
    return PageResult(items=items[start:start + size], total=total, page=page, size=size)


@router.get("/batches", response_model=PageResult[dict])
def list_batches(
    rule: str | None = Query(default=None, description="按规则编号检索批次"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """查看已校验的指标批次及其消费状态。"""
    items = [
        dict(row)
        for row in store.rows("metric_batch")
        if not rule or str(row.get("规则编号")) == rule
    ]
    total = len(items)
    start = max(page - 1, 0) * size
    return PageResult(items=items[start:start + size], total=total, page=page, size=size)


@router.post("/batches", response_model=ActionResult)
def ingest_batch(payload: EntryPayload) -> ActionResult:
    """登记一个聚合指标批次；真正的校验在重算入口完成。"""
    values: dict[str, Any] = dict(payload.values)
    if not values:
        return _fail("批次内容为空，未写入告警判定结果")
    result = service.recompute(values)
    return ActionResult(
        ok=bool(result["ok"]),
        message=str(result["message"]),
        entry=result["decision"],
    )
