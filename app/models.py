"""Request/response schemas for the HTTP API."""
from __future__ import annotations

import math
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator


class RecordIn(BaseModel):
    seq: int = Field(ge=0, description="Monotonically increasing per-device sequence number")
    payload: dict[str, Any] = Field(default_factory=dict, description="Telemetry payload")


class BatchIn(BaseModel):
    device_id: str = Field(min_length=1, max_length=128)
    batch_id: Optional[str] = Field(
        default=None,
        description="Optional client-side batch identifier (echoed back; retries are "
        "safe because every sequence is idempotent on its own)",
    )
    records: list[RecordIn] = Field(min_length=1, max_length=10_000)


class RecordResult(BaseModel):
    seq: int
    status: str  # committed | buffered | duplicate | gap_filled | out_of_window | stale


class WindowSummary(BaseModel):
    next_expected: int
    buffered_count: int


class BatchResponse(BaseModel):
    device_id: str
    batch_id: Optional[str]
    accepted: int
    results: list[RecordResult]
    window: WindowSummary


class RecordOut(BaseModel):
    seq: int
    payload: dict[str, Any]
    status: str  # committed | buffered | gap_filled
    received_at: float


class RecordPage(BaseModel):
    device_id: str
    records: list[RecordOut]
    # Opaque, HMAC-signed keyset cursor; null when this is the last page.
    next_cursor: Optional[str] = None


class RuleCreate(BaseModel):
    field: str = Field(
        min_length=1, max_length=128,
        description="payload 顶层数值字段名（仅顶层，不支持嵌套路径）",
    )
    op: Literal["gt", "lt"] = Field(description="比较方向：gt 大于阈值 / lt 小于阈值")
    threshold: float = Field(description="告警阈值")
    consecutive_count: int = Field(
        ge=1, le=1_000_000,
        description="连续违反阈值的 committed 记录条数，首次达到即生成一条告警",
    )

    @field_validator("threshold")
    @classmethod
    def _finite_threshold(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("threshold must be a finite number")
        return value

    @field_validator("field")
    @classmethod
    def _top_level_field(cls, value: str) -> str:
        # Only top-level keys are supported: reject dotted/pointer-looking
        # paths rather than silently treating the literal string as a key.
        if "." in value:
            raise ValueError("field must be a top-level payload key (dots are not allowed)")
        return value


class RuleOut(BaseModel):
    rule_id: int
    device_id: str
    field: str
    op: Literal["gt", "lt"]
    threshold: float
    consecutive_count: int
    created_at: float


class AlertOut(BaseModel):
    alert_id: int
    device_id: str
    rule_id: int
    field: str
    op: Literal["gt", "lt"]
    threshold: float
    consecutive_count: int
    trigger_seq: int = Field(description="首次达到连续条数的那条记录的 seq")
    run_start_seq: int = Field(description="本次连续违反区段起始记录的 seq")
    value: float = Field(description="触发记录中被监视字段的值")
    created_at: float


class RuleList(BaseModel):
    rules: list[RuleOut]


class DeviceRuleList(BaseModel):
    device_id: str
    rules: list[RuleOut]


class AlertList(BaseModel):
    alerts: list[AlertOut]


class DeviceAlertList(BaseModel):
    device_id: str
    alerts: list[AlertOut]
