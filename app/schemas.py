from __future__ import annotations

import datetime as dt
from typing import Optional, Sequence

from pydantic import BaseModel, Field

from app.models import UploadStatus, UserRole


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"


class LoginForm(BaseModel):
    email: str
    password: str


class UserRead(BaseModel):
    id: int
    email: str
    full_name: str
    role: UserRole


class UploadRead(BaseModel):
    id: int
    original_filename: str
    status: UploadStatus
    row_count: int
    pricing_pretax_total: float
    billing_pretax_total: float
    created_at: dt.datetime
    completed_at: Optional[dt.datetime]
    error_message: Optional[str]


class UploadSummary(BaseModel):
    total_pricing: float
    total_billing: float
    total_records: int
    forex_multiplier: float
    margin: float
    vat_rate: float


class ChartSeries(BaseModel):
    label: str
    value: float


class TimelinePoint(BaseModel):
    date: dt.date
    value: float


class DataPage(BaseModel):
    records: Sequence[dict]
    total: int


class PricingStrategyForm(BaseModel):
    name: str
    description: Optional[str] = None
    markup_percent: float = Field(default=0.0, ge=-100.0, le=500.0)
    discount_percent: float = Field(default=0.0, ge=0.0, le=100.0)

