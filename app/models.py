from __future__ import annotations

import datetime as dt
import enum
from typing import Optional

from sqlmodel import SQLModel, Field


class UserRole(str, enum.Enum):
    admin = "admin"
    analyst = "analyst"


class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    email: str = Field(index=True, unique=True)
    full_name: str
    role: UserRole = Field(default=UserRole.analyst, index=True)
    password_hash: str
    is_active: bool = Field(default=True)
    created_at: dt.datetime = Field(default_factory=lambda: dt.datetime.utcnow())


class UploadStatus(str, enum.Enum):
    pending = "pending"
    processing = "processing"
    completed = "completed"
    failed = "failed"


class Upload(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    original_filename: str
    stored_path: str
    user_id: int = Field(foreign_key="user.id")
    status: UploadStatus = Field(default=UploadStatus.pending)
    row_count: int = Field(default=0)
    pricing_pretax_total: float = Field(default=0.0)
    billing_pretax_total: float = Field(default=0.0)
    created_at: dt.datetime = Field(default_factory=lambda: dt.datetime.utcnow())
    completed_at: Optional[dt.datetime] = None
    error_message: Optional[str] = None


class PricingStrategy(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    description: Optional[str] = None
    markup_percent: float = Field(default=0.0)
    discount_percent: float = Field(default=0.0)
    created_at: dt.datetime = Field(default_factory=lambda: dt.datetime.utcnow())

