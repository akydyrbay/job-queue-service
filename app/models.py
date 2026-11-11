from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    cancelled = "cancelled"


class JobCreate(BaseModel):
    idempotency_key: str = Field(min_length=1, max_length=255)
    payload: dict[str, Any] = Field(default_factory=dict)


class Job(BaseModel):
    id: UUID
    idempotency_key: str
    payload: dict[str, Any]
    status: JobStatus
    worker_id: str | None
    lease_expires_at: datetime | None
    attempts: int
    result_output: str | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime
    finished_at: datetime | None


class JobPage(BaseModel):
    items: list[Job]
    page: int
    limit: int
    total: int
