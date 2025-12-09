from pydantic import BaseModel
from typing import Any
from datetime import datetime
from enum import Enum


class PipelineRequest(BaseModel):
    yaml_config: str
    dataset_url: str | None = None
    source_version_id: str | None = None
    project_id: str | None = None
    subset: str | None = None
    namespace: str | None = None
    dataset_env: str | None = None

class PipelineConfigRequest(BaseModel):
    namespace: str
    name: str
    config: str
    input_path: str
    output_path: str
    sink: str | None = None
    metadata: dict[str, Any] | None = None
    pipeline_id: str | None = None
    version: str | None = None
    owner: str | None = None

class TaskStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class OptimizeResult(BaseModel):
    task_id: str
    status: TaskStatus
    should_optimize: str | None = None
    input_data: list[dict[str, Any]] | None = None
    output_data: list[dict[str, Any]] | None = None
    cost: float | None = None
    error: str | None = None
    created_at: datetime
    completed_at: datetime | None = None

class OptimizeRequest(BaseModel):
    yaml_config: str
    step_name: str
    op_name: str
