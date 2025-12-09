from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from utils.errors.error_types import ConfigurationError
from utils.storage import StorageConfig, build_storage_client


def _home_dir() -> Path:
    return Path(os.getenv("DOCETL_HOME_DIR", os.path.expanduser("~")))


@dataclass
class PipelineSaveResult:
    path: str
    uri: str | None = None
    manifest: dict[str, Any] | None = field(default_factory=dict)
    input_path: str | None = None
    output_path: str | None = None


class PipelineSink(ABC):
    """Abstract pipeline persistence sink."""

    @abstractmethod
    async def save(
        self,
        *,
        namespace: str,
        name: str,
        yaml_str: str,
        metadata: dict[str, Any] | None = None,
    ) -> PipelineSaveResult: ...


class LocalPipelineSink(PipelineSink):
    """Persist pipeline YAML to the local ~/.docetl namespace."""

    def __init__(self, home_dir: Path | None = None) -> None:
        self.home_dir = home_dir or _home_dir()

    async def save(
        self,
        *,
        namespace: str,
        name: str,
        yaml_str: str,
        metadata: dict[str, Any] | None = None,
    ) -> PipelineSaveResult:
        metadata = metadata or {}
        pipeline_dir = self.home_dir / ".docetl" / namespace / "pipelines"
        config_dir = pipeline_dir / "configs"
        name_dir = pipeline_dir / name / "intermediates"

        config_dir.mkdir(parents=True, exist_ok=True)
        name_dir.mkdir(parents=True, exist_ok=True)

        file_path = config_dir / f"{name}.yaml"
        file_path.write_text(yaml_str)

        manifest = {
            "pipeline_id": metadata.get("pipeline_id") or name,
            "version": metadata.get("version") or "local",
            "owner": metadata.get("owner"),
            "sink": "local",
        }

        return PipelineSaveResult(
            path=str(file_path),
            manifest=manifest,
            input_path=metadata.get("input_path"),
            output_path=metadata.get("output_path"),
        )


class RTPipelineSink(PipelineSink):
    """Persist pipeline YAML to RT spec storage (specs/docetl/<pipeline>/<version>.yaml)."""

    def __init__(self, *, storage_client=None) -> None:
        self._storage_client = storage_client

    def _ensure_storage(self):
        if self._storage_client:
            return self._storage_client
        try:
            cfg = StorageConfig()
        except Exception as exc:  # pragma: no cover - defensive guard
            raise HTTPException(
                status_code=500, detail=f"Invalid storage configuration: {exc}"
            ) from exc
        if not cfg.enabled:
            raise HTTPException(
                status_code=500,
                detail="Storage client disabled; set STORAGE__* to enable RT pipeline sink.",
            )
        try:
            self._storage_client = build_storage_client(cfg, monitoring_profile=None)
        except ConfigurationError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return self._storage_client

    async def save(
        self,
        *,
        namespace: str,
        name: str,
        yaml_str: str,
        metadata: dict[str, Any] | None = None,
    ) -> PipelineSaveResult:
        metadata = metadata or {}
        storage = self._ensure_storage()

        pipeline_id = metadata.get("pipeline_id") or name
        version = metadata.get("version") or datetime.now(timezone.utc).strftime(
            "%Y%m%d%H%M%S"
        )
        owner = metadata.get("owner")

        prefix = f"specs/docetl/{pipeline_id}"
        filename = f"{version}.yaml"
        upload_metadata = {
            "content_type": "text/yaml",
            "metadata": {
                "pipeline_id": pipeline_id,
                "version": version,
                "owner": owner,
                "namespace": namespace,
            },
        }

        file_ref = await storage.store_file(
            document_id=prefix,
            filename=filename,
            content=yaml_str.encode("utf-8"),
            metadata=upload_metadata,
        )

        bucket = getattr(getattr(storage, "provider", None), "bucket_name", "")
        persisted_path = file_ref.path or f"{prefix}/{filename}"
        uri = f"gs://{bucket}/{persisted_path}" if bucket else persisted_path

        manifest = {
            "pipeline_id": pipeline_id,
            "version": version,
            "owner": owner,
            "sink": "rt",
            "uri": uri,
            "saved_at": datetime.now(timezone.utc).isoformat(),
        }

        return PipelineSaveResult(
            path=uri,
            uri=uri,
            manifest=manifest,
            input_path=metadata.get("input_path"),
            output_path=metadata.get("output_path"),
        )
