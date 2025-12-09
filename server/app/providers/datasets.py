from __future__ import annotations

import csv
import json
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
from fastapi import HTTPException, UploadFile

from utils.errors.error_types import ConfigurationError
from utils.parsing.rt_storage import RTStorageResolver
from utils.storage import StorageConfig, build_storage_client


def _home_dir() -> Path:
    """Resolve the base home directory for DocETL data."""
    return Path(os.getenv("DOCETL_HOME_DIR", os.path.expanduser("~")))


def _validate_json_content(content: bytes) -> None:
    try:
        json.loads(content)
    except json.JSONDecodeError as exc:  # pragma: no cover - fastapi surfaces detail
        raise HTTPException(
            status_code=400, detail=f"Invalid JSON format: {exc}"
        ) from exc


def _convert_csv_to_json(csv_content: bytes) -> bytes:
    try:
        csv_string = csv_content.decode("utf-8")
    except UnicodeDecodeError as exc:  # pragma: no cover - guarded upstream
        raise HTTPException(status_code=400, detail="Invalid CSV encoding") from exc

    reader = csv.DictReader(csv_string.splitlines())
    data = list(reader)
    if not data:
        raise HTTPException(status_code=400, detail="CSV file is empty")
    return json.dumps(data).encode("utf-8")


def _is_likely_csv(content: bytes, filename: str) -> bool:
    if filename.lower().endswith(".csv"):
        return True
    try:
        first_line = content.split(b"\n")[0].decode("utf-8")
        return "," in first_line and not any(c in first_line for c in "{}[]")
    except Exception:
        return False


@dataclass
class DatasetLoadResult:
    path: str
    metadata: dict[str, Any] = field(default_factory=dict)
    content: bytes | None = None
    content_type: str = "application/json"


class DatasetProvider(ABC):
    """Abstract dataset ingress provider."""

    @abstractmethod
    async def save_upload(
        self, *, namespace: str, file: UploadFile | None = None, url: str | None = None
    ) -> DatasetLoadResult: ...

    @abstractmethod
    async def load_dataset(
        self,
        *,
        namespace: str,
        dataset_url: str | None = None,
        source_version_id: str | None = None,
        project_id: str | None = None,
        subset: str | None = None,
        env: str | None = None,
        canonical_source_id: str | None = None,
    ) -> DatasetLoadResult: ...


class LocalDatasetProvider(DatasetProvider):
    """Backs datasets by the local ~/.docetl namespace structure."""

    def __init__(self, home_dir: Path | None = None, subdir: str = "files") -> None:
        self.home_dir = home_dir or _home_dir()
        self.subdir = subdir

    def _namespace_dir(self, namespace: str) -> Path:
        return self.home_dir / ".docetl" / namespace / self.subdir

    async def _write_content(
        self, namespace: str, filename: str, content: bytes
    ) -> DatasetLoadResult:
        target_dir = self._namespace_dir(namespace)
        target_dir.mkdir(parents=True, exist_ok=True)

        final_content = content
        if _is_likely_csv(content, filename):
            final_content = _convert_csv_to_json(content)

        _validate_json_content(final_content)

        safe_name = filename.replace(".csv", ".json")
        file_path = target_dir / safe_name
        file_path.write_bytes(final_content)

        return DatasetLoadResult(
            path=str(file_path),
            content=final_content,
            metadata={
                "provider": "local",
                "filename": safe_name,
                "namespace": namespace,
            },
        )

    async def _download_url(self, namespace: str, url: str) -> DatasetLoadResult:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, follow_redirects=True)
            if resp.status_code != 200:
                raise HTTPException(
                    status_code=400,
                    detail=f"Failed to download from URL: {resp.status_code}",
                )
            filename = url.split("/")[-1] or "dataset.json"
            return await self._write_content(namespace, filename, resp.content)

    async def save_upload(
        self, *, namespace: str, file: UploadFile | None = None, url: str | None = None
    ) -> DatasetLoadResult:
        if not file and not url:
            raise HTTPException(
                status_code=400, detail="Either file or url must be provided"
            )
        if url:
            return await self._download_url(namespace, url)

        content = await file.read()
        return await self._write_content(namespace, file.filename, content)

    async def load_dataset(
        self,
        *,
        namespace: str,
        dataset_url: str | None = None,
        source_version_id: str | None = None,
        project_id: str | None = None,
        subset: str | None = None,
        env: str | None = None,
        canonical_source_id: str | None = None,
    ) -> DatasetLoadResult:
        _ = (project_id, subset, env, canonical_source_id)
        if dataset_url:
            if dataset_url.startswith(("http://", "https://")):
                return await self._download_url(namespace, dataset_url)
            file_path = Path(dataset_url)
            if not file_path.exists():
                raise HTTPException(status_code=404, detail="Dataset not found")
            content = file_path.read_bytes()
            return DatasetLoadResult(
                path=str(file_path),
                content=content,
                metadata={"provider": "local", "filename": file_path.name},
            )

        if source_version_id:
            raise HTTPException(
                status_code=400,
                detail="source_version_id requires the RT dataset provider",
            )

        raise HTTPException(
            status_code=400, detail="dataset_url or source_version_id is required"
        )


class RTDatasetProvider(DatasetProvider):
    """
    Dataset provider that understands RT storage layout.

    If a direct dataset_url is provided, it is downloaded (HTTP(S) or GCS URI).
    Otherwise, a SourceVersion envelope is resolved through RTStorageResolver and
    materialised locally for DocETL consumption.
    """

    def __init__(
        self,
        *,
        storage_client=None,
        home_dir: Path | None = None,
        subdir: str = "rt",
    ) -> None:
        self._storage_client = storage_client
        self.home_dir = home_dir or _home_dir()
        self.subdir = subdir

    def _namespace_dir(self, namespace: str) -> Path:
        return self.home_dir / ".docetl" / namespace / self.subdir

    def _ensure_storage(self):
        if self._storage_client:
            return self._storage_client
        try:
            cfg = StorageConfig()
        except Exception as exc:  # pragma: no cover - defensive
            raise HTTPException(
                status_code=500, detail=f"Invalid storage configuration: {exc}"
            ) from exc
        if not cfg.enabled:
            raise HTTPException(
                status_code=500,
                detail="Storage client is disabled; set STORAGE__* to enable RT provider.",
            )
        try:
            self._storage_client = build_storage_client(cfg, monitoring_profile=None)
        except ConfigurationError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return self._storage_client

    async def _write_bytes(
        self, namespace: str, filename: str, content: bytes, *, metadata: dict[str, Any]
    ) -> DatasetLoadResult:
        target_dir = self._namespace_dir(namespace)
        target_dir.mkdir(parents=True, exist_ok=True)
        path = target_dir / filename
        path.write_bytes(content)
        return DatasetLoadResult(
            path=str(path),
            content=content,
            metadata=metadata | {"namespace": namespace, "provider": "rt"},
        )

    async def _download_http(
        self, namespace: str, url: str, *, metadata: dict[str, Any]
    ) -> DatasetLoadResult:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, follow_redirects=True)
            if resp.status_code != 200:
                raise HTTPException(
                    status_code=400,
                    detail=f"Failed to download dataset: {resp.status_code}",
                )
            filename = url.split("/")[-1] or "dataset.json"
            return await self._write_bytes(
                namespace, filename, resp.content, metadata=metadata
            )

    async def _fetch_storage_uri(
        self, namespace: str, uri: str, *, metadata: dict[str, Any]
    ) -> DatasetLoadResult:
        storage = self._ensure_storage()
        payload = await storage.get_file_from_uri(uri)
        filename = uri.split("/")[-1] or "dataset.json"
        return await self._write_bytes(
            namespace, filename, payload, metadata=metadata | {"source_uri": uri}
        )

    async def _resolve_source_version(
        self,
        namespace: str,
        source_version_id: str,
        *,
        project_id: str | None,
        env: str | None,
        canonical_source_id: str | None,
    ) -> DatasetLoadResult:
        storage = self._ensure_storage()
        resolver = RTStorageResolver(storage_client=storage)
        layout = await resolver.load_layout(env)
        cid = canonical_source_id or project_id or "document"
        prefix = resolver.build_unstructured_sv_prefix(
            layout,
            canonical_source_id=cid,
            source_version_id=source_version_id,
        )
        envelope_uri = f"gs://{layout.bucket}/{prefix}/envelope/envelope.json"
        return await self._fetch_storage_uri(
            namespace,
            envelope_uri,
            metadata={
                "source_version_id": source_version_id,
                "canonical_source_id": cid,
                "env": env or layout.env_prefix.rstrip("/"),
            },
        )

    async def save_upload(
        self, *, namespace: str, file: UploadFile | None = None, url: str | None = None
    ) -> DatasetLoadResult:
        # Uploads still land locally; RT provider keeps the same interface
        local_provider = LocalDatasetProvider(
            home_dir=self.home_dir, subdir=self.subdir
        )
        return await local_provider.save_upload(namespace=namespace, file=file, url=url)

    async def load_dataset(
        self,
        *,
        namespace: str,
        dataset_url: str | None = None,
        source_version_id: str | None = None,
        project_id: str | None = None,
        subset: str | None = None,
        env: str | None = None,
        canonical_source_id: str | None = None,
    ) -> DatasetLoadResult:
        _ = subset  # Subsetting handled upstream; preserved for signature parity
        if dataset_url:
            if dataset_url.startswith(("http://", "https://")):
                return await self._download_http(
                    namespace, dataset_url, metadata={"dataset_url": dataset_url}
                )
            if dataset_url.startswith("gs://"):
                return await self._fetch_storage_uri(
                    namespace, dataset_url, metadata={"dataset_url": dataset_url}
                )
            file_path = Path(dataset_url)
            if file_path.exists():
                return DatasetLoadResult(
                    path=str(file_path),
                    content=file_path.read_bytes(),
                    metadata={"provider": "rt", "dataset_url": dataset_url},
                )
            raise HTTPException(status_code=404, detail="Dataset not found")

        if source_version_id:
            return await self._resolve_source_version(
                namespace,
                source_version_id,
                project_id=project_id,
                env=env,
                canonical_source_id=canonical_source_id,
            )

        raise HTTPException(
            status_code=400, detail="dataset_url or source_version_id is required"
        )
