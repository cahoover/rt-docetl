from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse, JSONResponse
import os
import shutil
import json
from pathlib import Path

from server.app.models import PipelineConfigRequest
from server.app.providers import (
    DatasetLoadResult,
    DatasetProvider,
    LocalDatasetProvider,
    RTDatasetProvider,
    LocalPipelineSink,
    PipelineSink,
    RTPipelineSink,
)

router = APIRouter()


def _dataset_provider() -> DatasetProvider:
    provider = os.getenv("DATA_PROVIDER", "local").lower()
    if provider == "rt":
        return RTDatasetProvider()
    return LocalDatasetProvider()


def _pipeline_sink(preferred: str | None = None) -> PipelineSink:
    sink = (preferred or os.getenv("PIPELINE_SINK", "local")).lower()
    if sink == "rt":
        return RTPipelineSink()
    return LocalPipelineSink()


def get_home_dir() -> str:
    """Get the home directory from env var or user home"""
    return os.getenv("DOCETL_HOME_DIR", os.path.expanduser("~"))

def get_namespace_dir(namespace: str) -> Path:
    """Get the namespace directory path"""
    home_dir = get_home_dir()
    return Path(home_dir) / ".docetl" / namespace

@router.post("/check-namespace")
async def check_namespace(namespace: str):
    """Check if namespace exists and create if it doesn't"""
    try:
        namespace_dir = get_namespace_dir(namespace)
        exists = namespace_dir.exists()
        
        if not exists:
            namespace_dir.mkdir(parents=True, exist_ok=True)
            
        return {"exists": exists}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to check/create namespace: {str(e)}")

@router.post("/upload-file")
async def upload_file(
    file: UploadFile | None = File(None),
    url: str | None = Form(None),
    namespace: str = Form(...)
):
    """Upload a file to the namespace files directory, either from a direct upload or a URL"""
    try:
        provider = _dataset_provider()
        result = await provider.save_upload(namespace=namespace, file=file, url=url)
        return {"path": result.path, "metadata": result.metadata}
    except HTTPException:
        raise
    except Exception as e:  # pragma: no cover - defensive guard
        raise HTTPException(
            status_code=500, detail=f"Failed to upload file: {str(e)}"
        ) from e


def _preview_dataset(result: DatasetLoadResult) -> list[dict] | dict | None:
    """Return a small preview of the dataset for the UI without loading the full file."""
    try:
        raw = result.content
        if raw is None:
            raw = Path(result.path).read_bytes()
        snippet = raw[:200000]
        try:
            return json.loads(snippet.decode("utf-8"))
        except Exception:
            lines = snippet.decode("utf-8").splitlines()
            preview_lines = [line for line in lines if line.strip()][:50]
            return [json.loads(line) for line in preview_lines]
    except Exception:
        return None


@router.get("/dataset")
async def load_dataset(
    dataset_url: str | None = None,
    source_version_id: str | None = None,
    project_id: str | None = None,
    subset: str | None = None,
    env: str | None = None,
    namespace: str = "default",
):
    """Load a dataset either via URL or RT SourceVersion and materialize locally."""
    try:
        provider = _dataset_provider()
        result = await provider.load_dataset(
            namespace=namespace,
            dataset_url=dataset_url,
            source_version_id=source_version_id,
            project_id=project_id,
            subset=subset,
            env=env,
        )

        preview = _preview_dataset(result)
        response = {
            "path": result.path,
            "metadata": result.metadata,
            "dataset_url": dataset_url,
            "source_version_id": source_version_id,
            "content_type": result.content_type,
            "preview": preview,
        }
        return JSONResponse(response)
    except HTTPException:
        raise
    except Exception as e:  # pragma: no cover - defensive guard
        raise HTTPException(
            status_code=500, detail=f"Failed to load dataset: {str(e)}"
        ) from e

@router.post("/save-documents")
async def save_documents(files: list[UploadFile] = File(...), namespace: str = Form(...)):
    """Save multiple documents to the namespace documents directory"""
    try:
        uploads_dir = get_namespace_dir(namespace) / "documents"
        uploads_dir.mkdir(parents=True, exist_ok=True)
        
        saved_files = []
        for file in files:
            # Create safe filename
            safe_name = "".join(c if c.isalnum() or c in ".-" else "_" for c in file.filename)
            file_path = uploads_dir / safe_name
            
            with file_path.open("wb") as f:
                shutil.copyfileobj(file.file, f)
                
            saved_files.append({
                "name": file.filename,
                "path": str(file_path)
            })
            
        return {"files": saved_files}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save documents: {str(e)}")

@router.post("/write-pipeline-config")
async def write_pipeline_config(request: PipelineConfigRequest):
    """Write pipeline configuration YAML file"""
    try:
        sink = _pipeline_sink(getattr(request, "sink", None))
        metadata = (getattr(request, "metadata", None) or {}) | {
            "input_path": request.input_path,
            "output_path": request.output_path,
            "pipeline_id": getattr(request, "pipeline_id", None),
            "version": getattr(request, "version", None),
            "owner": getattr(request, "owner", None),
        }

        result = await sink.save(
            namespace=request.namespace,
            name=request.name,
            yaml_str=request.config,
            metadata=metadata,
        )

        return {
            "filePath": result.path,
            "inputPath": result.input_path or request.input_path,
            "outputPath": result.output_path or request.output_path,
            "uri": result.uri,
            "manifest": result.manifest,
        }
    except HTTPException:
        raise
    except Exception as e:  # pragma: no cover - defensive
        raise HTTPException(
            status_code=500,
            detail=f"Failed to write pipeline configuration: {str(e)}",
        ) from e


@router.post("/pipeline")
async def save_pipeline(request: PipelineConfigRequest):
    """Persist pipeline YAML using the configured sink (local or RT)."""
    try:
        sink = _pipeline_sink(getattr(request, "sink", None))
        metadata = (getattr(request, "metadata", None) or {}) | {
            "input_path": request.input_path,
            "output_path": request.output_path,
            "pipeline_id": getattr(request, "pipeline_id", None),
            "version": getattr(request, "version", None),
            "owner": getattr(request, "owner", None),
        }

        result = await sink.save(
            namespace=request.namespace,
            name=request.name,
            yaml_str=request.config,
            metadata=metadata,
        )

        return {
            "filePath": result.path,
            "inputPath": result.input_path or request.input_path,
            "outputPath": result.output_path or request.output_path,
            "uri": result.uri,
            "manifest": result.manifest,
        }
    except HTTPException:
        raise
    except Exception as e:  # pragma: no cover - defensive
        raise HTTPException(
            status_code=500, detail=f"Failed to save pipeline: {str(e)}"
        ) from e

@router.get("/read-file")
async def read_file(path: str):
    """Read file contents"""
    try:
        dataset: DatasetLoadResult | None = None
        if path.startswith(("http://", "https://", "gs://")):
            provider = _dataset_provider()
            dataset = await provider.load_dataset(
                namespace="default", dataset_url=path
            )
            file_path = Path(dataset.path)
        else:
            file_path = Path(path)

        if not file_path.exists():
            raise HTTPException(status_code=404, detail="File not found")
            
        return FileResponse(str(file_path))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read file: {str(e)}")

@router.get("/read-file-page")
async def read_file_page(path: str, page: int = 0, chunk_size: int = 500000):
    """Read file contents by page"""
    try:
        if path.startswith(("http://", "https://", "gs://")):
            dataset = await _dataset_provider().load_dataset(
                namespace="default", dataset_url=path
            )
            file_path = Path(dataset.path)
        else:
            file_path = Path(path)

        if not file_path.exists():
            raise HTTPException(status_code=404, detail="File not found")
            
        file_size = file_path.stat().st_size
        start = page * chunk_size
        
        with file_path.open("rb") as f:
            f.seek(start)
            content = f.read(chunk_size).decode("utf-8")
            
        return {
            "content": content,
            "totalSize": file_size,
            "page": page,
            "hasMore": start + len(content) < file_size
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read file: {str(e)}")

@router.get("/serve-document/{path:path}")
async def serve_document(path: str):
    """Serve document files"""
    try:
        # Security check for path traversal
        if ".." in path:
            raise HTTPException(status_code=400, detail="Invalid file path")
            
        file_path = Path(path)
        if not file_path.exists():
            raise HTTPException(status_code=404, detail="File not found")
            
        return FileResponse(
            path=file_path,
            filename=file_path.name,
            headers={"Cache-Control": "public, max-age=3600"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to serve file: {str(e)}")

@router.get("/check-file")
async def check_file(path: str):
    """Check if a file exists without reading it"""
    try:
        file_path = Path(path)
        exists = file_path.exists()
        return {"exists": exists}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to check file: {str(e)}")
