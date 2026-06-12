from contextlib import asynccontextmanager
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.services.connection_pools import ConnectionPoolRegistry
from app.services.csv_service import CsvPreprocessArtifactStore
from app.services.execution_registry import ExecutionRegistry
from app.services.preview_sessions import PreviewSessionManager
from app.services.project_db_registry import ProjectDuckDBRegistry
from app.routers import pipelines, execution, data, preview_sessions, settings, upload


def _guard_single_worker():
    # Per-project DuckDB files assume exactly one writer process. A second
    # uvicorn worker could not open the files read-write and would fail in
    # confusing ways mid-request, so refuse to start instead.
    workers = os.getenv("WEB_CONCURRENCY") or os.getenv("UVICORN_WORKERS")
    if workers and workers.isdigit() and int(workers) > 1:
        raise RuntimeError(
            "Shori must run with a single worker process: project DuckDB files "
            "support one writer process at a time."
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    _guard_single_worker()
    app.state.project_dbs = ProjectDuckDBRegistry()
    app.state.connection_pools = ConnectionPoolRegistry()
    app.state.preview_sessions = PreviewSessionManager(app.state.connection_pools)
    app.state.csv_preprocess_artifacts = CsvPreprocessArtifactStore()
    app.state.execution_registry = ExecutionRegistry()
    yield
    app.state.execution_registry.close()
    app.state.csv_preprocess_artifacts.close()
    await app.state.preview_sessions.close_all()
    await app.state.connection_pools.close_all()
    app.state.project_dbs.close_all()


app = FastAPI(title="Shori", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(pipelines.router, prefix="/api/pipelines", tags=["pipelines"])
app.include_router(settings.router, prefix="/api/settings", tags=["settings"])
app.include_router(execution.router, prefix="/api/execute", tags=["execution"])
app.include_router(preview_sessions.router, prefix="/api/data/preview-session", tags=["preview-sessions"])
app.include_router(data.router, prefix="/api/data", tags=["data"])
app.include_router(upload.router, prefix="/api", tags=["upload"])


@app.get("/api/health")
def health():
    return {"status": "ok"}
