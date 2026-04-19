from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.services.csv_service import CsvPreprocessArtifactStore
from app.services.duckdb_manager import DuckDBManager
from app.services.execution_registry import ExecutionRegistry
from app.routers import pipelines, execution, data, settings, upload


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.duckdb = DuckDBManager()
    app.state.csv_preprocess_artifacts = CsvPreprocessArtifactStore()
    app.state.execution_registry = ExecutionRegistry()
    yield
    app.state.execution_registry.close()
    app.state.csv_preprocess_artifacts.close()
    app.state.duckdb.close()


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
app.include_router(data.router, prefix="/api/data", tags=["data"])
app.include_router(upload.router, prefix="/api", tags=["upload"])


@app.get("/api/health")
def health():
    return {"status": "ok"}
