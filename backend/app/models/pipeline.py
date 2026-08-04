from typing import Annotated, Literal, Optional

from pydantic import BaseModel, Field
from enum import Enum


class NodeType(str, Enum):
    CSV_SOURCE = "csv_source"
    EXCEL_SOURCE = "excel_source"
    # Workbook hub: represents an uploaded Excel file, owns the sheet picker.
    # Has no table and no data state, and is invisible to the execution DAG —
    # see docs/excel-node-model.md.
    EXCEL_WORKBOOK = "excel_workbook"
    DB_SOURCE = "db_source"
    TRANSFORM = "transform"
    EXPORT = "export"


class NodeStatus(str, Enum):
    IDLE = "idle"
    CONNECTING = "connecting"
    RUNNING = "running"
    SUCCESS = "success"
    ERROR = "error"
    CANCELLED = "cancelled"


# Where a source/transform node's result is held. `in_memory` lives in the
# attached scratch catalog (RAM-only, gone on restart); `materialized` is a
# real table in the project's DuckDB file.
NodeLoadMode = Literal["in_memory", "materialized"]

# The single derived label the node card shows, blending activity + location +
# freshness. Computed in the cache-status endpoint from the persisted meta.
NodeLifecycle = Literal[
    "new", "idle", "in_memory", "materialized", "running", "error"
]


class OracleConnectionConfig(BaseModel):
    host: str
    port: int = 1521
    service_name: str
    user: str
    password: str


class CsvSourceConfig(BaseModel):
    file_path: str
    original_filename: str


class PostgresConnectionConfig(BaseModel):
    host: str
    port: int = 5432
    database: str
    user: str
    password: str


class TransformConfig(BaseModel):
    sql: str


class Position(BaseModel):
    x: float
    y: float


class SavedConnectionBase(BaseModel):
    id: str
    name: str


class SavedConnectionInputBase(BaseModel):
    name: str


class SavedPostgresConnection(SavedConnectionBase, PostgresConnectionConfig):
    db_type: Literal["postgres"] = "postgres"


class SavedOracleConnection(SavedConnectionBase, OracleConnectionConfig):
    db_type: Literal["oracle"] = "oracle"
    # Opt-in write permission: readable does not imply writable. Export nodes
    # only offer connections with this on, and the export service re-checks it.
    allow_export: bool = False


DatabaseConnectionDefinition = Annotated[
    SavedPostgresConnection | SavedOracleConnection,
    Field(discriminator="db_type"),
]


class SavedPostgresConnectionInput(SavedConnectionInputBase, PostgresConnectionConfig):
    db_type: Literal["postgres"] = "postgres"


class SavedOracleConnectionInput(SavedConnectionInputBase, OracleConnectionConfig):
    db_type: Literal["oracle"] = "oracle"
    allow_export: bool = False


DatabaseConnectionInputDefinition = Annotated[
    SavedPostgresConnectionInput | SavedOracleConnectionInput,
    Field(discriminator="db_type"),
]


class NodeDefinition(BaseModel):
    id: str
    type: NodeType
    # None is valid only for EXCEL_WORKBOOK hubs (they produce no table);
    # every other type is enforced non-empty by the table-name validators.
    table_name: Optional[str] = None
    label: str
    description: Optional[str] = None
    auto_label: Optional[str] = None
    label_mode: Optional[Literal["auto", "custom"]] = None
    position: Position
    config: dict


class EdgeDefinition(BaseModel):
    id: str
    source: str
    target: str


class ProjectSettings(BaseModel):
    """Per-project advanced execution/storage settings."""

    max_concurrent_nodes: int = Field(default=4, ge=1, le=32)
    max_connections_per_database: int = Field(default=2, ge=1, le=16)
    duckdb_memory_limit: str = Field(
        default="2GB", pattern=r"^\d+(\.\d+)?\s*(?i:[KMGT]i?B)$"
    )
    preview_chunk_rows: int = Field(default=200, ge=10, le=2000)
    preview_max_buffer_rows: int = Field(default=10_000, ge=200, le=1_000_000)
    preview_session_ttl_seconds: int = Field(default=600, ge=30, le=86_400)


class PipelineDefinition(BaseModel):
    id: str
    name: str
    database_connections: list[DatabaseConnectionDefinition] = Field(default_factory=list)
    nodes: list[NodeDefinition]
    edges: list[EdgeDefinition]
    settings: ProjectSettings = Field(default_factory=ProjectSettings)


class ProjectSummary(BaseModel):
    id: str
    name: str
    starred: bool = False
    created_at: str
    updated_at: str


class ProjectStarUpdate(BaseModel):
    starred: bool


class NodeExecutionResult(BaseModel):
    node_id: str
    status: NodeStatus
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    columns: Optional[list[str]] = None
    error: Optional[str] = None
    execution_time_ms: Optional[float] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    # True when the result was served from the project's persisted cache
    # without re-executing the node.
    cached: bool = False


class ExecutionRunStatus(BaseModel):
    execution_id: str
    kind: Literal["node", "pipeline"]
    status: NodeStatus
    started_at: str
    finished_at: Optional[str] = None
    node_results: dict[str, NodeExecutionResult] = Field(default_factory=dict)
