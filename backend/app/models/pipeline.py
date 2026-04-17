from typing import Annotated, Literal, Optional

from pydantic import BaseModel, Field
from enum import Enum


class NodeType(str, Enum):
    CSV_SOURCE = "csv_source"
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


class ExportConfig(BaseModel):
    format: str = "csv"


class Position(BaseModel):
    x: float
    y: float


class SavedConnectionBase(BaseModel):
    id: str
    name: str


class SavedPostgresConnection(SavedConnectionBase, PostgresConnectionConfig):
    db_type: Literal["postgres"] = "postgres"


class SavedOracleConnection(SavedConnectionBase, OracleConnectionConfig):
    db_type: Literal["oracle"] = "oracle"


DatabaseConnectionDefinition = Annotated[
    SavedPostgresConnection | SavedOracleConnection,
    Field(discriminator="db_type"),
]


class NodeDefinition(BaseModel):
    id: str
    type: NodeType
    table_name: str
    label: str
    auto_label: Optional[str] = None
    label_mode: Optional[Literal["auto", "custom"]] = None
    position: Position
    config: dict


class EdgeDefinition(BaseModel):
    id: str
    source: str
    target: str


class PipelineDefinition(BaseModel):
    id: str
    name: str
    database_connections: list[DatabaseConnectionDefinition] = Field(default_factory=list)
    nodes: list[NodeDefinition]
    edges: list[EdgeDefinition]


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


class ExecutionRunStatus(BaseModel):
    execution_id: str
    kind: Literal["node", "pipeline"]
    status: NodeStatus
    started_at: str
    finished_at: Optional[str] = None
    node_results: dict[str, NodeExecutionResult] = Field(default_factory=dict)
