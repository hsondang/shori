from pydantic import BaseModel
from enum import Enum
from typing import Optional


class NodeType(str, Enum):
    CSV_SOURCE = "csv_source"
    DB_SOURCE = "db_source"
    TRANSFORM = "transform"
    EXPORT = "export"


class NodeStatus(str, Enum):
    IDLE = "idle"
    RUNNING = "running"
    SUCCESS = "success"
    ERROR = "error"


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


class NodeDefinition(BaseModel):
    id: str
    type: NodeType
    table_name: str
    label: str
    position: Position
    config: dict


class EdgeDefinition(BaseModel):
    id: str
    source: str
    target: str


class PipelineDefinition(BaseModel):
    id: str
    name: str
    nodes: list[NodeDefinition]
    edges: list[EdgeDefinition]


class NodeExecutionResult(BaseModel):
    node_id: str
    status: NodeStatus
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    columns: Optional[list[str]] = None
    error: Optional[str] = None
    execution_time_ms: Optional[float] = None
