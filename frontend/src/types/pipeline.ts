export type NodeType = 'csv_source' | 'excel_source' | 'db_source' | 'transform' | 'export'
export type DbType = 'oracle' | 'postgres'
export type ConnectionScope = 'local' | 'global'
export type NodeStatus = 'idle' | 'connecting' | 'running' | 'success' | 'error' | 'cancelled'
export type NodeLabelMode = 'auto' | 'custom'
export type NodeEditorMode = 'closed' | 'create' | 'edit'
/** Where a node's result is held: RAM-only scratch vs the project DuckDB file. */
export type NodeLoadMode = 'in_memory' | 'materialized'
/** Single derived card label combining activity + location + freshness. */
export type NodeLifecycle = 'new' | 'idle' | 'in_memory' | 'materialized' | 'running' | 'error'

export interface PostgresConnectionConfig {
  host: string
  port: number
  database: string
  user: string
  password: string
}

export interface OracleConnectionConfig {
  host: string
  port: number
  service_name: string
  user: string
  password: string
}

export type OracleFetchMode = 'fetchall' | 'fetchmany'

export interface OracleFetchConfig {
  mode: OracleFetchMode
  arraysize: number
  prefetchrows: number
}

export type DatabaseConnectionConfig = PostgresConnectionConfig | OracleConnectionConfig

export interface CsvSourceConfig {
  file_path: string
  original_filename: string
  load_mode?: NodeLoadMode
  preprocessing?: CsvPreprocessingConfig
}

export interface ExcelSourceConfig {
  file_path: string
  original_filename: string
  sheet_names: string[]
  selected_sheet: string
  load_mode?: NodeLoadMode
  /** A1-style range passed to DuckDB read_xlsx, e.g. "A1:C100". */
  cell_range?: string
  /** Treat the first row as column names (read_xlsx header). */
  header?: boolean
  /** Disable type inference; every column comes back VARCHAR. */
  all_varchar?: boolean
}

export interface CsvPreprocessingConfig {
  enabled: boolean
  /** Path to a .py file exposing `preprocess(file) -> pandas.DataFrame`. */
  script_path: string
}

export interface PostgresDatabaseSourceConfig {
  connection_mode?: 'local'
  db_type: 'postgres'
  connection: PostgresConnectionConfig
  query: string
  load_mode?: NodeLoadMode
}

export interface OracleDatabaseSourceConfig {
  connection_mode?: 'local'
  db_type: 'oracle'
  connection: OracleConnectionConfig
  query: string
  fetch_config?: OracleFetchConfig
  load_mode?: NodeLoadMode
}

export interface GlobalPostgresDatabaseSourceConfig {
  connection_mode: 'global'
  connection_source_id: string
  db_type: 'postgres'
  query: string
  load_mode?: NodeLoadMode
}

export interface GlobalOracleDatabaseSourceConfig {
  connection_mode: 'global'
  connection_source_id: string
  db_type: 'oracle'
  query: string
  fetch_config?: OracleFetchConfig
  load_mode?: NodeLoadMode
}

export type DatabaseSourceConfig =
  | PostgresDatabaseSourceConfig
  | OracleDatabaseSourceConfig
  | GlobalPostgresDatabaseSourceConfig
  | GlobalOracleDatabaseSourceConfig

export type SavedDatabaseConnection =
  | ({ id: string; name: string; db_type: 'postgres' } & PostgresConnectionConfig)
  | ({ id: string; name: string; db_type: 'oracle' } & OracleConnectionConfig)

export type SavedDatabaseConnectionInput =
  | ({ name: string; db_type: 'postgres' } & PostgresConnectionConfig)
  | ({ name: string; db_type: 'oracle' } & OracleConnectionConfig)

export type ScopedDatabaseConnection = SavedDatabaseConnection & { scope: ConnectionScope }

export interface TransformConfig {
  sql: string
  load_mode?: NodeLoadMode
}

export interface ExportConfig {
  format: string
  /** Only 'local' for now; structured so other destinations can be added. */
  destination?: 'local'
  output_path?: string
}

export interface NodeEditorDraft {
  id: string
  type: NodeType
  position: { x: number; y: number }
  label: string
  description?: string
  autoLabel: string
  labelMode: NodeLabelMode
  tableName: string
  config: Record<string, unknown>
}

export interface NodeExecutionResult {
  node_id: string
  status: NodeStatus
  row_count?: number
  column_count?: number
  columns?: string[]
  error?: string
  execution_time_ms?: number
  started_at?: string
  finished_at?: string
  /** True when served from the project's persisted cache without re-running. */
  cached?: boolean
}

export interface ExecutionRunStatus {
  execution_id: string
  kind: 'node' | 'pipeline'
  status: NodeStatus
  started_at: string
  finished_at?: string
  node_results: Record<string, NodeExecutionResult>
}

export interface ProjectSettings {
  max_concurrent_nodes: number
  max_connections_per_database: number
  duckdb_memory_limit: string
  preview_chunk_rows: number
  preview_max_buffer_rows: number
  preview_session_ttl_seconds: number
}

export const DEFAULT_PROJECT_SETTINGS: ProjectSettings = {
  max_concurrent_nodes: 4,
  max_connections_per_database: 2,
  duckdb_memory_limit: '2GB',
  preview_chunk_rows: 200,
  preview_max_buffer_rows: 10000,
  preview_session_ttl_seconds: 600,
}

export interface PipelineDefinition {
  id: string
  name: string
  database_connections: SavedDatabaseConnection[]
  nodes: Array<{
    id: string
    type: NodeType
    table_name: string
    label: string
    description?: string
    auto_label?: string
    label_mode?: NodeLabelMode
    position: { x: number; y: number }
    config: Record<string, unknown>
  }>
  edges: Array<{
    id: string
    source: string
    target: string
  }>
  settings?: ProjectSettings
}

export type NodeCacheState = 'fresh' | 'stale' | 'missing' | 'loading' | 'failed'

export interface NodeCacheStatus {
  state: NodeCacheState
  /** Where the cached table lives, or null when nothing is cached. */
  location: NodeLoadMode | null
  /** Single derived card label (activity + location + freshness). */
  lifecycle: NodeLifecycle
  row_count: number | null
  column_count: number | null
  finished_at: string | null
  error: string | null
}

export interface CacheStatusResponse {
  nodes: Record<string, NodeCacheStatus>
}

export interface PreviewSessionChunk {
  session_id: string
  rows: unknown[][]
  buffered_rows: number
  has_more: boolean
  buffer_capped: boolean
}

export interface PreviewSessionStart extends PreviewSessionChunk {
  node_id: string
  columns: string[]
  column_types: string[]
}

export interface ProjectStorageInfo {
  file_size_bytes: number
  path: string
}

export interface ProjectSummary {
  id: string
  name: string
  starred: boolean
  created_at: string
  updated_at: string
}

export interface TablePreviewData {
  kind: 'table'
  columns: string[]
  column_types: string[]
  rows: unknown[][]
  total_rows: number
  offset: number
  limit: number
}

export interface CsvTextPreviewData {
  kind: 'csv_text'
  csv_stage: 'raw' | 'preprocessed'
  rows: string[][]
  limit: number
  truncated: boolean
  artifact_ready: boolean
}

export type DataPreview = TablePreviewData | CsvTextPreviewData

export interface MaterializedPreviewTab {
  nodeId: string
  tableNameAtLoad: string
  data: TablePreviewData | null
  loading: boolean
  error: string | null
  isStale: boolean
}

export interface LivePreviewState {
  nodeId: string
  sessionId: string | null
  columns: string[]
  columnTypes: string[]
  rows: unknown[][]
  hasMore: boolean
  bufferCapped: boolean
  loading: boolean
  materializing: boolean
  error: string | null
}

export interface TransientPreviewState {
  nodeId: string | null
  data: CsvTextPreviewData | null
  loading: boolean
  error: string | null
}

export type ActivePreviewTarget =
  | { kind: 'tab'; nodeId: string }
  | { kind: 'transient'; nodeId: string }
  | { kind: 'live'; nodeId: string }
