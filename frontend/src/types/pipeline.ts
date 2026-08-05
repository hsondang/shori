export type NodeType = 'csv_source' | 'excel_source' | 'excel_workbook' | 'db_source' | 'transform' | 'export'
export type DbType = 'oracle' | 'postgres'
export type ConnectionScope = 'local' | 'global'
export type NodeStatus = 'idle' | 'connecting' | 'running' | 'success' | 'error' | 'cancelled'
export type NodeLabelMode = 'auto' | 'custom'
export type NodeEditorMode = 'closed' | 'create' | 'edit'
/** Where a node's result is held: RAM-only scratch vs the project DuckDB file. */
export type NodeLoadMode = 'in_memory' | 'materialized'
/** Single derived card label combining activity + location + freshness. */
export type NodeLifecycle = 'new' | 'idle' | 'in_memory' | 'materialized' | 'running' | 'error'
/** What a run is producing — drives the StatusBadge verb (Loading/Materializing/Live preview). */
export type RunMode = 'preview' | 'load' | 'materialize'

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

/** Workbook hub (docs/excel-node-model.md): owns the upload + sheet picker.
 * No table, no data state, invisible to the execution DAG — extraction fields
 * (selected_sheet, cell_range, …) belong to its sheet nodes (ExcelSourceConfig). */
export interface ExcelWorkbookConfig {
  file_path: string
  original_filename: string
  sheet_names: string[]
  /** Best-effort per-sheet extent read from the zip's <dimension ref> at
   * upload time; null/absent when the writer omitted it. Display-only. */
  sheet_dimensions?: Record<string, { rows: number; cols: number } | null>
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

/** Opt-in write permission on an Oracle connection. Readable does not imply
 * writable: only connections with this on appear as export destinations.
 *
 * Optional because it is a *global* connection concept — project-local
 * connections live in the pipeline JSON, are never export destinations, and
 * carry no such field. Absent always means not approved. */
export interface ExportPermission {
  allow_export?: boolean
}

export type SavedDatabaseConnection =
  | ({ id: string; name: string; db_type: 'postgres' } & PostgresConnectionConfig)
  | ({ id: string; name: string; db_type: 'oracle' } & OracleConnectionConfig & ExportPermission)

export type SavedDatabaseConnectionInput =
  | ({ name: string; db_type: 'postgres' } & PostgresConnectionConfig)
  | ({ name: string; db_type: 'oracle' } & OracleConnectionConfig & ExportPermission)

export type ScopedDatabaseConnection = SavedDatabaseConnection & { scope: ConnectionScope }

export interface TransformConfig {
  sql: string
  load_mode?: NodeLoadMode
}

export type ExportDestination = 'local' | 'ai_workspace' | 'database'

export interface ExportConfig {
  format: string
  /** 'local' writes a file; 'ai_workspace' clones the table into the project's
   * AI workspace; 'database' appends rows to an approved Oracle connection. */
  destination?: ExportDestination
  output_path?: string
  /** Global Oracle connection id, when destination === 'database'. */
  connection_source_id?: string
  /** "SCHEMA.TABLE_NAME" append target, when destination === 'database'. */
  target_table?: string
  /** Off (the default) exports the whole upstream table; on uses `sql`. */
  use_sql?: boolean
  sql?: string
}

export interface DatabaseExportColumnCheck {
  source_column: string
  source_type: string
  target_column: string | null
  target_type: string | null
  status: 'ok' | 'type_warning' | 'missing_in_target'
  message: string | null
}

export interface DatabaseExportValidation {
  target_table: string
  target_exists: boolean
  columns: DatabaseExportColumnCheck[]
  unmapped_target_columns: string[]
  errors: string[]
  warnings: string[]
  ok: boolean
}

/** Per-node validation report, with its own in-flight/failed states — the call
 * reaches the destination database, so it can be slow or fail on its own. */
export interface DatabaseExportValidationState {
  status: 'running' | 'done' | 'error'
  report?: DatabaseExportValidation
  error?: string
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
  /** Frontend-only decoration (not sent by the backend): which verb a 'running' result is. */
  mode?: RunMode
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
    /** Absent only for excel_workbook hubs (they produce no table). */
    table_name?: string
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

/** Presence + freshness of one (node, location) copy. Both `in_memory` and
 * `materialized` copies can exist independently — a node is not limited to one. */
export interface NodeLocationStatus {
  present: boolean
  state: NodeCacheState
  row_count: number | null
  column_count: number | null
  finished_at: string | null
  error: string | null
}

export interface NodeCacheStatus {
  /** Every persisted copy this node currently has, keyed by location. */
  locations: Partial<Record<NodeLoadMode, NodeLocationStatus>>
  /** Single derived card label (activity + location + freshness), projected over `locations`. */
  lifecycle: NodeLifecycle
  // Back-compat single-copy fields (precedence-preferred present copy: in_memory over materialized).
  state: NodeCacheState
  /** Where the preferred cached table lives, or null when nothing is cached. */
  location: NodeLoadMode | null
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

/** A node with no data in either DuckDB location — needs a destination pick
 * before a dependent run can proceed (node-state-model.md §6). */
export interface LoadDestinationCandidate {
  nodeId: string
  label: string
  tableName: string
}

export interface LoadDestinationPromptState {
  targetNodeId: string
  /** 'materialize': run candidates + target together (writes the target's table).
   * 'live-preview': run only the candidates, then retry the target's (view-only) live preview. */
  resumeKind: 'materialize' | 'live-preview'
  candidates: LoadDestinationCandidate[]
  choices: Record<string, NodeLoadMode>
}
