export type NodeType = 'csv_source' | 'db_source' | 'transform' | 'export'
export type DbType = 'oracle' | 'postgres'
export type NodeStatus = 'idle' | 'running' | 'success' | 'error'

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

export type DatabaseConnectionConfig = PostgresConnectionConfig | OracleConnectionConfig

export interface CsvSourceConfig {
  file_path: string
  original_filename: string
}

export interface DatabaseSourceConfig {
  db_type: DbType
  connection: DatabaseConnectionConfig
  query: string
}

export type SavedDatabaseConnection =
  | ({ id: string; name: string; db_type: 'postgres' } & PostgresConnectionConfig)
  | ({ id: string; name: string; db_type: 'oracle' } & OracleConnectionConfig)

export type SavedDatabaseConnectionInput =
  | ({ name: string; db_type: 'postgres' } & PostgresConnectionConfig)
  | ({ name: string; db_type: 'oracle' } & OracleConnectionConfig)

export interface TransformConfig {
  sql: string
}

export interface ExportConfig {
  format: string
}

export interface NodeExecutionResult {
  node_id: string
  status: NodeStatus
  row_count?: number
  column_count?: number
  columns?: string[]
  error?: string
  execution_time_ms?: number
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
    position: { x: number; y: number }
    config: Record<string, unknown>
  }>
  edges: Array<{
    id: string
    source: string
    target: string
  }>
}

export interface DataPreview {
  columns: string[]
  column_types: string[]
  rows: unknown[][]
  total_rows: number
  offset: number
  limit: number
}
