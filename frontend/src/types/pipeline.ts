export type NodeType = 'csv_source' | 'db_source' | 'transform' | 'export'
export type DbType = 'oracle' | 'postgres'
export type NodeStatus = 'idle' | 'running' | 'success' | 'error'

export interface CsvSourceConfig {
  file_path: string
  original_filename: string
}

export interface DatabaseSourceConfig {
  db_type: DbType
  connection: Record<string, unknown>
  query: string
}

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
