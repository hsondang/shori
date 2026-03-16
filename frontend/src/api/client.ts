import axios from 'axios'
import type {
  DataPreview,
  DatabaseConnectionConfig,
  NodeExecutionResult,
  PipelineDefinition,
} from '../types/pipeline'

const api = axios.create({ baseURL: '/api' })

export async function uploadCsv(file: File): Promise<{ file_path: string; filename: string }> {
  const form = new FormData()
  form.append('file', file)
  const { data } = await api.post('/upload/csv', form)
  return data
}

export async function executePipeline(
  pipeline: PipelineDefinition,
  force = false
): Promise<Record<string, NodeExecutionResult>> {
  const { data } = await api.post(`/execute/pipeline?force=${force}`, pipeline)
  return data
}

export async function executeNode(
  node: PipelineDefinition['nodes'][0]
): Promise<NodeExecutionResult> {
  const { data } = await api.post('/execute/node', node)
  return data
}

export async function previewData(
  tableName: string,
  offset = 0,
  limit = 100
): Promise<DataPreview> {
  const { data } = await api.get(`/data/preview/${tableName}`, {
    params: { offset, limit },
  })
  return data
}

export async function exportData(tableName: string): Promise<void> {
  const { data } = await api.get(`/data/export/${tableName}`, {
    responseType: 'blob',
  })
  const url = URL.createObjectURL(data)
  const a = document.createElement('a')
  a.href = url
  a.download = `${tableName}.csv`
  a.click()
  URL.revokeObjectURL(url)
}

export async function savePipeline(pipeline: PipelineDefinition): Promise<void> {
  await api.post('/pipelines', pipeline)
}

export async function loadPipeline(id: string): Promise<PipelineDefinition> {
  const { data } = await api.get(`/pipelines/${id}`)
  return data
}

export async function listPipelines(): Promise<Array<{ id: string; name: string }>> {
  const { data } = await api.get('/pipelines')
  return data
}

export async function deletePipeline(id: string): Promise<void> {
  await api.delete(`/pipelines/${id}`)
}

export async function testDbConnection(
  dbType: 'oracle' | 'postgres',
  config: DatabaseConnectionConfig
): Promise<{ success: boolean; error?: string }> {
  const endpoint = dbType === 'oracle' ? '/oracle/test-connection' : '/postgres/test-connection'
  const { data } = await api.post(endpoint, config)
  return data
}
