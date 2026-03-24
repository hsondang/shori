import type { PipelineDefinition } from '../types/pipeline'

export function createBlankPipelineDefinition(id = crypto.randomUUID()): PipelineDefinition {
  return {
    id,
    name: 'Untitled Pipeline',
    database_connections: [],
    nodes: [],
    edges: [],
  }
}

export function snapshotPipelineDefinition(pipeline: PipelineDefinition): string {
  return JSON.stringify(pipeline)
}
