import type { PipelineDefinition } from '../types/pipeline'
import { DEFAULT_PROJECT_SETTINGS } from '../types/pipeline'

export function createBlankPipelineDefinition(id = crypto.randomUUID()): PipelineDefinition {
  return {
    id,
    name: 'Untitled Pipeline',
    database_connections: [],
    nodes: [],
    edges: [],
    settings: { ...DEFAULT_PROJECT_SETTINGS },
  }
}

export function snapshotPipelineDefinition(pipeline: PipelineDefinition): string {
  return JSON.stringify(pipeline)
}
