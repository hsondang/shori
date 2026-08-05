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

/**
 * Deep-copy a pipeline definition into a brand new project. Node/edge ids are
 * kept (they are scoped to the project), but the project gets a fresh id and a
 * distinct name so it lands as a separate entry in the catalog.
 */
export function duplicatePipelineDefinition(
  source: PipelineDefinition,
  overrides: { id?: string; name?: string } = {},
): PipelineDefinition {
  const clone =
    typeof structuredClone === 'function'
      ? structuredClone(source)
      : (JSON.parse(JSON.stringify(source)) as PipelineDefinition)

  return {
    ...clone,
    id: overrides.id ?? crypto.randomUUID(),
    name: overrides.name ?? `Copy of ${source.name}`,
  }
}
