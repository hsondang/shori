import { statusPresentation, type NodeResultLike } from '@shori/design-system'
import type { NodeExecutionResult, NodeLoadMode, RunMode } from '../types/pipeline'

export function toResultLike(
  result: NodeExecutionResult,
  elapsedLabel?: string | null,
  mode?: RunMode | null,
): NodeResultLike {
  return {
    status: result.status,
    cached: result.cached,
    rowCount: result.row_count,
    columnCount: result.column_count,
    executionTimeMs: result.execution_time_ms,
    elapsedLabel: elapsedLabel ?? null,
    mode: mode ?? result.mode ?? undefined,
  }
}

/**
 * The run-mode verb for a `running` result (node-state-model.md §1.1): 'preview'
 * while a live-preview session is opening, else 'load'/'materialize' from the
 * node's persisted load_mode (every other run writes a table to one of those).
 */
export function deriveRunMode(params: {
  isLivePreviewOpening: boolean
  loadMode: NodeLoadMode | undefined
}): RunMode {
  if (params.isLivePreviewOpening) return 'preview'
  return params.loadMode === 'materialized' ? 'materialize' : 'load'
}

export { statusPresentation }
