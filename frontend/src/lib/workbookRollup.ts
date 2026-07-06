import type { Edge } from '@xyflow/react'
import type { NodeExecutionResult } from '../types/pipeline'

/**
 * The workbook hub's displayed status (docs/excel-node-model.md §3.3): a pure
 * projection of its sheet nodes' last results. Never persisted, never an
 * engine result, never propagates — `error` only when every child's last run
 * failed; mixed when some did; neutral otherwise.
 */
export interface WorkbookRollup {
  childIds: string[]
  total: number
  succeeded: number
  failed: number
  kind: 'neutral' | 'mixed' | 'error'
  /** Short badge copy, e.g. "3/5 loaded, 1 failed"; null when nothing to say. */
  label: string | null
}

export function computeWorkbookRollup({
  hubId,
  edges,
  nodeResults,
}: {
  hubId: string
  edges: Array<Pick<Edge, 'source' | 'target'>>
  nodeResults: Record<string, NodeExecutionResult>
}): WorkbookRollup {
  const childIds = edges
    .filter((edge) => edge.source === hubId)
    .map((edge) => edge.target)
  const results = childIds
    .map((id) => nodeResults[id])
    .filter((result): result is NodeExecutionResult => result != null)
  const succeeded = results.filter((r) => r.status === 'success').length
  const failed = results.filter((r) => r.status === 'error').length

  let kind: WorkbookRollup['kind'] = 'neutral'
  if (failed > 0 && childIds.length > 0 && failed === childIds.length) {
    kind = 'error'
  } else if (failed > 0) {
    kind = 'mixed'
  }

  let label: string | null = null
  if (failed > 0) {
    label = `${succeeded}/${childIds.length} loaded, ${failed} failed`
  } else if (succeeded > 0) {
    label = `${succeeded}/${childIds.length} loaded`
  }

  return { childIds, total: childIds.length, succeeded, failed, kind, label }
}
