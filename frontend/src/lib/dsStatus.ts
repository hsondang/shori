import { statusPresentation, type NodeResultLike } from '@shori/design-system'
import type { NodeExecutionResult } from '../types/pipeline'

export function toResultLike(
  result: NodeExecutionResult,
  elapsedLabel?: string | null,
): NodeResultLike {
  return {
    status: result.status,
    cached: result.cached,
    rowCount: result.row_count,
    columnCount: result.column_count,
    executionTimeMs: result.execution_time_ms,
    elapsedLabel: elapsedLabel ?? null,
  }
}

export { statusPresentation }
