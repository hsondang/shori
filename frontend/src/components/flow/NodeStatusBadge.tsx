import { StatusBadge } from '@shori/design-system'
import { usePipelineStore } from '../../store/pipelineStore'
import { getResultElapsedLabel } from '../../lib/executionTiming'
import { toResultLike } from '../../lib/dsStatus'
import type { NodeExecutionResult } from '../../types/pipeline'

export default function NodeStatusBadge({
  result,
  onViewError,
}: {
  result: NodeExecutionResult
  onViewError?: () => void
}) {
  const executionClockNow = usePipelineStore((s) => s.executionClockNow)
  const elapsedLabel = getResultElapsedLabel(result, executionClockNow)

  return (
    <div className="space-y-0.5">
      <StatusBadge result={toResultLike(result, elapsedLabel)} />
      {result.error && onViewError && (
        <button
          type="button"
          onClick={onViewError}
          className="text-left text-red-700 font-semibold hover:underline"
        >
          View error
        </button>
      )}
    </div>
  )
}
