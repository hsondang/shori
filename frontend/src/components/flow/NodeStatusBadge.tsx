import type { NodeExecutionResult } from '../../types/pipeline'
import { usePipelineStore } from '../../store/pipelineStore'
import { getResultElapsedLabel } from '../../lib/executionTiming'

const statusColors = {
  idle: 'bg-gray-100 text-gray-600',
  connecting: 'bg-blue-100 text-blue-700',
  running: 'bg-yellow-100 text-yellow-700',
  success: 'bg-green-100 text-green-700',
  error: 'bg-red-100 text-red-700',
  cancelled: 'bg-stone-200 text-stone-700',
}

export default function NodeStatusBadge({
  result,
  onViewError,
}: {
  result: NodeExecutionResult
  onViewError?: () => void
}) {
  const executionClockNow = usePipelineStore((s) => s.executionClockNow)
  const runningElapsed = getResultElapsedLabel(result, executionClockNow)
  const statusLabel = result.status === 'running'
    ? `Running${runningElapsed ? ` · ${runningElapsed}` : ''}`
    : result.status === 'connecting'
      ? 'Connecting'
      : result.status === 'cancelled'
        ? 'Cancelled'
      : result.status

  return (
    <div className="space-y-0.5">
      <span className={`inline-block px-1.5 py-0.5 rounded text-[10px] font-medium ${statusColors[result.status]}`}>
        {(result.status === 'running' || result.status === 'connecting') && (
          <span className="mr-1 inline-block h-1.5 w-1.5 animate-pulse rounded-full bg-current align-middle" />
        )}
        {statusLabel}
        {result.execution_time_ms != null && ` (${Math.round(result.execution_time_ms)}ms)`}
      </span>
      {result.row_count != null && (
        <div className="text-gray-500">
          {result.row_count.toLocaleString()} rows × {result.column_count} cols
        </div>
      )}
      {result.error && (
        onViewError ? (
          <button
            type="button"
            onClick={onViewError}
            className="text-left text-red-700 font-semibold hover:underline"
          >
            View error
          </button>
        ) : null
      )}
    </div>
  )
}
