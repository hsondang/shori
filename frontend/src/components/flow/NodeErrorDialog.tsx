import { useEffect } from 'react'
import { usePipelineStore } from '../../store/pipelineStore'
import type { NodeType } from '../../types/pipeline'

const nodeTypeLabels: Record<NodeType, string> = {
  csv_source: 'CSV Source',
  db_source: 'Database Source',
  transform: 'Transform',
  export: 'Export',
}

export default function NodeErrorDialog() {
  const errorDialogNodeId = usePipelineStore((s) => s.errorDialogNodeId)
  const closeNodeError = usePipelineStore((s) => s.closeNodeError)
  const node = usePipelineStore((s) => s.nodes.find((candidate) => candidate.id === s.errorDialogNodeId))
  const result = usePipelineStore((s) => s.errorDialogNodeId ? s.nodeResults[s.errorDialogNodeId] : undefined)

  useEffect(() => {
    if (!errorDialogNodeId) return

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        closeNodeError()
      }
    }

    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [closeNodeError, errorDialogNodeId])

  if (!node || result?.status !== 'error' || !result.error) {
    return null
  }

  const data = node.data as Record<string, unknown>
  const title = (data.label as string) || nodeTypeLabels[node.type as NodeType] || 'Node'
  const nodeTypeLabel = nodeTypeLabels[node.type as NodeType] || 'Node'

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-gray-950/45 px-4"
      onClick={closeNodeError}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="node-error-dialog-title"
        className="w-full max-w-2xl rounded-2xl border border-red-200 bg-white shadow-2xl"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="border-b border-red-100 bg-red-50 px-5 py-4">
          <div className="text-xs font-semibold uppercase tracking-[0.18em] text-red-600">{nodeTypeLabel}</div>
          <h2 id="node-error-dialog-title" className="mt-1 text-lg font-bold text-gray-900">
            {title}
          </h2>
        </div>

        <div className="px-5 py-4">
          <div className="mb-2 text-sm font-medium text-gray-700">Execution error</div>
          <pre className="max-h-[60vh] overflow-y-auto rounded-xl bg-gray-950 px-4 py-3 text-sm leading-6 whitespace-pre-wrap break-words text-red-100">
            {result.error}
          </pre>
        </div>

        <div className="flex justify-end border-t border-gray-200 px-5 py-4">
          <button
            type="button"
            onClick={closeNodeError}
            className="rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"
          >
            Dismiss
          </button>
        </div>
      </div>
    </div>
  )
}
