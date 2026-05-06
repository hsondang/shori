import { Handle, Position, type NodeProps } from '@xyflow/react'
import { usePipelineStore } from '../../../store/pipelineStore'
import NodeStatusBadge from '../NodeStatusBadge'
import type { ExcelSourceConfig } from '../../../types/pipeline'

export default function ExcelSourceNode({ id, data }: NodeProps) {
  const nodeResults = usePipelineStore((s) => s.nodeResults)
  const setSelectedNodeId = usePipelineStore((s) => s.setSelectedNodeId)
  const openNodeError = usePipelineStore((s) => s.openNodeError)
  const loadCsvPreview = usePipelineStore((s) => s.loadCsvPreview)
  const result = nodeResults[id]
  const hasError = result?.status === 'error'
  const d = data as Record<string, unknown>
  const config = d.config as ExcelSourceConfig
  const tableName = d.tableName as string
  const previewPath = config.materialized_csv_path

  return (
    <div
      className={`min-w-[190px] cursor-pointer rounded-lg border-2 bg-white ${
        hasError
          ? 'border-red-500 shadow-lg shadow-red-100 ring-2 ring-red-200/80'
          : 'border-emerald-400 shadow-md'
      }`}
      onClick={() => setSelectedNodeId(id)}
    >
      <div
        className={`flex items-center gap-2 rounded-t-md px-3 py-1.5 text-sm text-white ${
          hasError ? 'bg-red-500 font-bold' : 'bg-emerald-500 font-semibold'
        }`}
      >
        <span>▦</span>
        <span>{(d.label as string) || 'Excel Source'}</span>
      </div>
      <div className="space-y-1 px-3 py-2 text-xs">
        <div className="font-mono text-gray-500">{tableName}</div>
        {config.original_filename && (
          <div className="max-w-[170px] truncate text-gray-700">{config.original_filename}</div>
        )}
        {config.selected_sheet && (
          <div className="max-w-[170px] truncate text-emerald-700">Sheet: {config.selected_sheet}</div>
        )}
        {result && (
          <NodeStatusBadge
            result={result}
            onViewError={result.status === 'error' ? () => openNodeError(id) : undefined}
          />
        )}
        {previewPath && (
          <button
            className="text-emerald-600 hover:underline text-xs"
            onClick={(e) => { e.stopPropagation(); loadCsvPreview(id, previewPath) }}
          >
            Preview data
          </button>
        )}
      </div>
      <Handle type="source" position={Position.Right} className="!h-3 !w-3 !bg-emerald-500" />
    </div>
  )
}
