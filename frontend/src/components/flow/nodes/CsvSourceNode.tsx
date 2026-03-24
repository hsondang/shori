import { Handle, Position, type NodeProps } from '@xyflow/react'
import { usePipelineStore } from '../../../store/pipelineStore'
import NodeStatusBadge from '../NodeStatusBadge'
import type { CsvSourceConfig } from '../../../types/pipeline'

export default function CsvSourceNode({ id, data }: NodeProps) {
  const nodeResults = usePipelineStore((s) => s.nodeResults)
  const setSelectedNodeId = usePipelineStore((s) => s.setSelectedNodeId)
  const openNodeError = usePipelineStore((s) => s.openNodeError)
  const loadCsvPreview = usePipelineStore((s) => s.loadCsvPreview)
  const result = nodeResults[id]
  const hasError = result?.status === 'error'
  const d = data as Record<string, unknown>
  const config = d.config as CsvSourceConfig
  const tableName = d.tableName as string

  return (
    <div
      className={`bg-white border-2 rounded-lg min-w-[180px] cursor-pointer ${
        hasError
          ? 'border-red-500 shadow-lg shadow-red-100 ring-2 ring-red-200/80'
          : 'border-blue-400 shadow-md'
      }`}
      onClick={() => setSelectedNodeId(id)}
    >
      <div
        className={`text-white px-3 py-1.5 rounded-t-md text-sm flex items-center gap-2 ${
          hasError ? 'bg-red-500 font-bold' : 'bg-blue-400 font-semibold'
        }`}
      >
        <span>📄</span>
        <span>{(d.label as string) || 'CSV Source'}</span>
      </div>
      <div className="px-3 py-2 text-xs space-y-1">
        <div className="text-gray-500 font-mono">{tableName}</div>
        {config.original_filename && (
          <div className="text-gray-700 truncate max-w-[160px]">{config.original_filename}</div>
        )}
        {result && (
          <NodeStatusBadge
            result={result}
            onViewError={result.status === 'error' ? () => openNodeError(id) : undefined}
          />
        )}
        {config.file_path && (
          <button
            className="text-blue-500 hover:underline text-xs"
            onClick={(e) => { e.stopPropagation(); loadCsvPreview(id, config.file_path) }}
          >
            Preview data
          </button>
        )}
      </div>
      <Handle type="source" position={Position.Right} className="!bg-blue-400 !w-3 !h-3" />
    </div>
  )
}
