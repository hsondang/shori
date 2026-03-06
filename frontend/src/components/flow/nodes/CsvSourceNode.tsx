import { Handle, Position, type NodeProps } from '@xyflow/react'
import { usePipelineStore } from '../../../store/pipelineStore'
import NodeStatusBadge from '../NodeStatusBadge'

export default function CsvSourceNode({ id, data }: NodeProps) {
  const nodeResults = usePipelineStore((s) => s.nodeResults)
  const setSelectedNodeId = usePipelineStore((s) => s.setSelectedNodeId)
  const loadPreview = usePipelineStore((s) => s.loadPreview)
  const result = nodeResults[id]
  const d = data as Record<string, unknown>
  const config = d.config as Record<string, string>
  const tableName = d.tableName as string

  return (
    <div
      className="bg-white border-2 border-blue-400 rounded-lg shadow-md min-w-[180px] cursor-pointer"
      onClick={() => setSelectedNodeId(id)}
    >
      <div className="bg-blue-400 text-white px-3 py-1.5 rounded-t-md text-sm font-semibold flex items-center gap-2">
        <span>📄</span>
        <span>{(d.label as string) || 'CSV Source'}</span>
      </div>
      <div className="px-3 py-2 text-xs space-y-1">
        <div className="text-gray-500 font-mono">{tableName}</div>
        {config.original_filename && (
          <div className="text-gray-700 truncate max-w-[160px]">{config.original_filename}</div>
        )}
        {result && <NodeStatusBadge result={result} />}
        {result?.status === 'success' && (
          <button
            className="text-blue-500 hover:underline text-xs"
            onClick={(e) => { e.stopPropagation(); loadPreview(id, tableName) }}
          >
            Preview data
          </button>
        )}
      </div>
      <Handle type="source" position={Position.Right} className="!bg-blue-400 !w-3 !h-3" />
    </div>
  )
}
