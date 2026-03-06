import { Handle, Position, type NodeProps } from '@xyflow/react'
import { usePipelineStore } from '../../../store/pipelineStore'
import NodeStatusBadge from '../NodeStatusBadge'

export default function TransformNode({ id, data }: NodeProps) {
  const nodeResults = usePipelineStore((s) => s.nodeResults)
  const setSelectedNodeId = usePipelineStore((s) => s.setSelectedNodeId)
  const loadPreview = usePipelineStore((s) => s.loadPreview)
  const result = nodeResults[id]
  const d = data as Record<string, unknown>
  const config = d.config as Record<string, string>
  const tableName = d.tableName as string
  const sqlPreview = config.sql ? config.sql.substring(0, 50) + (config.sql.length > 50 ? '...' : '') : 'No SQL defined'

  return (
    <div
      className="bg-white border-2 border-purple-400 rounded-lg shadow-md min-w-[180px] cursor-pointer"
      onClick={() => setSelectedNodeId(id)}
    >
      <Handle type="target" position={Position.Left} className="!bg-purple-400 !w-3 !h-3" />
      <div className="bg-purple-400 text-white px-3 py-1.5 rounded-t-md text-sm font-semibold flex items-center gap-2">
        <span>⚙️</span>
        <span>{(d.label as string) || 'Transform'}</span>
      </div>
      <div className="px-3 py-2 text-xs space-y-1">
        <div className="text-gray-500 font-mono">{tableName}</div>
        <div className="text-gray-600 italic truncate max-w-[160px]">{sqlPreview}</div>
        {result && <NodeStatusBadge result={result} />}
        {result?.status === 'success' && (
          <button
            className="text-purple-500 hover:underline text-xs"
            onClick={(e) => { e.stopPropagation(); loadPreview(id, tableName) }}
          >
            Preview data
          </button>
        )}
      </div>
      <Handle type="source" position={Position.Right} className="!bg-purple-400 !w-3 !h-3" />
    </div>
  )
}
