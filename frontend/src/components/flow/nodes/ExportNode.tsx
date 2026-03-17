import { Handle, Position, type NodeProps } from '@xyflow/react'
import { usePipelineStore } from '../../../store/pipelineStore'
import { exportData } from '../../../api/client'
import NodeStatusBadge from '../NodeStatusBadge'

export default function ExportNode({ id, data }: NodeProps) {
  const nodeResults = usePipelineStore((s) => s.nodeResults)
  const setSelectedNodeId = usePipelineStore((s) => s.setSelectedNodeId)
  const openNodeError = usePipelineStore((s) => s.openNodeError)
  const edges = usePipelineStore((s) => s.edges)
  const nodes = usePipelineStore((s) => s.nodes)
  const result = nodeResults[id]
  const hasError = result?.status === 'error'
  const d = data as Record<string, unknown>

  const sourceEdge = edges.find((e) => e.target === id)
  const sourceNode = sourceEdge ? nodes.find((n) => n.id === sourceEdge.source) : null
  const sourceTableName = sourceNode ? (sourceNode.data as Record<string, unknown>).tableName as string : null

  return (
    <div
      className={`bg-white border-2 rounded-lg min-w-[180px] cursor-pointer ${
        hasError
          ? 'border-red-500 shadow-lg shadow-red-100 ring-2 ring-red-200/80'
          : 'border-green-400 shadow-md'
      }`}
      onClick={() => setSelectedNodeId(id)}
    >
      <Handle type="target" position={Position.Left} className="!bg-green-400 !w-3 !h-3" />
      <div
        className={`text-white px-3 py-1.5 rounded-t-md text-sm flex items-center gap-2 ${
          hasError ? 'bg-red-500 font-bold' : 'bg-green-400 font-semibold'
        }`}
      >
        <span>📥</span>
        <span>{(d.label as string) || 'Export'}</span>
      </div>
      <div className="px-3 py-2 text-xs space-y-1">
        {sourceTableName && (
          <div className="text-gray-500">Source: <span className="font-mono">{sourceTableName}</span></div>
        )}
        {result && (
          <NodeStatusBadge
            result={result}
            onViewError={result.status === 'error' ? () => openNodeError(id) : undefined}
          />
        )}
        {sourceTableName && (
          <button
            className="bg-green-100 text-green-700 px-2 py-1 rounded text-xs hover:bg-green-200"
            onClick={(e) => {
              e.stopPropagation()
              exportData(sourceTableName)
            }}
          >
            Download CSV
          </button>
        )}
      </div>
    </div>
  )
}
