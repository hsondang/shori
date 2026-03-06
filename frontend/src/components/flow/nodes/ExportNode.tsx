import { Handle, Position, type NodeProps } from '@xyflow/react'
import { usePipelineStore } from '../../../store/pipelineStore'
import { exportData } from '../../../api/client'
import NodeStatusBadge from '../NodeStatusBadge'

export default function ExportNode({ id, data }: NodeProps) {
  const nodeResults = usePipelineStore((s) => s.nodeResults)
  const setSelectedNodeId = usePipelineStore((s) => s.setSelectedNodeId)
  const edges = usePipelineStore((s) => s.edges)
  const nodes = usePipelineStore((s) => s.nodes)
  const result = nodeResults[id]
  const d = data as Record<string, unknown>

  const sourceEdge = edges.find((e) => e.target === id)
  const sourceNode = sourceEdge ? nodes.find((n) => n.id === sourceEdge.source) : null
  const sourceTableName = sourceNode ? (sourceNode.data as Record<string, unknown>).tableName as string : null

  return (
    <div
      className="bg-white border-2 border-green-400 rounded-lg shadow-md min-w-[180px] cursor-pointer"
      onClick={() => setSelectedNodeId(id)}
    >
      <Handle type="target" position={Position.Left} className="!bg-green-400 !w-3 !h-3" />
      <div className="bg-green-400 text-white px-3 py-1.5 rounded-t-md text-sm font-semibold flex items-center gap-2">
        <span>📥</span>
        <span>{(d.label as string) || 'Export'}</span>
      </div>
      <div className="px-3 py-2 text-xs space-y-1">
        {sourceTableName && (
          <div className="text-gray-500">Source: <span className="font-mono">{sourceTableName}</span></div>
        )}
        {result && <NodeStatusBadge result={result} />}
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
