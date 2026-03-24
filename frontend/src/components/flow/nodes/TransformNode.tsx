import { Handle, Position, type NodeProps } from '@xyflow/react'
import { usePipelineStore } from '../../../store/pipelineStore'
import NodeStatusBadge from '../NodeStatusBadge'

export default function TransformNode({ id, data }: NodeProps) {
  const nodeResults = usePipelineStore((s) => s.nodeResults)
  const setSelectedNodeId = usePipelineStore((s) => s.setSelectedNodeId)
  const openNodeError = usePipelineStore((s) => s.openNodeError)
  const loadTablePreview = usePipelineStore((s) => s.loadTablePreview)
  const runTransformPreview = usePipelineStore((s) => s.runTransformPreview)
  const result = nodeResults[id]
  const hasError = result?.status === 'error'
  const d = data as Record<string, unknown>
  const config = d.config as Record<string, string>
  const tableName = d.tableName as string
  const sqlPreview = config.sql ? config.sql.substring(0, 50) + (config.sql.length > 50 ? '...' : '') : 'No SQL defined'
  const canRunPreview = Boolean(config.sql?.trim()) && result?.status !== 'running'

  return (
    <div
      className={`bg-white border-2 rounded-lg min-w-[180px] cursor-pointer ${
        hasError
          ? 'border-red-500 shadow-lg shadow-red-100 ring-2 ring-red-200/80'
          : 'border-purple-400 shadow-md'
      }`}
      onClick={() => setSelectedNodeId(id)}
    >
      <Handle type="target" position={Position.Left} className="!bg-purple-400 !w-3 !h-3" />
      <div
        className={`text-white px-3 py-1.5 rounded-t-md text-sm flex items-center gap-2 ${
          hasError ? 'bg-red-500 font-bold' : 'bg-purple-400 font-semibold'
        }`}
      >
        <span>⚙️</span>
        <span>{(d.label as string) || 'Transform'}</span>
      </div>
      <div className="px-3 py-2 text-xs space-y-1">
        <div className="text-gray-500 font-mono">{tableName}</div>
        <div className="text-gray-600 italic truncate max-w-[160px]">{sqlPreview}</div>
        {result && (
          <NodeStatusBadge
            result={result}
            onViewError={result.status === 'error' ? () => openNodeError(id) : undefined}
          />
        )}
        <button
          type="button"
          className={`text-xs text-left ${canRunPreview ? 'text-purple-500 hover:underline' : 'text-gray-400 cursor-not-allowed'}`}
          onClick={(e) => {
            e.stopPropagation()
            if (!canRunPreview) return
            void runTransformPreview(id)
          }}
          disabled={!canRunPreview}
        >
          {result?.status === 'running' ? 'Running...' : 'Run and Preview'}
        </button>
        {result?.status === 'success' && (
          <button
            className="text-purple-500 hover:underline text-xs"
            onClick={(e) => { e.stopPropagation(); loadTablePreview(id, tableName) }}
          >
            Preview data
          </button>
        )}
      </div>
      <Handle type="source" position={Position.Right} className="!bg-purple-400 !w-3 !h-3" />
    </div>
  )
}
