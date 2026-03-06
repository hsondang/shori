import { Handle, Position, type NodeProps } from '@xyflow/react'
import { usePipelineStore } from '../../../store/pipelineStore'
import NodeStatusBadge from '../NodeStatusBadge'

const dbStyles = {
  oracle: { border: 'border-orange-400', bg: 'bg-orange-400', text: 'text-orange-500', handle: '!bg-orange-400', emoji: '🗄️' },
  postgres: { border: 'border-teal-400', bg: 'bg-teal-400', text: 'text-teal-500', handle: '!bg-teal-400', emoji: '🐘' },
}

function connectionDisplay(dbType: string, connection: Record<string, unknown>): string {
  const host = connection.host as string
  const port = connection.port as number
  if (dbType === 'oracle') return `${host}:${port}/${connection.service_name as string}`
  return `${host}:${port}/${connection.database as string}`
}

export default function DatabaseSourceNode({ id, data }: NodeProps) {
  const nodeResults = usePipelineStore((s) => s.nodeResults)
  const setSelectedNodeId = usePipelineStore((s) => s.setSelectedNodeId)
  const loadPreview = usePipelineStore((s) => s.loadPreview)
  const result = nodeResults[id]
  const d = data as Record<string, unknown>
  const config = d.config as Record<string, unknown>
  const connection = config.connection as Record<string, unknown> | undefined
  const dbType = (config.db_type as string) || 'postgres'
  const tableName = d.tableName as string
  const style = dbStyles[dbType as keyof typeof dbStyles] || dbStyles.postgres

  return (
    <div
      className={`bg-white border-2 ${style.border} rounded-lg shadow-md min-w-[180px] cursor-pointer`}
      onClick={() => setSelectedNodeId(id)}
    >
      <div className={`${style.bg} text-white px-3 py-1.5 rounded-t-md text-sm font-semibold flex items-center gap-2`}>
        <span>{style.emoji}</span>
        <span>{(d.label as string) || 'Database Source'}</span>
      </div>
      <div className="px-3 py-2 text-xs space-y-1">
        <div className="text-gray-500 font-mono">{tableName}</div>
        {connection?.host && (
          <div className="text-gray-700 truncate max-w-[160px]">
            {connectionDisplay(dbType, connection)}
          </div>
        )}
        {result && <NodeStatusBadge result={result} />}
        {result?.status === 'success' && (
          <button
            className={`${style.text} hover:underline text-xs`}
            onClick={(e) => { e.stopPropagation(); loadPreview(id, tableName) }}
          >
            Preview data
          </button>
        )}
      </div>
      <Handle type="source" position={Position.Right} className={`${style.handle} !w-3 !h-3`} />
    </div>
  )
}
