import { Handle, Position, type NodeProps } from '@xyflow/react'
import { usePipelineStore } from '../../../store/pipelineStore'
import NodeStatusBadge from '../NodeStatusBadge'
import { getConnectionSummary } from '../../../lib/databaseConnections'
import type { DatabaseConnectionConfig, DbType } from '../../../types/pipeline'

const dbStyles = {
  oracle: { border: 'border-orange-400', bg: 'bg-orange-400', text: 'text-orange-500', handle: '!bg-orange-400', emoji: '🗄️' },
  postgres: { border: 'border-teal-400', bg: 'bg-teal-400', text: 'text-teal-500', handle: '!bg-teal-400', emoji: '🐘' },
}

export default function DatabaseSourceNode({ id, data }: NodeProps) {
  const nodeResults = usePipelineStore((s) => s.nodeResults)
  const setSelectedNodeId = usePipelineStore((s) => s.setSelectedNodeId)
  const openNodeError = usePipelineStore((s) => s.openNodeError)
  const loadPreview = usePipelineStore((s) => s.loadPreview)
  const result = nodeResults[id]
  const hasError = result?.status === 'error'
  const d = data as Record<string, unknown>
  const config = d.config as Record<string, unknown>
  const connection = config.connection as DatabaseConnectionConfig | undefined
  const dbType = ((config.db_type as string) || 'postgres') as DbType
  const tableName = d.tableName as string
  const style = dbStyles[dbType as keyof typeof dbStyles] || dbStyles.postgres

  return (
    <div
      className={`bg-white border-2 rounded-lg min-w-[180px] cursor-pointer ${
        hasError
          ? 'border-red-500 shadow-lg shadow-red-100 ring-2 ring-red-200/80'
          : `${style.border} shadow-md`
      }`}
      onClick={() => setSelectedNodeId(id)}
    >
      <div
        className={`text-white px-3 py-1.5 rounded-t-md text-sm flex items-center gap-2 ${
          hasError ? 'bg-red-500 font-bold' : `${style.bg} font-semibold`
        }`}
      >
        <span>{style.emoji}</span>
        <span>{(d.label as string) || 'Database Source'}</span>
      </div>
      <div className="px-3 py-2 text-xs space-y-1">
        <div className="text-gray-500 font-mono">{tableName}</div>
        {connection?.host && (
          <div className="text-gray-700 truncate max-w-[160px]">
            {getConnectionSummary(dbType, connection)}
          </div>
        )}
        {result && (
          <NodeStatusBadge
            result={result}
            onViewError={result.status === 'error' ? () => openNodeError(id) : undefined}
          />
        )}
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
