import { Handle, Position, type NodeProps } from '@xyflow/react'
import { NodeCard } from '@shori/design-system'
import { usePipelineStore } from '../../../store/pipelineStore'
import { useSettingsStore } from '../../../store/settingsStore'
import NodeCacheChip from '../NodeCacheChip'
import { toResultLike } from '../../../lib/dsStatus'
import { getResultElapsedLabel } from '../../../lib/executionTiming'
import {
  findSavedConnectionById,
  getConnectionSummary,
  getDatabaseSourceConnectionScope,
  getDatabaseSourceConnectionSourceId,
} from '../../../lib/databaseConnections'
import type { DatabaseConnectionConfig, DbType } from '../../../types/pipeline'

export default function DatabaseSourceNode({ id, data }: NodeProps) {
  const nodeResults = usePipelineStore((s) => s.nodeResults)
  const executionClockNow = usePipelineStore((s) => s.executionClockNow)
  const setSelectedNodeId = usePipelineStore((s) => s.setSelectedNodeId)
  const openNodeError = usePipelineStore((s) => s.openNodeError)
  const loadTablePreview = usePipelineStore((s) => s.loadTablePreview)
  const startLivePreview = usePipelineStore((s) => s.startLivePreview)
  const runNodeWithLoadMode = usePipelineStore((s) => s.runNodeWithLoadMode)
  const globalDatabaseConnections = useSettingsStore((s) => s.globalDatabaseConnections)
  const result = nodeResults[id]
  const d = data as Record<string, unknown>
  const config = d.config as Record<string, unknown>
  const tableName = d.tableName as string
  const elapsed = result ? getResultElapsedLabel(result, executionClockNow) : null

  const connectionScope = getDatabaseSourceConnectionScope(config)
  const globalConnection = findSavedConnectionById(
    globalDatabaseConnections,
    getDatabaseSourceConnectionSourceId(config),
  )
  const connection = (
    connectionScope === 'global' ? globalConnection : config.connection
  ) as DatabaseConnectionConfig | undefined
  const dbType = ((config.db_type as string) || 'postgres') as DbType

  const accentMap = { oracle: 'oracle', postgres: 'postgres' } as const
  const accent = accentMap[dbType as keyof typeof accentMap] ?? 'postgres'
  const iconMap = { oracle: '🗄️', postgres: '🐘' }
  const icon = iconMap[dbType as keyof typeof iconMap] ?? '🗄️'

  const subtitle = connectionScope === 'global'
    ? (globalConnection ? `Global · ${globalConnection.name}` : 'Global · Missing source')
    : connection?.host
      ? getConnectionSummary(dbType, connection)
      : undefined

  const actions = [
    { label: 'Preview', onClick: () => startLivePreview(id) },
    { label: 'Load to memory', onClick: () => runNodeWithLoadMode(id, 'in_memory') },
    { label: 'Materialize', tone: 'muted' as const, onClick: () => runNodeWithLoadMode(id, 'materialized') },
    ...(result?.status === 'success'
      ? [{ label: 'View table', tone: 'muted' as const, onClick: () => loadTablePreview(id, tableName) }]
      : []),
  ]

  return (
    <div>
      <NodeCard
        kind="db"
        accent={accent}
        icon={icon}
        title={(d.label as string) || 'Database Source'}
        tableName={tableName}
        subtitle={subtitle}
        result={result ? toResultLike(result, elapsed) : undefined}
        onSelect={() => setSelectedNodeId(id)}
        onViewError={result?.status === 'error' ? () => openNodeError(id) : undefined}
        actions={actions}
      >
        <NodeCacheChip nodeId={id} />
      </NodeCard>
      <Handle type="source" position={Position.Right} className="!bg-teal-400 !w-3 !h-3" />
    </div>
  )
}
