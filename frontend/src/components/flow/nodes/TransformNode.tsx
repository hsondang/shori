import { Handle, Position, type NodeProps } from '@xyflow/react'
import { NodeCard } from '@shori/design-system'
import { usePipelineStore } from '../../../store/pipelineStore'
import NodeCacheChip from '../NodeCacheChip'
import { deriveRunMode, toResultLike } from '../../../lib/dsStatus'
import { getResultElapsedLabel } from '../../../lib/executionTiming'
import type { NodeLoadMode } from '../../../types/pipeline'

export default function TransformNode({ id, data }: NodeProps) {
  const nodeResults = usePipelineStore((s) => s.nodeResults)
  const executionClockNow = usePipelineStore((s) => s.executionClockNow)
  const setSelectedNodeId = usePipelineStore((s) => s.setSelectedNodeId)
  const openNodeError = usePipelineStore((s) => s.openNodeError)
  const loadTablePreview = usePipelineStore((s) => s.loadTablePreview)
  const runTransformPreview = usePipelineStore((s) => s.runTransformPreview)
  const startLivePreview = usePipelineStore((s) => s.startLivePreview)
  const live = usePipelineStore((s) => s.livePreviewsByNodeId[id])
  const result = nodeResults[id]
  const d = data as Record<string, unknown>
  const config = d.config as Record<string, string>
  const tableName = d.tableName as string
  const elapsed = result ? getResultElapsedLabel(result, executionClockNow) : null
  const isRunning = result?.status === 'running'
  const canPreview = Boolean(config.sql?.trim()) && !isRunning
  const isLivePreviewOpening = Boolean(live?.loading && !live?.sessionId)
  const mode = deriveRunMode({ isLivePreviewOpening, loadMode: config.load_mode as NodeLoadMode | undefined })
  const sqlPreview = config.sql
    ? config.sql.substring(0, 50) + (config.sql.length > 50 ? '…' : '')
    : 'No SQL defined'

  const actions = [
    {
      label: isRunning && isLivePreviewOpening ? 'Preview…' : 'Preview',
      onClick: canPreview ? () => startLivePreview(id) : undefined,
      tone: canPreview ? 'default' as const : 'muted' as const,
    },
    {
      label: isRunning && !isLivePreviewOpening ? 'Running…' : 'Run and Preview',
      onClick: canPreview ? () => runTransformPreview(id) : undefined,
      tone: canPreview ? 'default' as const : 'muted' as const,
    },
    ...(result?.status === 'success'
      ? [{ label: 'Preview data', onClick: () => loadTablePreview(id, tableName) }]
      : []),
  ]

  return (
    <div>
      <Handle type="target" position={Position.Left} className="!bg-purple-400 !w-3 !h-3" />
      <NodeCard
        kind="transform"
        icon="⚙️"
        title={(d.label as string) || 'Transform'}
        tableName={tableName}
        subtitle={<span className="italic">{sqlPreview}</span>}
        result={result ? toResultLike(result, elapsed, mode) : undefined}
        onSelect={() => setSelectedNodeId(id)}
        onViewError={result?.status === 'error' ? () => openNodeError(id) : undefined}
        actions={actions}
      >
        <NodeCacheChip nodeId={id} />
      </NodeCard>
      <Handle type="source" position={Position.Right} className="!bg-purple-400 !w-3 !h-3" />
    </div>
  )
}
