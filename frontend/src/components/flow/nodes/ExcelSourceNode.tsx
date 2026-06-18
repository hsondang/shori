import { Handle, Position, type NodeProps } from '@xyflow/react'
import { NodeCard } from '@shori/design-system'
import { usePipelineStore } from '../../../store/pipelineStore'
import NodeCacheChip from '../NodeCacheChip'
import { toResultLike } from '../../../lib/dsStatus'
import { getResultElapsedLabel } from '../../../lib/executionTiming'
import type { ExcelSourceConfig } from '../../../types/pipeline'

export default function ExcelSourceNode({ id, data }: NodeProps) {
  const nodeResults = usePipelineStore((s) => s.nodeResults)
  const executionClockNow = usePipelineStore((s) => s.executionClockNow)
  const setSelectedNodeId = usePipelineStore((s) => s.setSelectedNodeId)
  const openNodeError = usePipelineStore((s) => s.openNodeError)
  const runNodeWithLoadMode = usePipelineStore((s) => s.runNodeWithLoadMode)
  const loadTablePreview = usePipelineStore((s) => s.loadTablePreview)
  const result = nodeResults[id]
  const d = data as Record<string, unknown>
  const config = d.config as ExcelSourceConfig
  const tableName = d.tableName as string
  const elapsed = result ? getResultElapsedLabel(result, executionClockNow) : null
  const subtitle = [
    config.original_filename,
    config.selected_sheet ? `Sheet: ${config.selected_sheet}` : null,
  ].filter(Boolean).join(' · ') || undefined

  const canRun = Boolean(config.selected_sheet)
  const actions = [
    ...(canRun
      ? [
          { label: 'Load to memory', onClick: () => runNodeWithLoadMode(id, 'in_memory') },
          { label: 'Materialize', onClick: () => runNodeWithLoadMode(id, 'materialized') },
        ]
      : []),
    ...(result?.status === 'success'
      ? [{ label: 'View table', tone: 'muted' as const, onClick: () => loadTablePreview(id, tableName) }]
      : []),
  ]

  return (
    <div>
      <NodeCard
        kind="excel"
        icon="▦"
        title={(d.label as string) || 'Excel Source'}
        tableName={tableName}
        subtitle={subtitle}
        result={result ? toResultLike(result, elapsed) : undefined}
        onSelect={() => setSelectedNodeId(id)}
        onViewError={result?.status === 'error' ? () => openNodeError(id) : undefined}
        actions={actions}
      >
        <NodeCacheChip nodeId={id} />
      </NodeCard>
      <Handle type="source" position={Position.Right} className="!bg-emerald-500 !w-3 !h-3" />
    </div>
  )
}
