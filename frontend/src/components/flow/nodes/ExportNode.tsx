import { Handle, Position, type NodeProps } from '@xyflow/react'
import { NodeCard } from '@shori/design-system'
import { usePipelineStore } from '../../../store/pipelineStore'
import { toResultLike } from '../../../lib/dsStatus'
import { getResultElapsedLabel } from '../../../lib/executionTiming'
import { exportData } from '../../../api/client'

export default function ExportNode({ id, data }: NodeProps) {
  const nodeResults = usePipelineStore((s) => s.nodeResults)
  const executionClockNow = usePipelineStore((s) => s.executionClockNow)
  const setSelectedNodeId = usePipelineStore((s) => s.setSelectedNodeId)
  const openNodeError = usePipelineStore((s) => s.openNodeError)
  const edges = usePipelineStore((s) => s.edges)
  const nodes = usePipelineStore((s) => s.nodes)
  const pipelineId = usePipelineStore((s) => s.pipelineId)
  const result = nodeResults[id]
  const d = data as Record<string, unknown>
  const elapsed = result ? getResultElapsedLabel(result, executionClockNow) : null

  const sourceEdge = edges.find((e) => e.target === id)
  const sourceNode = sourceEdge ? nodes.find((n) => n.id === sourceEdge.source) : null
  const sourceTableName = sourceNode
    ? (sourceNode.data as Record<string, unknown>).tableName as string
    : null

  const actions = sourceTableName
    ? [{ label: 'Download CSV', onClick: () => exportData(pipelineId, sourceTableName) }]
    : []

  return (
    <div>
      <Handle type="target" position={Position.Left} className="!bg-green-400 !w-3 !h-3" />
      <NodeCard
        kind="export"
        icon="📥"
        title={(d.label as string) || 'Export'}
        subtitle={sourceTableName ? `Source: ${sourceTableName}` : undefined}
        result={result ? toResultLike(result, elapsed) : undefined}
        onSelect={() => setSelectedNodeId(id)}
        onViewError={result?.status === 'error' ? () => openNodeError(id) : undefined}
        actions={actions}
      />
    </div>
  )
}
