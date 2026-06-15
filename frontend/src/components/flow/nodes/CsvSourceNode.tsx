import { Handle, Position, type NodeProps } from '@xyflow/react'
import { NodeCard } from '@shori/design-system'
import { usePipelineStore } from '../../../store/pipelineStore'
import NodeCacheChip from '../NodeCacheChip'
import { toResultLike } from '../../../lib/dsStatus'
import { getResultElapsedLabel } from '../../../lib/executionTiming'
import type { CsvSourceConfig } from '../../../types/pipeline'

export default function CsvSourceNode({ id, data }: NodeProps) {
  const nodeResults = usePipelineStore((s) => s.nodeResults)
  const executionClockNow = usePipelineStore((s) => s.executionClockNow)
  const setSelectedNodeId = usePipelineStore((s) => s.setSelectedNodeId)
  const openNodeError = usePipelineStore((s) => s.openNodeError)
  const loadCsvPreview = usePipelineStore((s) => s.loadCsvPreview)
  const result = nodeResults[id]
  const d = data as Record<string, unknown>
  const config = d.config as CsvSourceConfig
  const tableName = d.tableName as string
  const elapsed = result ? getResultElapsedLabel(result, executionClockNow) : null

  return (
    <div>
      <NodeCard
        kind="csv"
        icon="📄"
        title={(d.label as string) || 'CSV Source'}
        tableName={tableName}
        subtitle={config.original_filename || undefined}
        result={result ? toResultLike(result, elapsed) : undefined}
        onSelect={() => setSelectedNodeId(id)}
        onViewError={result?.status === 'error' ? () => openNodeError(id) : undefined}
        actions={config.file_path ? [{ label: 'Preview data', onClick: () => loadCsvPreview(id, config.file_path) }] : []}
      >
        <NodeCacheChip nodeId={id} />
      </NodeCard>
      <Handle type="source" position={Position.Right} className="!bg-blue-400 !w-3 !h-3" />
    </div>
  )
}
