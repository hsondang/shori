import { Handle, Position, type NodeProps } from '@xyflow/react'
import { NodeCard } from '@shori/design-system'
import { usePipelineStore } from '../../../store/pipelineStore'
import { computeWorkbookRollup } from '../../../lib/workbookRollup'
import type { ExcelWorkbookConfig } from '../../../types/pipeline'

/**
 * The workbook hub (docs/excel-node-model.md): a connection-like node with no
 * table, no data state, and no run actions. Its badge is a display-only rollup
 * of its sheet nodes' results; the source handle emits only structural edges
 * (created by the sheet picker, never by user drags).
 */
export default function ExcelWorkbookNode({ id, data }: NodeProps) {
  const edges = usePipelineStore((s) => s.edges)
  const nodeResults = usePipelineStore((s) => s.nodeResults)
  const setSelectedNodeId = usePipelineStore((s) => s.setSelectedNodeId)
  const openSheetPicker = usePipelineStore((s) => s.openSheetPicker)
  const d = data as Record<string, unknown>
  const config = d.config as ExcelWorkbookConfig
  const rollup = computeWorkbookRollup({ hubId: id, edges, nodeResults })
  const actions = config.sheet_names.length > 0
    ? [{ label: 'Add sheets…', onClick: () => openSheetPicker(id) }]
    : []

  const sheetCount = config.sheet_names.length
  const subtitle = config.original_filename
    ? `${config.original_filename} · ${sheetCount} ${sheetCount === 1 ? 'sheet' : 'sheets'}`
    : 'No workbook uploaded'

  return (
    <div>
      <NodeCard
        kind="workbook"
        icon="▤"
        title={(d.label as string) || 'Excel Workbook'}
        subtitle={subtitle}
        onSelect={() => setSelectedNodeId(id)}
        actions={actions}
      >
        {rollup.label && (
          <div
            className={`mt-1 inline-flex items-center gap-1 rounded px-1.5 py-0.5 text-xs ${
              rollup.kind === 'error'
                ? 'bg-red-100 text-red-700'
                : rollup.kind === 'mixed'
                  ? 'bg-amber-100 text-amber-700'
                  : 'bg-emerald-100 text-emerald-700'
            }`}
          >
            {rollup.label}
          </div>
        )}
      </NodeCard>
      <Handle type="source" position={Position.Right} className="!bg-emerald-700 !w-3 !h-3" />
    </div>
  )
}
