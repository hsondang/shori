import { useState } from 'react'
import { usePipelineStore } from '../../store/pipelineStore'
import { getRunElapsedLabel } from '../../lib/executionTiming'
import type { NodeType } from '../../types/pipeline'
import { NODE_TYPE_MIME } from '../../lib/dragData'
import DatabaseSourcePicker from './DatabaseSourcePicker'

const nodeTypeOptions: { type: NodeType; label: string; color: string }[] = [
  { type: 'csv_source', label: 'CSV Source', color: 'bg-blue-100 text-blue-700 border-blue-300' },
  { type: 'transform', label: 'Transform', color: 'bg-purple-100 text-purple-700 border-purple-300' },
  { type: 'export', label: 'Export', color: 'bg-green-100 text-green-700 border-green-300' },
]

export default function Toolbar() {
  const pipelineName = usePipelineStore((s) => s.pipelineName)
  const hasUnsavedChanges = usePipelineStore((s) => s.hasUnsavedChanges)
  const setPipelineName = usePipelineStore((s) => s.setPipelineName)
  const executePipeline = usePipelineStore((s) => s.executePipeline)
  const activePipelineExecutionId = usePipelineStore((s) => s.activePipelineExecutionId)
  const activeExecutions = usePipelineStore((s) => s.activeExecutions)
  const executionClockNow = usePipelineStore((s) => s.executionClockNow)
  const savePipeline = usePipelineStore((s) => s.savePipeline)
  const [saving, setSaving] = useState(false)
  const activePipelineExecution = activePipelineExecutionId
    ? activeExecutions[activePipelineExecutionId] ?? null
    : null
  const pipelineElapsedLabel = getRunElapsedLabel(activePipelineExecution, executionClockNow)

  const handleSave = async () => {
    setSaving(true)
    try { await savePipeline() } finally { setSaving(false) }
  }

  const handleExecute = async (force: boolean) => {
    await executePipeline(force)
  }

  const onDragStart = (e: React.DragEvent, type: NodeType) => {
    e.dataTransfer.setData(NODE_TYPE_MIME, type)
    e.dataTransfer.effectAllowed = 'move'
  }

  return (
    <div className="relative flex min-w-0 items-center gap-3 bg-white px-4 py-2">
      <div className="min-w-0 mr-2">
        <div className="text-[11px] font-semibold uppercase tracking-[0.24em] text-gray-400">Project</div>
        <div className="flex items-center gap-2">
          <input
            type="text"
            value={pipelineName}
            onChange={(e) => setPipelineName(e.target.value)}
            className="border border-gray-300 rounded px-2 py-1 text-sm w-56"
          />
          <span className={`text-xs font-medium ${hasUnsavedChanges ? 'text-amber-600' : 'text-emerald-600'}`}>
            {hasUnsavedChanges ? 'Unsaved changes' : 'Saved'}
          </span>
        </div>
      </div>

      <div className="h-6 w-px bg-gray-300" />

      {nodeTypeOptions.map((opt) => (
        <div
          key={opt.type}
          draggable
          onDragStart={(e) => onDragStart(e, opt.type)}
          className={`border rounded px-2 py-1 text-xs font-medium cursor-grab active:cursor-grabbing select-none ${opt.color}`}
        >
          {opt.label}
        </div>
      ))}
      <DatabaseSourcePicker />

      <div className="h-6 w-px bg-gray-300" />
      <button onClick={handleSave} disabled={saving} className="px-3 py-1 text-sm border border-gray-300 rounded hover:bg-gray-50 disabled:opacity-50">
        {saving ? 'Saving...' : 'Save'}
      </button>

      <div className="h-6 w-px bg-gray-300" />

      {activePipelineExecution && (
        <div className="rounded border border-amber-200 bg-amber-50 px-3 py-1 text-xs font-medium text-amber-700">
          Running pipeline{pipelineElapsedLabel ? ` · ${pipelineElapsedLabel}` : ''}
        </div>
      )}

      <button
        onClick={() => handleExecute(false)}
        disabled={Boolean(activePipelineExecution)}
        className="px-3 py-1 text-sm bg-green-500 text-white rounded hover:bg-green-600 disabled:opacity-50"
      >
        {activePipelineExecution ? 'Running...' : 'Execute'}
      </button>
      <button
        onClick={() => handleExecute(true)}
        disabled={Boolean(activePipelineExecution)}
        className="px-3 py-1 text-sm bg-orange-500 text-white rounded hover:bg-orange-600 disabled:opacity-50"
      >
        Force Refresh
      </button>
    </div>
  )
}
