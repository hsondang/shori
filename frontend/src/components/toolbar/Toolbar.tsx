import { useState } from 'react'
import { usePipelineStore } from '../../store/pipelineStore'
import { listPipelines } from '../../api/client'
import type { NodeType } from '../../types/pipeline'

const nodeTypeOptions: { type: NodeType; label: string; color: string }[] = [
  { type: 'csv_source', label: 'CSV Source', color: 'bg-blue-100 text-blue-700 border-blue-300' },
  { type: 'db_source', label: 'Database Source', color: 'bg-indigo-100 text-indigo-700 border-indigo-300' },
  { type: 'transform', label: 'Transform', color: 'bg-purple-100 text-purple-700 border-purple-300' },
  { type: 'export', label: 'Export', color: 'bg-green-100 text-green-700 border-green-300' },
]

export default function Toolbar() {
  const pipelineName = usePipelineStore((s) => s.pipelineName)
  const setPipelineName = usePipelineStore((s) => s.setPipelineName)
  const executePipeline = usePipelineStore((s) => s.executePipeline)
  const savePipeline = usePipelineStore((s) => s.savePipeline)
  const loadPipeline = usePipelineStore((s) => s.loadPipeline)
  const newPipeline = usePipelineStore((s) => s.newPipeline)
  const [saving, setSaving] = useState(false)
  const [executing, setExecuting] = useState(false)
  const [showLoadDialog, setShowLoadDialog] = useState(false)
  const [pipelines, setPipelines] = useState<Array<{ id: string; name: string }>>([])

  const handleSave = async () => {
    setSaving(true)
    try { await savePipeline() } finally { setSaving(false) }
  }

  const handleExecute = async (force: boolean) => {
    setExecuting(true)
    try { await executePipeline(force) } finally { setExecuting(false) }
  }

  const handleLoad = async () => {
    const list = await listPipelines()
    setPipelines(list)
    setShowLoadDialog(true)
  }

  const onDragStart = (e: React.DragEvent, type: NodeType) => {
    e.dataTransfer.setData('application/shori-node-type', type)
    e.dataTransfer.effectAllowed = 'move'
  }

  return (
    <div className="bg-white border-b border-gray-200 px-4 py-2 flex items-center gap-3 relative">
      <div className="font-bold text-gray-800 text-lg mr-2">Shori</div>

      <input
        type="text"
        value={pipelineName}
        onChange={(e) => setPipelineName(e.target.value)}
        className="border border-gray-300 rounded px-2 py-1 text-sm w-48"
      />

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

      <div className="h-6 w-px bg-gray-300" />

      <button onClick={handleLoad} className="px-3 py-1 text-sm border border-gray-300 rounded hover:bg-gray-50">
        Load
      </button>
      <button onClick={handleSave} disabled={saving} className="px-3 py-1 text-sm border border-gray-300 rounded hover:bg-gray-50 disabled:opacity-50">
        {saving ? 'Saving...' : 'Save'}
      </button>
      <button onClick={newPipeline} className="px-3 py-1 text-sm border border-gray-300 rounded hover:bg-gray-50">
        New
      </button>

      <div className="h-6 w-px bg-gray-300" />

      <button
        onClick={() => handleExecute(false)}
        disabled={executing}
        className="px-3 py-1 text-sm bg-green-500 text-white rounded hover:bg-green-600 disabled:opacity-50"
      >
        {executing ? 'Running...' : 'Execute'}
      </button>
      <button
        onClick={() => handleExecute(true)}
        disabled={executing}
        className="px-3 py-1 text-sm bg-orange-500 text-white rounded hover:bg-orange-600 disabled:opacity-50"
      >
        Force Refresh
      </button>

      {showLoadDialog && (
        <div className="absolute top-full left-0 right-0 bg-white border border-gray-200 shadow-lg z-50 p-4">
          <div className="flex justify-between items-center mb-2">
            <h3 className="font-semibold text-sm">Load Pipeline</h3>
            <button onClick={() => setShowLoadDialog(false)} className="text-gray-400 hover:text-gray-600 text-sm">Close</button>
          </div>
          {pipelines.length === 0 ? (
            <p className="text-sm text-gray-400">No saved pipelines</p>
          ) : (
            <div className="space-y-1">
              {pipelines.map((p) => (
                <button
                  key={p.id}
                  onClick={() => { loadPipeline(p.id); setShowLoadDialog(false) }}
                  className="block w-full text-left px-3 py-2 text-sm rounded hover:bg-gray-100"
                >
                  {p.name}
                </button>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
