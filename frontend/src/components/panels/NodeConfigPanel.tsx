import { usePipelineStore } from '../../store/pipelineStore'
import SqlEditor from './SqlEditor'
import { uploadCsv } from '../../api/client'
import { useCallback, useRef } from 'react'

export default function NodeConfigPanel() {
  const selectedNodeId = usePipelineStore((s) => s.selectedNodeId)
  const nodes = usePipelineStore((s) => s.nodes)
  const edges = usePipelineStore((s) => s.edges)
  const updateNodeData = usePipelineStore((s) => s.updateNodeData)
  const deleteNode = usePipelineStore((s) => s.deleteNode)
  const fileInputRef = useRef<HTMLInputElement>(null)

  const node = nodes.find((n) => n.id === selectedNodeId)

  const upstreamTableNames = useCallback(() => {
    if (!selectedNodeId) return []
    const upstreamIds = edges.filter((e) => e.target === selectedNodeId).map((e) => e.source)
    return nodes
      .filter((n) => upstreamIds.includes(n.id))
      .map((n) => (n.data as Record<string, unknown>).tableName as string)
  }, [selectedNodeId, edges, nodes])

  if (!node) {
    return (
      <div className="w-80 border-l border-gray-200 bg-white p-4 flex items-center justify-center text-gray-400 text-sm">
        Select a node to configure
      </div>
    )
  }

  const d = node.data as Record<string, unknown>
  const config = d.config as Record<string, unknown>
  const tableName = d.tableName as string

  const handleCsvUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return
    const result = await uploadCsv(file)
    updateNodeData(node.id, {
      config: { file_path: result.file_path, original_filename: result.filename },
    })
  }

  if (node.type === 'db_source') {
    return (
      <div className="w-80 border-l border-gray-200 bg-white overflow-y-auto">
        <div className="p-4 border-b border-gray-200">
          <div className="flex justify-between items-center">
            <h3 className="font-semibold text-sm text-gray-800">{(d.label as string) || 'Database Source'}</h3>
            <button
              onClick={() => deleteNode(node.id)}
              className="text-red-500 hover:text-red-700 text-xs"
            >
              Delete
            </button>
          </div>
        </div>

        <div className="p-4">
          <label className="block text-xs text-gray-500 mb-1">SQL Query</label>
          <SqlEditor
            value={(config.query as string) || ''}
            onChange={(sql) => updateNodeData(node.id, { config: { ...config, query: sql } })}
            upstreamTables={[]}
          />
        </div>
      </div>
    )
  }

  return (
    <div className="w-80 border-l border-gray-200 bg-white overflow-y-auto">
      <div className="p-4 border-b border-gray-200">
        <div className="flex justify-between items-center mb-3">
          <h3 className="font-semibold text-sm text-gray-800">Node Config</h3>
          <button
            onClick={() => deleteNode(node.id)}
            className="text-red-500 hover:text-red-700 text-xs"
          >
            Delete
          </button>
        </div>

        <label className="block text-xs text-gray-500 mb-1">Label</label>
        <input
          type="text"
          value={(d.label as string) || ''}
          onChange={(e) => updateNodeData(node.id, { label: e.target.value })}
          className="w-full border border-gray-300 rounded px-2 py-1 text-sm mb-3"
        />

        <label className="block text-xs text-gray-500 mb-1">Table Name</label>
        <input
          type="text"
          value={tableName}
          onChange={(e) => updateNodeData(node.id, { tableName: e.target.value })}
          className="w-full border border-gray-300 rounded px-2 py-1 text-sm font-mono"
        />
      </div>

      <div className="p-4">
        {node.type === 'csv_source' && (
          <div>
            <label className="block text-xs text-gray-500 mb-2">CSV File</label>
            <input ref={fileInputRef} type="file" accept=".csv" onChange={handleCsvUpload} className="hidden" />
            <button
              onClick={() => fileInputRef.current?.click()}
              className="w-full border-2 border-dashed border-gray-300 rounded-lg p-4 text-sm text-gray-500 hover:border-blue-400 hover:text-blue-500 transition"
            >
              {(config.original_filename as string) || 'Click to upload CSV'}
            </button>
          </div>
        )}

        {node.type === 'transform' && (
          <div>
            {upstreamTableNames().length > 0 && (
              <div className="mb-3">
                <label className="block text-xs text-gray-500 mb-1">Available Tables</label>
                <div className="flex flex-wrap gap-1">
                  {upstreamTableNames().map((t) => (
                    <span key={t} className="bg-purple-100 text-purple-700 px-2 py-0.5 rounded text-xs font-mono">
                      {t}
                    </span>
                  ))}
                </div>
              </div>
            )}
            <label className="block text-xs text-gray-500 mb-1">SQL Query</label>
            <SqlEditor
              value={(config.sql as string) || ''}
              onChange={(sql) => updateNodeData(node.id, { config: { ...config, sql } })}
              upstreamTables={upstreamTableNames()}
            />
          </div>
        )}

        {node.type === 'export' && (
          <div className="text-sm text-gray-500">
            Connect this node to a source to export its data as CSV.
          </div>
        )}
      </div>
    </div>
  )
}
