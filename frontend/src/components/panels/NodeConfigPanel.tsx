import { usePipelineStore } from '../../store/pipelineStore'
import SqlEditor from './SqlEditor'
import { uploadCsv } from '../../api/client'
import { useCallback, useEffect, useRef, useState } from 'react'

export default function NodeConfigPanel() {
  const selectedNodeId = usePipelineStore((s) => s.selectedNodeId)
  const nodes = usePipelineStore((s) => s.nodes)
  const edges = usePipelineStore((s) => s.edges)
  const updateNodeData = usePipelineStore((s) => s.updateNodeData)
  const deleteNode = usePipelineStore((s) => s.deleteNode)
  const executeSingleNode = usePipelineStore((s) => s.executeSingleNode)
  const runTransformPreview = usePipelineStore((s) => s.runTransformPreview)
  const nodeResults = usePipelineStore((s) => s.nodeResults)
  const fileInputRef = useRef<HTMLInputElement>(null)
  const [isCsvEditing, setIsCsvEditing] = useState(false)
  const [csvDraft, setCsvDraft] = useState({ label: '', tableName: '' })
  const [csvSnapshot, setCsvSnapshot] = useState({ label: '', tableName: '' })

  const node = nodes.find((n) => n.id === selectedNodeId)
  const nodeId = node?.id ?? null
  const d = (node?.data as Record<string, unknown> | undefined) ?? {}
  const config = (d.config as Record<string, unknown> | undefined) ?? {}
  const tableName = (d.tableName as string | undefined) ?? ''
  const nodeResult = nodeId ? nodeResults[nodeId] : undefined
  const transformSql = ((config.sql as string | undefined) ?? '').trim()
  const isCsvNode = node?.type === 'csv_source'
  const csvLabel = isCsvNode ? ((d.label as string) || '') : ''
  const labelInputId = nodeId ? `${nodeId}-label` : 'node-label'
  const tableNameInputId = nodeId ? `${nodeId}-table-name` : 'node-table-name'

  const upstreamTableNames = useCallback(() => {
    if (!selectedNodeId) return []
    const upstreamIds = edges.filter((e) => e.target === selectedNodeId).map((e) => e.source)
    return nodes
      .filter((n) => upstreamIds.includes(n.id))
      .map((n) => (n.data as Record<string, unknown>).tableName as string)
  }, [selectedNodeId, edges, nodes])

  useEffect(() => {
    setIsCsvEditing(false)
  }, [nodeId])

  useEffect(() => {
    if (!isCsvNode) {
      setIsCsvEditing(false)
      return
    }

    if (!isCsvEditing) {
      const nextMetadata = { label: csvLabel, tableName }
      setCsvDraft(nextMetadata)
      setCsvSnapshot(nextMetadata)
    }
  }, [csvLabel, isCsvEditing, isCsvNode, tableName])

  const hasCsvMetadataChanges = isCsvEditing
    && (csvDraft.label !== csvSnapshot.label || csvDraft.tableName !== csvSnapshot.tableName)

  const handleCsvUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file || !nodeId) return
    const result = await uploadCsv(file)
    updateNodeData(nodeId, {
      config: { file_path: result.file_path, original_filename: result.filename },
    })
  }

  const startCsvEditing = () => {
    if (!isCsvNode) return
    const nextMetadata = { label: csvLabel, tableName }
    setCsvDraft(nextMetadata)
    setCsvSnapshot(nextMetadata)
    setIsCsvEditing(true)
  }

  const discardCsvChanges = () => {
    setCsvDraft(csvSnapshot)
    setIsCsvEditing(false)
  }

  const saveCsvChanges = () => {
    if (!hasCsvMetadataChanges) return
    if (!nodeId) return
    updateNodeData(nodeId, {
      label: csvDraft.label,
      tableName: csvDraft.tableName,
    })
    setIsCsvEditing(false)
  }

  if (!node) {
    return (
      <div className="w-80 border-l border-gray-200 bg-white p-4 flex items-center justify-center text-gray-400 text-sm">
        Select a node to configure
      </div>
    )
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

        {node.type === 'csv_source' ? (
          <>
            <div className="flex justify-between items-center mb-3">
              <span className="text-xs font-semibold uppercase tracking-wide text-gray-500">Metadata</span>
              {isCsvEditing ? (
                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    onClick={discardCsvChanges}
                    className="text-xs text-gray-500 hover:text-gray-700"
                  >
                    Discard
                  </button>
                  <button
                    type="button"
                    onClick={saveCsvChanges}
                    disabled={!hasCsvMetadataChanges}
                    className={`rounded px-2 py-1 text-xs font-medium transition ${
                      hasCsvMetadataChanges
                        ? 'bg-blue-500 text-white hover:bg-blue-600'
                        : 'bg-gray-100 text-gray-400'
                    }`}
                  >
                    Save
                  </button>
                </div>
              ) : (
                <button
                  type="button"
                  onClick={startCsvEditing}
                  className="text-xs text-blue-500 hover:text-blue-700"
                >
                  Edit
                </button>
              )}
            </div>

            <label htmlFor={labelInputId} className="block text-xs text-gray-500 mb-1">Label</label>
            {isCsvEditing ? (
              <input
                id={labelInputId}
                type="text"
                value={csvDraft.label}
                onChange={(e) => setCsvDraft((current) => ({ ...current, label: e.target.value }))}
                className="w-full border border-gray-300 rounded px-2 py-1 text-sm mb-3"
              />
            ) : (
              <div className="mb-3 rounded border border-gray-200 bg-gray-50 px-2 py-1.5 text-sm text-gray-700">
                {(d.label as string) || 'CSV Source'}
              </div>
            )}

            <label htmlFor={tableNameInputId} className="block text-xs text-gray-500 mb-1">Table Name</label>
            {isCsvEditing ? (
              <input
                id={tableNameInputId}
                type="text"
                value={csvDraft.tableName}
                onChange={(e) => setCsvDraft((current) => ({ ...current, tableName: e.target.value }))}
                className="w-full border border-gray-300 rounded px-2 py-1 text-sm font-mono"
              />
            ) : (
              <div className="rounded border border-gray-200 bg-gray-50 px-2 py-1.5 text-sm font-mono text-gray-700">
                {tableName}
              </div>
            )}
          </>
        ) : (
          <>
            <label htmlFor={labelInputId} className="block text-xs text-gray-500 mb-1">Label</label>
            <input
              id={labelInputId}
              type="text"
              value={(d.label as string) || ''}
              onChange={(e) => updateNodeData(node.id, { label: e.target.value })}
              className="w-full border border-gray-300 rounded px-2 py-1 text-sm mb-3"
            />

            <label htmlFor={tableNameInputId} className="block text-xs text-gray-500 mb-1">Table Name</label>
            <input
              id={tableNameInputId}
              type="text"
              value={tableName}
              onChange={(e) => updateNodeData(node.id, { tableName: e.target.value })}
              className="w-full border border-gray-300 rounded px-2 py-1 text-sm font-mono"
            />
          </>
        )}
      </div>

      <div className="p-4">
        {node.type === 'csv_source' && (
          <div className="space-y-6">
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

            <div>
              <div className="flex items-center justify-between mb-2">
                <label className="block text-xs text-gray-500">Run Node</label>
                {nodeResult && (
                  <span className="text-xs text-gray-400">
                    {nodeResult.status === 'running' ? 'Running...' : `Status: ${nodeResult.status}`}
                  </span>
                )}
              </div>
              <p className="mb-3 text-xs text-gray-500">
                Execute this CSV source and open the data preview on success.
              </p>
              <button
                type="button"
                onClick={() => void executeSingleNode(node.id, { loadPreviewOnSuccess: true })}
                disabled={!config.file_path || nodeResult?.status === 'running'}
                className={`w-full rounded px-3 py-2 text-sm font-medium transition ${
                  !config.file_path || nodeResult?.status === 'running'
                    ? 'bg-gray-100 text-gray-400'
                    : 'bg-blue-500 text-white hover:bg-blue-600'
                }`}
              >
                {nodeResult?.status === 'running' ? 'Running...' : 'Run and Preview'}
              </button>
            </div>
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
            <div className="mt-4">
              <div className="flex items-center justify-between mb-2">
                <label className="block text-xs text-gray-500">Run Node</label>
                {nodeResult && (
                  <span className="text-xs text-gray-400">
                    {nodeResult.status === 'running' ? 'Running...' : `Status: ${nodeResult.status}`}
                  </span>
                )}
              </div>
              <p className="mb-3 text-xs text-gray-500">
                Execute this transform and open its preview. Missing upstream tables will prompt before running dependencies.
              </p>
              <button
                type="button"
                onClick={() => void runTransformPreview(node.id)}
                disabled={!transformSql || nodeResult?.status === 'running'}
                className={`w-full rounded px-3 py-2 text-sm font-medium transition ${
                  !transformSql || nodeResult?.status === 'running'
                    ? 'bg-gray-100 text-gray-400'
                    : 'bg-purple-500 text-white hover:bg-purple-600'
                }`}
              >
                {nodeResult?.status === 'running' ? 'Running...' : 'Run and Preview'}
              </button>
            </div>
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
