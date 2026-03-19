import { usePipelineStore } from '../../store/pipelineStore'
import SqlEditor from './SqlEditor'
import { uploadCsv } from '../../api/client'
import { useCallback, useEffect, useRef, useState } from 'react'
import type { CsvPreprocessingConfig, CsvSourceConfig } from '../../types/pipeline'
import { getCsvPreprocessFingerprint } from '../../lib/csvPreprocessing'

export default function NodeConfigPanel() {
  const selectedNodeId = usePipelineStore((s) => s.selectedNodeId)
  const nodes = usePipelineStore((s) => s.nodes)
  const edges = usePipelineStore((s) => s.edges)
  const updateNodeData = usePipelineStore((s) => s.updateNodeData)
  const deleteNode = usePipelineStore((s) => s.deleteNode)
  const executeSingleNode = usePipelineStore((s) => s.executeSingleNode)
  const runTransformPreview = usePipelineStore((s) => s.runTransformPreview)
  const loadCsvPreview = usePipelineStore((s) => s.loadCsvPreview)
  const loadPreprocessedCsvPreview = usePipelineStore((s) => s.loadPreprocessedCsvPreview)
  const csvPreprocessArtifacts = usePipelineStore((s) => s.csvPreprocessArtifacts)
  const nodeResults = usePipelineStore((s) => s.nodeResults)
  const fileInputRef = useRef<HTMLInputElement>(null)
  const [isCsvEditing, setIsCsvEditing] = useState(false)
  const [csvDraft, setCsvDraft] = useState({ label: '', tableName: '' })
  const [csvSnapshot, setCsvSnapshot] = useState({ label: '', tableName: '' })

  const node = nodes.find((n) => n.id === selectedNodeId)
  const nodeId = node?.id ?? null
  const d = (node?.data as Record<string, unknown> | undefined) ?? {}
  const config = (d.config as Record<string, unknown> | undefined) ?? {}
  const isCsvNode = node?.type === 'csv_source'
  const csvConfig = (isCsvNode ? config : null) as CsvSourceConfig | null
  const csvPreprocessing: CsvPreprocessingConfig = csvConfig?.preprocessing ?? {
    enabled: false,
    runtime: 'python',
    script: '',
  }
  const tableName = (d.tableName as string | undefined) ?? ''
  const nodeResult = nodeId ? nodeResults[nodeId] : undefined
  const transformSql = ((config.sql as string | undefined) ?? '').trim()
  const csvLabel = isCsvNode ? ((d.label as string) || '') : ''
  const labelInputId = nodeId ? `${nodeId}-label` : 'node-label'
  const tableNameInputId = nodeId ? `${nodeId}-table-name` : 'node-table-name'
  const preprocessingEditorId = nodeId ? `${nodeId}-preprocessing-script` : 'preprocessing-script'
  const canPreviewCsv = Boolean(csvConfig?.file_path) && nodeResult?.status !== 'running'
  const preprocessFingerprint = getCsvPreprocessFingerprint(csvConfig)
  const hasReviewedPreprocess = Boolean(
    nodeId
    && preprocessFingerprint
    && csvPreprocessArtifacts[nodeId] === preprocessFingerprint
  )
  const canRunPreprocess = Boolean(csvConfig?.file_path)
    && csvPreprocessing.enabled
    && Boolean(csvPreprocessing.script.trim())
    && nodeResult?.status !== 'running'
  const canLoadCsv = Boolean(csvConfig?.file_path)
    && nodeResult?.status !== 'running'
    && (!csvPreprocessing.enabled || hasReviewedPreprocess)

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
    const existingConfig = (d.config as CsvSourceConfig | undefined) ?? {
      file_path: '',
      original_filename: '',
      preprocessing: csvPreprocessing,
    }
    updateNodeData(nodeId, {
      config: {
        ...existingConfig,
        file_path: result.file_path,
        original_filename: result.filename,
        preprocessing: existingConfig.preprocessing ?? csvPreprocessing,
      },
    })
  }

  const updateCsvConfig = (patch: Partial<CsvSourceConfig>) => {
    if (!nodeId || !csvConfig) return
    updateNodeData(nodeId, {
      config: {
        ...csvConfig,
        ...patch,
        preprocessing: patch.preprocessing ?? csvPreprocessing,
      },
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
                {csvConfig?.original_filename || 'Click to upload CSV'}
              </button>
            </div>

            <div>
              <div className="flex items-center justify-between mb-2">
                <label className="block text-xs text-gray-500">Preprocessing</label>
                <label className="flex items-center gap-2 text-xs text-gray-500">
                  <input
                    type="checkbox"
                    checked={csvPreprocessing.enabled}
                    onChange={(e) => updateCsvConfig({
                      preprocessing: {
                        ...csvPreprocessing,
                        enabled: e.target.checked,
                      },
                    })}
                  />
                  Enable
                </label>
              </div>
              <p className="mb-3 text-xs text-gray-500">
                When enabled, the script receives the uploaded CSV path as the first argument and via <code>SHORI_INPUT_CSV</code>. It must emit a cleaned CSV to stdout.
              </p>
              <div className="space-y-3 rounded-lg border border-gray-200 bg-gray-50 p-3">
                <div>
                  <label className="block text-xs text-gray-500 mb-1">Runtime</label>
                  <select
                    value={csvPreprocessing.runtime}
                    onChange={(e) => updateCsvConfig({
                      preprocessing: {
                        ...csvPreprocessing,
                        runtime: e.target.value as CsvPreprocessingConfig['runtime'],
                      },
                    })}
                    disabled={!csvPreprocessing.enabled}
                    className="w-full border border-gray-300 rounded px-2 py-1 text-sm disabled:bg-gray-100 disabled:text-gray-400"
                  >
                    <option value="python">Python</option>
                    <option value="bash">Bash</option>
                  </select>
                </div>
                <div>
                  <label htmlFor={preprocessingEditorId} className="block text-xs text-gray-500 mb-1">Script</label>
                  <textarea
                    id={preprocessingEditorId}
                    value={csvPreprocessing.script}
                    onChange={(e) => updateCsvConfig({
                      preprocessing: {
                        ...csvPreprocessing,
                        script: e.target.value,
                      },
                    })}
                    disabled={!csvPreprocessing.enabled}
                    rows={8}
                    spellCheck={false}
                    className="w-full rounded border border-gray-300 bg-white px-2 py-1.5 text-sm font-mono disabled:bg-gray-100 disabled:text-gray-400"
                    placeholder={csvPreprocessing.runtime === 'bash'
                      ? 'tail -n +3 "$1"'
                      : 'import sys\nfrom pathlib import Path\nlines = Path(sys.argv[1]).read_text().splitlines()[2:]\nsys.stdout.write("\\n".join(lines))'}
                  />
                </div>
              </div>
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
                Preview the uploaded CSV before materializing it, then load it into DuckDB once the preprocessing is ready.
              </p>
              <div className="grid grid-cols-3 gap-3">
                <button
                  type="button"
                  onClick={() => csvConfig?.file_path && void loadCsvPreview(node.id, csvConfig.file_path)}
                  disabled={!canPreviewCsv}
                  className={`rounded px-3 py-2 text-sm font-medium transition ${
                    canPreviewCsv
                      ? 'border border-blue-200 bg-blue-50 text-blue-700 hover:bg-blue-100'
                      : 'bg-gray-100 text-gray-400'
                  }`}
                >
                  Preview data
                </button>
                <button
                  type="button"
                  onClick={() => csvConfig?.file_path && void loadPreprocessedCsvPreview(node.id, csvConfig.file_path, csvPreprocessing)}
                  disabled={!canRunPreprocess}
                  className={`rounded px-3 py-2 text-sm font-medium transition ${
                    canRunPreprocess
                      ? 'border border-emerald-200 bg-emerald-50 text-emerald-700 hover:bg-emerald-100'
                      : 'bg-gray-100 text-gray-400'
                  }`}
                >
                  Preprocess
                </button>
                <button
                  type="button"
                  onClick={() => void executeSingleNode(node.id, { loadPreviewOnSuccess: true })}
                  disabled={!canLoadCsv}
                  className={`rounded px-3 py-2 text-sm font-medium transition ${
                    canLoadCsv
                      ? 'bg-blue-500 text-white hover:bg-blue-600'
                      : 'bg-gray-100 text-gray-400'
                  }`}
                >
                  {nodeResult?.status === 'running' ? 'Running...' : 'Load data'}
                </button>
              </div>
              {csvPreprocessing.enabled && !csvPreprocessing.script.trim() && (
                <p className="mt-2 text-xs text-amber-600">
                  Add a preprocessing script before running Preprocess.
                </p>
              )}
              {csvPreprocessing.enabled && csvPreprocessing.script.trim() && !hasReviewedPreprocess && (
                <p className="mt-2 text-xs text-amber-600">
                  Run Preprocess and review the output before loading data.
                </p>
              )}
              {csvPreprocessing.enabled && hasReviewedPreprocess && (
                <p className="mt-2 text-xs text-emerald-600">
                  Reviewed preprocess output is ready to load into DuckDB.
                </p>
              )}
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
