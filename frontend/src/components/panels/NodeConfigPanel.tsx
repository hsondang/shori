import { usePipelineStore } from '../../store/pipelineStore'
import SqlEditor from './SqlEditor'
import { uploadCsv } from '../../api/client'
import { useCallback, useEffect, useRef, useState } from 'react'
import type { ReactNode } from 'react'
import type {
  CsvPreprocessingConfig,
  CsvSourceConfig,
  DatabaseConnectionConfig,
  DbType,
  NodeLabelMode,
} from '../../types/pipeline'
import { getCsvPreprocessFingerprint } from '../../lib/csvPreprocessing'
import { getConnectionSummary } from '../../lib/databaseConnections'
import {
  NODE_CONFIG_PANEL_EXPANDED_MAX_WIDTH,
  NODE_CONFIG_PANEL_EXPANDED_MIN_WIDTH,
  NODE_CONFIG_PANEL_EXPANDED_WIDTH,
  NODE_CONFIG_PANEL_WIDTH_PX,
} from '../projects/pipelineEditorLayout'

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
  const [isDbEditMode, setIsDbEditMode] = useState(false)
  const [isTransformEditMode, setIsTransformEditMode] = useState(false)
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
  const dbQuery = ((config.query as string | undefined) ?? '')
  const dbType = ((config.db_type as string | undefined) ?? 'postgres') as DbType
  const dbConnection = config.connection as DatabaseConnectionConfig | undefined
  const transformQuery = (config.sql as string | undefined) ?? ''
  const csvLabel = isCsvNode ? ((d.label as string) || '') : ''
  const autoLabel = typeof d.autoLabel === 'string'
    ? d.autoLabel
    : node?.type === 'db_source'
      ? ((d.label as string) || 'Database Source')
      : node?.type
        ? ({ csv_source: 'CSV Source', db_source: 'Database Source', transform: 'Transform', export: 'Export' }[node.type] ?? '')
        : ''
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
  const availableUpstreamTables = node?.type === 'transform' ? upstreamTableNames() : []

  useEffect(() => {
    setIsCsvEditing(false)
    setIsDbEditMode(false)
    setIsTransformEditMode(false)
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
    const labelMode: NodeLabelMode = csvDraft.label === autoLabel ? 'auto' : 'custom'
    updateNodeData(nodeId, {
      label: csvDraft.label,
      labelMode,
      tableName: csvDraft.tableName,
    })
    setIsCsvEditing(false)
  }

  const renderQueryPanel = ({
    expanded,
    setExpanded,
    title,
    defaultLabel,
    queryValue,
    onQueryChange,
    canExecute,
    actionLabel,
    enabledButtonClassName,
    overlayTestId,
    description,
    metadata,
    extraEditorContent,
    onExecute,
  }: {
    expanded: boolean
    setExpanded: React.Dispatch<React.SetStateAction<boolean>>
    title: string
    defaultLabel: string
    queryValue: string
    onQueryChange: (query: string) => void
    canExecute: boolean
    actionLabel: string
    enabledButtonClassName: string
    overlayTestId: string
    description: string
    metadata: ReactNode
    extraEditorContent?: ReactNode
    onExecute: () => void
  }) => {
    return (
      <div
        data-testid="node-config-panel"
        data-layout-state={expanded ? 'expanded' : 'collapsed'}
        data-panel-kind={overlayTestId}
        className="flex min-h-0 shrink-0 flex-col overflow-hidden border-l border-gray-200 bg-white"
        style={expanded
          ? {
            width: NODE_CONFIG_PANEL_EXPANDED_WIDTH,
            minWidth: NODE_CONFIG_PANEL_EXPANDED_MIN_WIDTH,
            maxWidth: NODE_CONFIG_PANEL_EXPANDED_MAX_WIDTH,
          }
          : { width: `${NODE_CONFIG_PANEL_WIDTH_PX}px` }}
      >
        <div className="border-b border-gray-200 px-4 py-4">
          <div className="flex items-start justify-between gap-3">
            <div className="min-w-0">
              <div className="text-[11px] font-semibold uppercase tracking-[0.24em] text-gray-400">{title}</div>
              <h3 className="mt-2 truncate text-base font-semibold text-gray-900">
                {(d.label as string) || defaultLabel}
              </h3>
            </div>
            <button
              onClick={() => deleteNode(node.id)}
              className="shrink-0 text-xs text-red-500 hover:text-red-700"
            >
              Delete
            </button>
          </div>

          <div className="mt-4 rounded-2xl border border-stone-200 bg-stone-50 px-3 py-3">
            <div className="flex items-center justify-between gap-3">
              <div>
                <div className="text-xs font-semibold uppercase tracking-wide text-stone-500">Edit mode</div>
                <p className="mt-1 text-xs text-stone-500">
                  Expand the SQL editor when you need more room to read or write the query.
                </p>
              </div>
              <button
                type="button"
                aria-pressed={expanded}
                onClick={() => setExpanded((current) => !current)}
                className={`relative inline-flex h-7 w-14 shrink-0 items-center rounded-full px-1 transition ${
                  expanded ? 'bg-stone-900' : 'bg-stone-300'
                }`}
              >
                <span
                  className={`h-5 w-5 rounded-full bg-white shadow-sm transition ${
                    expanded ? 'translate-x-7' : 'translate-x-0'
                  }`}
                />
                <span className="sr-only">Edit mode</span>
              </button>
            </div>
          </div>

          <div className="mt-4 space-y-2">{metadata}</div>
        </div>

        <div className="flex min-h-0 flex-1 flex-col px-4 py-4">
          {extraEditorContent}
          <div className="mb-2 flex items-center justify-between">
            <label className="block text-xs font-semibold uppercase tracking-wide text-gray-500">SQL Query</label>
            {nodeResult && (
              <span className="text-xs text-gray-400">
                {nodeResult.status === 'running' ? 'Running...' : `Status: ${nodeResult.status}`}
              </span>
            )}
          </div>
          <div className="min-h-0 flex-1">
            <SqlEditor
              value={queryValue}
              onChange={onQueryChange}
              upstreamTables={availableUpstreamTables}
              height="100%"
              containerClassName="h-full"
            />
          </div>
        </div>

        <div className="border-t border-gray-200 bg-white px-4 py-4">
          <p className="mb-3 text-xs text-gray-500">{description}</p>
          <button
            type="button"
            onClick={onExecute}
            disabled={!canExecute}
            className={`w-full rounded-lg px-4 py-2 text-sm font-medium transition ${
              canExecute
                ? enabledButtonClassName
                : 'bg-gray-100 text-gray-400'
            }`}
          >
            {nodeResult?.status === 'running' ? 'Running...' : actionLabel}
          </button>
        </div>
      </div>
    )
  }

  if (!node) {
    return (
      <div
        data-testid="node-config-panel"
        data-layout-state="collapsed"
        className="flex shrink-0 items-center justify-center border-l border-gray-200 bg-white p-4 text-sm text-gray-400"
        style={{ width: `${NODE_CONFIG_PANEL_WIDTH_PX}px` }}
      >
        Select a node to configure
      </div>
    )
  }

  if (node.type === 'db_source') {
    const canExecute = Boolean(dbQuery.trim()) && nodeResult?.status !== 'running'

    return renderQueryPanel({
      expanded: isDbEditMode,
      setExpanded: setIsDbEditMode,
      title: 'Database Source',
      defaultLabel: 'Database Source',
      queryValue: dbQuery,
      onQueryChange: (query) => updateNodeData(node.id, { config: { ...config, query } }),
      canExecute,
      actionLabel: 'Execute',
      enabledButtonClassName: 'bg-emerald-500 text-white hover:bg-emerald-600',
      overlayTestId: 'db-edit-overlay',
      description: 'Execute this source query and open its preview.',
      metadata: (
        <>
          <div>
            <div className="text-xs font-semibold uppercase tracking-wide text-gray-500">Table</div>
            <div className="mt-1 rounded-lg border border-gray-200 bg-gray-50 px-3 py-2 font-mono text-sm text-gray-700">
              {tableName}
            </div>
          </div>
          {dbConnection && (
            <div>
              <div className="text-xs font-semibold uppercase tracking-wide text-gray-500">Connection</div>
              <div className="mt-1 rounded-lg border border-gray-200 bg-gray-50 px-3 py-2 text-sm text-gray-600">
                {getConnectionSummary(dbType, dbConnection)}
              </div>
            </div>
          )}
        </>
      ),
      onExecute: () => { void executeSingleNode(node.id, { loadPreviewOnSuccess: true }) },
    })
  }

  if (node.type === 'transform') {
    const canExecute = Boolean(transformSql) && nodeResult?.status !== 'running'

    return renderQueryPanel({
      expanded: isTransformEditMode,
      setExpanded: setIsTransformEditMode,
      title: 'Transform',
      defaultLabel: 'Transform',
      queryValue: transformQuery,
      onQueryChange: (query) => updateNodeData(node.id, { config: { ...config, sql: query } }),
      canExecute,
      actionLabel: 'Run and Preview',
      enabledButtonClassName: 'bg-purple-500 text-white hover:bg-purple-600',
      overlayTestId: 'transform-edit-overlay',
      description: 'Execute this transform and open its preview. Missing upstream tables will prompt before running dependencies.',
      metadata: (
        <div>
          <div className="text-xs font-semibold uppercase tracking-wide text-gray-500">Table</div>
          <div className="mt-1 rounded-lg border border-gray-200 bg-gray-50 px-3 py-2 font-mono text-sm text-gray-700">
            {tableName}
          </div>
        </div>
      ),
      extraEditorContent: availableUpstreamTables.length > 0 ? (
        <div className="mb-3">
          <label className="mb-1 block text-xs text-gray-500">Available Tables</label>
          <div className="flex flex-wrap gap-1">
            {availableUpstreamTables.map((upstreamTable) => (
              <span key={upstreamTable} className="rounded bg-purple-100 px-2 py-0.5 text-xs font-mono text-purple-700">
                {upstreamTable}
              </span>
            ))}
          </div>
        </div>
      ) : undefined,
      onExecute: () => { void runTransformPreview(node.id) },
    })
  }

  return (
    <div
      data-testid="node-config-panel"
      data-layout-state="collapsed"
      className="min-h-0 shrink-0 overflow-y-auto border-l border-gray-200 bg-white"
      style={{ width: `${NODE_CONFIG_PANEL_WIDTH_PX}px` }}
    >
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
              onChange={(e) => updateNodeData(node.id, {
                label: e.target.value,
                labelMode: e.target.value === autoLabel ? 'auto' : 'custom',
              })}
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
                <button
                  type="button"
                  role="switch"
                  aria-checked={csvPreprocessing.enabled}
                  aria-label="Enable preprocessing"
                  onClick={() => updateCsvConfig({
                    preprocessing: {
                      ...csvPreprocessing,
                      enabled: !csvPreprocessing.enabled,
                    },
                  })}
                  className={`relative inline-flex h-6 w-11 shrink-0 items-center rounded-full px-0.5 transition ${
                    csvPreprocessing.enabled ? 'bg-blue-500' : 'bg-gray-300'
                  }`}
                >
                  <span
                    className={`h-5 w-5 rounded-full bg-white shadow-sm transition ${
                      csvPreprocessing.enabled ? 'translate-x-5' : 'translate-x-0'
                    }`}
                  />
                </button>
              </div>
              {csvPreprocessing.enabled && (
                <>
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
                        className="w-full border border-gray-300 rounded px-2 py-1 text-sm"
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
                        rows={8}
                        spellCheck={false}
                        className="w-full rounded border border-gray-300 bg-white px-2 py-1.5 text-sm font-mono"
                        placeholder={csvPreprocessing.runtime === 'bash'
                          ? 'tail -n +3 "$1"'
                          : 'import sys\nfrom pathlib import Path\nlines = Path(sys.argv[1]).read_text().splitlines()[2:]\nsys.stdout.write("\\n".join(lines))'}
                      />
                    </div>
                  </div>
                </>
              )}
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

        {node.type === 'export' && (
          <div className="text-sm text-gray-500">
            Connect this node to a source to export its data as CSV.
          </div>
        )}
      </div>
    </div>
  )
}
