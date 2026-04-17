import { uploadCsv } from '../../api/client'
import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ChangeEvent,
  type Dispatch,
  type MouseEvent as ReactMouseEvent,
  type ReactNode,
  type SetStateAction,
} from 'react'
import { usePipelineStore } from '../../store/pipelineStore'
import { getConnectionSummary } from '../../lib/databaseConnections'
import { getCsvPreprocessFingerprint } from '../../lib/csvPreprocessing'
import { getResultElapsedLabel } from '../../lib/executionTiming'
import SqlEditor from './SqlEditor'
import type {
  CsvPreprocessingConfig,
  CsvSourceConfig,
  DatabaseConnectionConfig,
  DbType,
} from '../../types/pipeline'
import {
  clampNodeConfigPanelWidth,
  getDefaultExpandedNodeConfigPanelWidth,
  NODE_CONFIG_PANEL_WIDTH_PX,
} from '../projects/pipelineEditorLayout'

function getNodeTitle(type?: string): string {
  switch (type) {
    case 'csv_source':
      return 'CSV Source'
    case 'db_source':
      return 'Database Source'
    case 'transform':
      return 'Transform'
    case 'export':
      return 'Export'
    default:
      return 'Node'
  }
}

function QueryPreview({
  title,
  value,
  emptyLabel,
}: {
  title: string
  value: string
  emptyLabel: string
}) {
  return (
    <div>
      <div className="mb-2 text-xs font-semibold uppercase tracking-wide text-gray-500">{title}</div>
      <div className="rounded-xl border border-gray-200 bg-gray-50 p-3">
        <pre className="m-0 whitespace-pre-wrap break-words text-sm text-gray-700">
          {value.trim() || emptyLabel}
        </pre>
      </div>
    </div>
  )
}

function NodeConfigPanelShell({
  widthPx,
  layoutState,
  onResizeStart,
  children,
}: {
  widthPx: number
  layoutState: 'collapsed' | 'expanded'
  onResizeStart: (event: ReactMouseEvent<HTMLDivElement>) => void
  children: ReactNode
}) {
  return (
    <div
      data-testid="node-config-panel"
      data-layout-state={layoutState}
      className="flex min-h-0 shrink-0 overflow-hidden bg-white"
      style={{ width: `${widthPx}px` }}
    >
      <div
        role="separator"
        aria-label="Resize node configuration panel"
        aria-orientation="vertical"
        data-testid="node-config-panel-resize-handle"
        onMouseDown={onResizeStart}
        className="group flex w-3 shrink-0 cursor-col-resize items-center justify-center bg-white"
      >
        <div className="h-16 w-1 rounded-full bg-stone-200 transition group-hover:bg-stone-300" />
      </div>
      <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden border-l border-gray-200 bg-white">
        {children}
      </div>
    </div>
  )
}

export default function NodeConfigPanel() {
  const selectedNodeId = usePipelineStore((s) => s.selectedNodeId)
  const nodes = usePipelineStore((s) => s.nodes)
  const edges = usePipelineStore((s) => s.edges)
  const updateNodeData = usePipelineStore((s) => s.updateNodeData)
  const deleteNode = usePipelineStore((s) => s.deleteNode)
  const openEditNodeEditor = usePipelineStore((s) => s.openEditNodeEditor)
  const executeSingleNode = usePipelineStore((s) => s.executeSingleNode)
  const abortDatabaseNodeExecution = usePipelineStore((s) => s.abortDatabaseNodeExecution)
  const runTransformPreview = usePipelineStore((s) => s.runTransformPreview)
  const loadCsvPreview = usePipelineStore((s) => s.loadCsvPreview)
  const loadPreprocessedCsvPreview = usePipelineStore((s) => s.loadPreprocessedCsvPreview)
  const csvPreprocessArtifacts = usePipelineStore((s) => s.csvPreprocessArtifacts)
  const nodeResults = usePipelineStore((s) => s.nodeResults)
  const executionClockNow = usePipelineStore((s) => s.executionClockNow)
  const fileInputRef = useRef<HTMLInputElement>(null)
  const menuRef = useRef<HTMLDivElement>(null)
  const resizeStateRef = useRef<{ expanded: boolean; startX: number; startWidthPx: number } | null>(null)
  const [menuOpen, setMenuOpen] = useState(false)
  const [isDbEditMode, setIsDbEditMode] = useState(false)
  const [isTransformEditMode, setIsTransformEditMode] = useState(false)
  const [collapsedWidthPx, setCollapsedWidthPx] = useState(NODE_CONFIG_PANEL_WIDTH_PX)
  const [expandedWidthPx, setExpandedWidthPx] = useState(() =>
    getDefaultExpandedNodeConfigPanelWidth(typeof window === 'undefined' ? 0 : window.innerWidth)
  )

  const node = nodes.find((candidate) => candidate.id === selectedNodeId)
  const nodeId = node?.id ?? null
  const data = (node?.data as Record<string, unknown> | undefined) ?? {}
  const config = (data.config as Record<string, unknown> | undefined) ?? {}
  const tableName = (data.tableName as string | undefined) ?? ''
  const label = (data.label as string | undefined) ?? (node ? getNodeTitle(node.type) : '')
  const nodeResult = nodeId ? nodeResults[nodeId] : undefined
  const isCsvNode = node?.type === 'csv_source'
  const csvConfig = (isCsvNode ? config : null) as CsvSourceConfig | null
  const csvPreprocessing: CsvPreprocessingConfig = csvConfig?.preprocessing ?? {
    enabled: false,
    runtime: 'python',
    script: '',
  }
  const preprocessFingerprint = getCsvPreprocessFingerprint(csvConfig)
  const hasReviewedPreprocess = Boolean(
    nodeId
    && preprocessFingerprint
    && csvPreprocessArtifacts[nodeId] === preprocessFingerprint
  )
  const canPreviewCsv = Boolean(csvConfig?.file_path) && nodeResult?.status !== 'running'
  const canRunPreprocess = Boolean(csvConfig?.file_path)
    && csvPreprocessing.enabled
    && Boolean(csvPreprocessing.script.trim())
    && nodeResult?.status !== 'running'
  const canLoadCsv = Boolean(csvConfig?.file_path)
    && nodeResult?.status !== 'running'
    && (!csvPreprocessing.enabled || hasReviewedPreprocess)

  const availableUpstreamTables = useMemo(() => {
    if (!selectedNodeId || node?.type !== 'transform') return []
    const upstreamIds = edges.filter((edge) => edge.target === selectedNodeId).map((edge) => edge.source)
    return nodes
      .filter((candidate) => upstreamIds.includes(candidate.id))
      .map((candidate) => ((candidate.data as Record<string, unknown>).tableName as string | undefined) ?? '')
      .filter(Boolean)
  }, [edges, node?.type, nodes, selectedNodeId])

  useEffect(() => {
    if (!menuOpen) return

    const handlePointerDown = (event: MouseEvent) => {
      if (!menuRef.current?.contains(event.target as Node)) {
        setMenuOpen(false)
      }
    }

    document.addEventListener('mousedown', handlePointerDown)
    return () => document.removeEventListener('mousedown', handlePointerDown)
  }, [menuOpen])

  useEffect(() => {
    setMenuOpen(false)
  }, [nodeId])

  useEffect(() => {
    setIsDbEditMode(false)
    setIsTransformEditMode(false)
  }, [nodeId])

  const setPanelWidthForMode = useCallback((requestedWidthPx: number, expanded: boolean) => {
    const nextWidthPx = clampNodeConfigPanelWidth(requestedWidthPx, expanded)
    if (expanded) {
      setExpandedWidthPx(nextWidthPx)
      return
    }

    setCollapsedWidthPx(nextWidthPx)
  }, [])

  const stopResize = useCallback(() => {
    resizeStateRef.current = null
    document.body.style.removeProperty('user-select')
  }, [])

  const handleResize = useCallback((event: MouseEvent) => {
    const resizeState = resizeStateRef.current
    if (!resizeState) {
      return
    }

    const nextWidthPx = resizeState.startWidthPx + (resizeState.startX - event.clientX)
    setPanelWidthForMode(nextWidthPx, resizeState.expanded)
  }, [setPanelWidthForMode])

  useEffect(() => {
    const handleMouseUp = () => {
      stopResize()
    }

    window.addEventListener('mousemove', handleResize)
    window.addEventListener('mouseup', handleMouseUp)

    return () => {
      window.removeEventListener('mousemove', handleResize)
      window.removeEventListener('mouseup', handleMouseUp)
      stopResize()
    }
  }, [handleResize, stopResize])

  const updateCsvConfig = useCallback((patch: Partial<CsvSourceConfig>) => {
    if (!nodeId || !csvConfig) return
    updateNodeData(nodeId, {
      config: {
        ...csvConfig,
        ...patch,
        preprocessing: patch.preprocessing ?? csvPreprocessing,
      },
    })
  }, [csvConfig, csvPreprocessing, nodeId, updateNodeData])

  const handleCsvUpload = async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (!file || !nodeId) return

    const result = await uploadCsv(file)
    const existingConfig = (data.config as CsvSourceConfig | undefined) ?? {
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

  const handleDeleteNode = () => {
    if (!node) return
    setMenuOpen(false)
    const confirmed = window.confirm(`Delete "${label}"? This cannot be undone.`)
    if (!confirmed) return
    deleteNode(node.id)
  }

  const isQueryPanelExpanded = (node?.type === 'db_source' && isDbEditMode)
    || (node?.type === 'transform' && isTransformEditMode)
  const activeWidthPx = isQueryPanelExpanded ? expandedWidthPx : collapsedWidthPx

  const startResize = useCallback((event: ReactMouseEvent<HTMLDivElement>) => {
    resizeStateRef.current = {
      expanded: isQueryPanelExpanded,
      startX: event.clientX,
      startWidthPx: activeWidthPx,
    }
    document.body.style.userSelect = 'none'
    event.preventDefault()
  }, [activeWidthPx, isQueryPanelExpanded])

  if (!node) {
    return (
      <NodeConfigPanelShell
        widthPx={collapsedWidthPx}
        layoutState="collapsed"
        onResizeStart={startResize}
      >
        <div className="flex flex-1 items-center justify-center p-4 text-sm text-gray-400">
          Select a node to configure
        </div>
      </NodeConfigPanelShell>
    )
  }

  const dbType = ((config.db_type as string | undefined) ?? 'postgres') as DbType
  const dbConnection = config.connection as DatabaseConnectionConfig | undefined
  const dbQuery = (config.query as string | undefined) ?? ''
  const transformQuery = (config.sql as string | undefined) ?? ''
  const isDbNodeBusy = nodeResult?.status === 'connecting' || nodeResult?.status === 'running'
  const canExecuteDb = Boolean(dbQuery.trim()) && !isDbNodeBusy
  const canExecuteTransform = Boolean(transformQuery.trim()) && nodeResult?.status !== 'running'
  const nodeRunningElapsed = nodeResult ? getResultElapsedLabel(nodeResult, executionClockNow) : null
  const nodeStatusLabel = nodeResult?.status === 'running'
    ? `Running${nodeRunningElapsed ? ` · ${nodeRunningElapsed}` : ''}`
    : nodeResult?.status === 'connecting'
      ? 'Connecting'
    : (nodeResult ? `Status: ${nodeResult.status}` : null)

  const renderActionsMenu = () => (
    <div ref={menuRef} className="relative shrink-0">
      <button
        type="button"
        aria-label={`More options for ${label}`}
        aria-expanded={menuOpen}
        onClick={() => setMenuOpen((current) => !current)}
        className="rounded-lg px-2 py-1 text-lg leading-none text-stone-500 transition hover:bg-stone-100 hover:text-stone-700"
      >
        ⋯
      </button>

      {menuOpen && (
        <div
          data-testid="node-config-actions-menu"
          className="absolute right-0 top-10 z-10 min-w-32 rounded-xl border border-stone-200 bg-white p-1.5 text-sm text-stone-700 shadow-lg"
        >
          <button
            type="button"
            onClick={() => {
              openEditNodeEditor(node.id)
              setMenuOpen(false)
            }}
            className="block w-full rounded-lg px-3 py-2 text-left transition hover:bg-stone-100"
          >
            Edit
          </button>
          <button
            type="button"
            onClick={handleDeleteNode}
            className="block w-full rounded-lg px-3 py-2 text-left text-red-600 transition hover:bg-red-50"
          >
            Delete
          </button>
        </div>
      )}
    </div>
  )

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
    isBusy = false,
    busyActionLabel,
    busyButtonClassName,
    description,
    metadata,
    extraEditorContent,
    onExecute,
    onBusyAction,
  }: {
    expanded: boolean
    setExpanded: Dispatch<SetStateAction<boolean>>
    title: string
    defaultLabel: string
    queryValue: string
    onQueryChange: (query: string) => void
    canExecute: boolean
    actionLabel: string
    enabledButtonClassName: string
    isBusy?: boolean
    busyActionLabel?: string
    busyButtonClassName?: string
    description: string
    metadata: ReactNode
    extraEditorContent?: ReactNode
    onExecute: () => void
    onBusyAction?: () => void
  }) => (
    <NodeConfigPanelShell
      widthPx={expanded ? expandedWidthPx : collapsedWidthPx}
      layoutState={expanded ? 'expanded' : 'collapsed'}
      onResizeStart={startResize}
    >
      <div className="border-b border-gray-200 px-4 py-4">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0">
            <div className="text-[11px] font-semibold uppercase tracking-[0.24em] text-gray-400">{title}</div>
            <h3 className="mt-2 truncate text-base font-semibold text-gray-900">
              {label || defaultLabel}
            </h3>
          </div>
          {renderActionsMenu()}
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
              {nodeStatusLabel}
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
          onClick={isBusy ? onBusyAction : onExecute}
          disabled={isBusy ? !onBusyAction : !canExecute}
          className={`w-full rounded-lg px-4 py-2 text-sm font-medium transition ${
            isBusy
              ? (busyButtonClassName ?? 'bg-red-500 text-white hover:bg-red-600')
              : canExecute
                ? enabledButtonClassName
                : 'bg-gray-100 text-gray-400'
          }`}
        >
          {isBusy
            ? (busyActionLabel ?? 'Abort')
            : nodeResult?.status === 'connecting'
              ? 'Connecting...'
              : nodeResult?.status === 'running'
                ? 'Running...'
                : actionLabel}
        </button>
      </div>
    </NodeConfigPanelShell>
  )

  if (node.type === 'db_source') {
    return renderQueryPanel({
      expanded: isDbEditMode,
      setExpanded: setIsDbEditMode,
      title: 'Database Source',
      defaultLabel: 'Database Source',
      queryValue: dbQuery,
      onQueryChange: (query) => updateNodeData(node.id, { config: { ...config, query } }),
      canExecute: canExecuteDb,
      actionLabel: 'Execute',
      enabledButtonClassName: 'bg-emerald-500 text-white hover:bg-emerald-600',
      isBusy: isDbNodeBusy,
      busyActionLabel: 'Abort',
      busyButtonClassName: 'bg-red-500 text-white hover:bg-red-600',
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
      onBusyAction: () => { void abortDatabaseNodeExecution(node.id) },
    })
  }

  if (node.type === 'transform') {
    return renderQueryPanel({
      expanded: isTransformEditMode,
      setExpanded: setIsTransformEditMode,
      title: 'Transform',
      defaultLabel: 'Transform',
      queryValue: transformQuery,
      onQueryChange: (query) => updateNodeData(node.id, { config: { ...config, sql: query } }),
      canExecute: canExecuteTransform,
      actionLabel: 'Run and Preview',
      enabledButtonClassName: 'bg-purple-500 text-white hover:bg-purple-600',
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
    <NodeConfigPanelShell
      widthPx={collapsedWidthPx}
      layoutState="collapsed"
      onResizeStart={startResize}
    >
      <div className="border-b border-gray-200 p-4">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0">
            <div className="text-[11px] font-semibold uppercase tracking-[0.24em] text-gray-400">
              {getNodeTitle(node.type)}
            </div>
            <h3 className="mt-2 truncate text-base font-semibold text-gray-900">{label}</h3>
          </div>
          {renderActionsMenu()}
        </div>

        <div className="mt-4 space-y-3">
          <div>
            <div className="text-xs font-semibold uppercase tracking-wide text-gray-500">Table</div>
            <div className="mt-1 rounded-lg border border-gray-200 bg-gray-50 px-3 py-2 font-mono text-sm text-gray-700">
              {tableName}
            </div>
          </div>

          {node.type === 'db_source' && dbConnection && (
            <div>
              <div className="text-xs font-semibold uppercase tracking-wide text-gray-500">Connection</div>
              <div className="mt-1 rounded-lg border border-gray-200 bg-gray-50 px-3 py-2 text-sm text-gray-600">
                {getConnectionSummary(dbType, dbConnection)}
              </div>
            </div>
          )}

          {nodeResult && (
            <div className="text-xs text-gray-500">
              {nodeStatusLabel}
            </div>
          )}
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto">
        <div className="space-y-6 p-4">
        {node.type === 'db_source' && (
          <>
            <QueryPreview title="SQL Query" value={dbQuery} emptyLabel="No query defined" />
            <button
              type="button"
              onClick={() => { void executeSingleNode(node.id, { loadPreviewOnSuccess: true }) }}
              disabled={!canExecuteDb}
              className={`w-full rounded-lg px-4 py-2 text-sm font-medium transition ${
                canExecuteDb
                  ? 'bg-emerald-500 text-white hover:bg-emerald-600'
                  : 'bg-gray-100 text-gray-400'
              }`}
            >
              {nodeResult?.status === 'connecting' ? 'Connecting...' : nodeResult?.status === 'running' ? 'Running...' : 'Execute'}
            </button>
          </>
        )}

        {node.type === 'transform' && (
          <>
            {availableUpstreamTables.length > 0 && (
              <div>
                <div className="mb-1 text-xs font-semibold uppercase tracking-wide text-gray-500">Available Tables</div>
                <div className="flex flex-wrap gap-1">
                  {availableUpstreamTables.map((upstreamTable) => (
                    <span key={upstreamTable} className="rounded bg-purple-100 px-2 py-0.5 text-xs font-mono text-purple-700">
                      {upstreamTable}
                    </span>
                  ))}
                </div>
              </div>
            )}
            <QueryPreview title="SQL Query" value={transformQuery} emptyLabel="No SQL defined" />
            <button
              type="button"
              onClick={() => { void runTransformPreview(node.id) }}
              disabled={!canExecuteTransform}
              className={`w-full rounded-lg px-4 py-2 text-sm font-medium transition ${
                canExecuteTransform
                  ? 'bg-purple-500 text-white hover:bg-purple-600'
                  : 'bg-gray-100 text-gray-400'
              }`}
            >
              {nodeResult?.status === 'running' ? 'Running...' : 'Run and Preview'}
            </button>
          </>
        )}

        {node.type === 'csv_source' && csvConfig && (
          <div className="space-y-6">
            <div>
              <label className="mb-2 block text-xs text-gray-500">CSV File</label>
              <input ref={fileInputRef} type="file" accept=".csv" onChange={handleCsvUpload} className="hidden" />
              <button
                type="button"
                onClick={() => fileInputRef.current?.click()}
                className="w-full rounded-lg border-2 border-dashed border-gray-300 p-4 text-sm text-gray-500 transition hover:border-blue-400 hover:text-blue-500"
              >
                {csvConfig.original_filename || 'Click to upload CSV'}
              </button>
            </div>

            <div>
              <div className="mb-2 flex items-center justify-between">
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
                      <label htmlFor="csv-runtime" className="mb-1 block text-xs text-gray-500">Runtime</label>
                      <select
                        id="csv-runtime"
                        value={csvPreprocessing.runtime}
                        onChange={(event) => updateCsvConfig({
                          preprocessing: {
                            ...csvPreprocessing,
                            runtime: event.target.value as CsvPreprocessingConfig['runtime'],
                          },
                        })}
                        className="w-full rounded border border-gray-300 px-2 py-1 text-sm"
                      >
                        <option value="python">Python</option>
                        <option value="bash">Bash</option>
                      </select>
                    </div>
                    <div>
                      <label htmlFor="csv-script" className="mb-1 block text-xs text-gray-500">Script</label>
                      <textarea
                        id="csv-script"
                        value={csvPreprocessing.script}
                        onChange={(event) => updateCsvConfig({
                          preprocessing: {
                            ...csvPreprocessing,
                            script: event.target.value,
                          },
                        })}
                        rows={8}
                        spellCheck={false}
                        className="w-full rounded border border-gray-300 bg-white px-2 py-1.5 text-sm font-mono"
                        placeholder={csvPreprocessing.runtime === 'bash'
                          ? 'tail -n +3 \"$1\"'
                          : 'import sys\nfrom pathlib import Path\nlines = Path(sys.argv[1]).read_text().splitlines()[2:]\nsys.stdout.write(\"\\n\".join(lines))'}
                      />
                    </div>
                  </div>
                </>
              )}
            </div>

            <div>
              <div className="mb-2 flex items-center justify-between">
                <label className="block text-xs text-gray-500">Run Node</label>
                {nodeResult && (
                  <span className="text-xs text-gray-400">
                    {nodeStatusLabel}
                  </span>
                )}
              </div>
              <p className="mb-3 text-xs text-gray-500">
                Preview the uploaded CSV before materializing it, then load it into DuckDB once the preprocessing is ready.
              </p>
              <div className="grid grid-cols-3 gap-3">
                <button
                  type="button"
                  onClick={() => csvConfig.file_path && void loadCsvPreview(node.id, csvConfig.file_path)}
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
                  onClick={() => csvConfig.file_path && void loadPreprocessedCsvPreview(node.id, csvConfig.file_path, csvPreprocessing)}
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
          <div className="space-y-3">
            <div>
              <div className="text-xs font-semibold uppercase tracking-wide text-gray-500">Format</div>
              <div className="mt-1 rounded-lg border border-gray-200 bg-gray-50 px-3 py-2 text-sm text-gray-700">
                {(config.format as string | undefined) ?? 'csv'}
              </div>
            </div>
            <div className="text-sm text-gray-500">
              Connect this node to a source to export its data as CSV.
            </div>
          </div>
        )}
        </div>
      </div>
    </NodeConfigPanelShell>
  )
}
