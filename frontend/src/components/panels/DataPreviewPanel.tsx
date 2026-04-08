import type { MaterializedPreviewTab } from '../../types/pipeline'
import { usePipelineStore } from '../../store/pipelineStore'

const csvCellColors = [
  'text-sky-700',
  'text-emerald-700',
  'text-amber-700',
  'text-rose-700',
  'text-violet-700',
  'text-cyan-700',
  'text-lime-700',
  'text-orange-700',
]

const defaultLabels = {
  csv_source: 'CSV Source',
  db_source: 'Database Source',
  transform: 'Transform',
  export: 'Export',
} as const

type DefaultLabelKey = keyof typeof defaultLabels

export default function DataPreviewPanel() {
  const previewTabsByNodeId = usePipelineStore((s) => s.previewTabsByNodeId)
  const previewTabOrder = usePipelineStore((s) => s.previewTabOrder)
  const activePreviewTarget = usePipelineStore((s) => s.activePreviewTarget)
  const transientPreview = usePipelineStore((s) => s.transientPreview)
  const nodes = usePipelineStore((s) => s.nodes)
  const loadTablePreview = usePipelineStore((s) => s.loadTablePreview)
  const selectPreviewTab = usePipelineStore((s) => s.selectPreviewTab)

  const previewTabs = previewTabOrder
    .map((nodeId) => previewTabsByNodeId[nodeId])
    .filter((tab): tab is MaterializedPreviewTab => Boolean(tab))
  const fallbackNodeId = previewTabs[previewTabs.length - 1]?.nodeId ?? null
  const resolvedActivePreviewTarget = activePreviewTarget ?? (fallbackNodeId ? { kind: 'tab' as const, nodeId: fallbackNodeId } : null)
  const activeTab = resolvedActivePreviewTarget?.kind === 'tab'
    ? previewTabsByNodeId[resolvedActivePreviewTarget.nodeId] ?? null
    : null
  const activeTransient = resolvedActivePreviewTarget?.kind === 'transient'
    ? transientPreview
    : null

  const getNodeById = (nodeId: string) => nodes.find((node) => node.id === nodeId)

  const getTabTitle = (tab: MaterializedPreviewTab) => {
    const node = getNodeById(tab.nodeId)
    const data = (node?.data as Record<string, unknown> | undefined) ?? {}
    const label = typeof data.label === 'string' ? data.label : ''
    const labelMode = data.labelMode === 'custom' || data.labelMode === 'auto'
      ? data.labelMode
      : node?.type === 'db_source'
        ? 'auto'
        : node?.type
          ? (label === defaultLabels[node.type as DefaultLabelKey] ? 'auto' : 'custom')
          : 'auto'
    const tableName = typeof data.tableName === 'string' ? data.tableName : tab.tableNameAtLoad

    if (labelMode === 'custom' && label) {
      return label
    }

    return tab.isStale ? tab.tableNameAtLoad : tableName
  }

  const renderEmptyState = () => (
    <div className="flex h-full min-h-0 items-center justify-center bg-white text-sm text-gray-400">
      Click "Preview data" on a node to see its contents
    </div>
  )

  const renderCsvPreview = () => {
    if (!activeTransient) return renderEmptyState()

    if (activeTransient.loading) {
      return (
        <div className="flex h-full min-h-0 items-center justify-center bg-white text-sm text-gray-400">
          Loading preview...
        </div>
      )
    }

    if (activeTransient.error) {
      return (
        <div className="flex h-full min-h-0 items-center justify-center bg-red-50 px-4 text-sm text-red-700">
          {activeTransient.error}
        </div>
      )
    }

    if (!activeTransient.data) {
      return renderEmptyState()
    }

    const previewNode = getNodeById(activeTransient.nodeId ?? '')
    const previewNodeData = (previewNode?.data as Record<string, unknown> | undefined) ?? {}
    const tableName = typeof previewNodeData.tableName === 'string' ? previewNodeData.tableName : null
    const config = (previewNodeData.config as Record<string, unknown> | undefined) ?? {}
    const filename = typeof config.original_filename === 'string' ? config.original_filename : null
    const isPreprocessed = activeTransient.data.csv_stage === 'preprocessed'

    return (
      <div className="flex h-full min-h-0 flex-col bg-white">
        <div className="flex items-center justify-between border-b border-gray-100 bg-gray-50 px-4 py-2">
          <div className="text-sm font-medium text-gray-700">
            <span className="font-mono text-blue-600">{filename || tableName || 'CSV Source'}</span>
            <span className="ml-2 text-gray-400">
              {isPreprocessed ? 'Preprocessed CSV preview' : 'Raw CSV preview'} · first {activeTransient.data.limit} rows
            </span>
          </div>
          <div className="text-xs text-gray-500">
            {isPreprocessed && activeTransient.data.artifact_ready
              ? 'Reviewed output ready for load'
              : activeTransient.data.truncated
                ? 'Truncated to preview limit'
                : 'Entire file fits in preview'}
          </div>
        </div>
        <div className="flex-1 overflow-auto bg-stone-50 font-mono text-xs" data-testid="csv-preview-scroll-region">
          {activeTransient.data.rows.length === 0 ? (
            <div className="px-4 py-3 text-gray-400">This CSV file is empty.</div>
          ) : (
            <div className="min-w-max divide-y divide-stone-200">
              {activeTransient.data.rows.map((row, rowIndex) => (
                <div key={rowIndex} data-testid="csv-preview-row" className="flex gap-3 whitespace-nowrap px-4 py-1.5 hover:bg-stone-100">
                  <span className="w-10 shrink-0 text-right text-stone-400">{rowIndex + 1}</span>
                  <div className="flex-1">
                    {row.map((cell, cellIndex) => (
                      <span key={`${rowIndex}-${cellIndex}`}>
                        <span className={csvCellColors[cellIndex % csvCellColors.length]}>
                          {cell.length === 0 ? '""' : cell}
                        </span>
                        {cellIndex < row.length - 1 && <span className="text-stone-300">, </span>}
                      </span>
                    ))}
                    {row.length === 0 && <span className="italic text-stone-300">(empty row)</span>}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    )
  }

  const renderTablePreview = () => {
    if (!activeTab) return renderEmptyState()

    const node = getNodeById(activeTab.nodeId)
    const nodeData = (node?.data as Record<string, unknown> | undefined) ?? {}
    const currentTableName = typeof nodeData.tableName === 'string' ? nodeData.tableName : activeTab.tableNameAtLoad

    if (activeTab.loading) {
      return (
        <div className="flex h-full min-h-0 items-center justify-center bg-white text-sm text-gray-400">
          Loading preview...
        </div>
      )
    }

    if (activeTab.error) {
      return (
        <div className="flex h-full min-h-0 items-center justify-center bg-red-50 px-4 text-sm text-red-700">
          {activeTab.error}
        </div>
      )
    }

    if (!activeTab.data) {
      return renderEmptyState()
    }

    const activeData = activeTab.data
    const totalPages = Math.max(1, Math.ceil(activeData.total_rows / activeData.limit))
    const currentPage = Math.floor(activeData.offset / activeData.limit) + 1
    const paginationDisabled = activeTab.isStale

    return (
      <div className="flex h-full min-h-0 flex-col bg-white">
        <div className="flex items-center justify-between border-b border-gray-100 bg-gray-50 px-4 py-2">
          <div className="text-sm font-medium text-gray-700">
            <span className="font-mono text-purple-600">{getTabTitle(activeTab)}</span>
            <span className="ml-2 text-gray-400">
              {activeData.total_rows.toLocaleString()} rows × {activeData.columns.length} cols
            </span>
            {activeTab.isStale && (
              <span className="ml-2 rounded-full bg-amber-100 px-2 py-0.5 text-[11px] font-semibold uppercase tracking-wide text-amber-700">
                Stale
              </span>
            )}
          </div>
          <div className="flex items-center gap-2 text-xs">
            <button
              disabled={paginationDisabled || currentPage <= 1}
              onClick={() => loadTablePreview(activeTab.nodeId, currentTableName, activeData.offset - activeData.limit)}
              className="rounded border border-gray-300 px-2 py-1 hover:bg-gray-100 disabled:opacity-30"
            >
              Prev
            </button>
            <span className="text-gray-500">Page {currentPage} of {totalPages}</span>
            <button
              disabled={paginationDisabled || currentPage >= totalPages}
              onClick={() => loadTablePreview(activeTab.nodeId, currentTableName, activeData.offset + activeData.limit)}
              className="rounded border border-gray-300 px-2 py-1 hover:bg-gray-100 disabled:opacity-30"
            >
              Next
            </button>
          </div>
        </div>
        <div className="flex-1 overflow-auto">
          <table className="w-full text-xs">
            <thead className="sticky top-0 bg-gray-50">
              <tr>
                {activeData.columns.map((col, i) => (
                  <th key={i} className="whitespace-nowrap border-b border-gray-200 px-3 py-1.5 text-left font-medium text-gray-600">
                    {col}
                    <span className="ml-1 font-normal text-gray-400">{activeData.column_types[i]}</span>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {activeData.rows.map((row, rowIndex) => (
                <tr key={rowIndex} className="hover:bg-blue-50">
                  {row.map((cell, cellIndex) => (
                    <td key={cellIndex} className="max-w-[200px] truncate border-b border-gray-100 px-3 py-1 whitespace-nowrap">
                      {cell === null ? <span className="italic text-gray-300">NULL</span> : String(cell)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    )
  }

  return (
    <div className="flex h-full min-h-0 flex-col bg-white">
      {previewTabs.length > 0 && (
        <div className="border-b border-gray-200 bg-white px-2 py-2">
          <div className="flex gap-2 overflow-x-auto pb-1" role="tablist" aria-label="Preview tabs">
            {previewTabs.map((tab) => {
              const isActive = resolvedActivePreviewTarget?.kind === 'tab' && resolvedActivePreviewTarget.nodeId === tab.nodeId
              return (
                <button
                  key={tab.nodeId}
                  type="button"
                  role="tab"
                  aria-selected={isActive}
                  onClick={() => selectPreviewTab(tab.nodeId)}
                  className={`shrink-0 rounded-full border px-3 py-1.5 text-xs font-medium transition ${
                    isActive
                      ? 'border-stone-900 bg-stone-900 text-white'
                      : 'border-stone-200 bg-stone-50 text-stone-600 hover:bg-stone-100'
                  }`}
                >
                  <span>{getTabTitle(tab)}</span>
                  {tab.isStale && (
                    <span className={`ml-2 rounded-full px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${
                      isActive ? 'bg-white/20 text-white' : 'bg-amber-100 text-amber-700'
                    }`}>
                      Stale
                    </span>
                  )}
                </button>
              )
            })}
          </div>
        </div>
      )}
      <div className="min-h-0 flex-1">
        {resolvedActivePreviewTarget?.kind === 'transient' ? renderCsvPreview() : renderTablePreview()}
      </div>
    </div>
  )
}
