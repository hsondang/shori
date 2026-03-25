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

export default function DataPreviewPanel() {
  const previewData = usePipelineStore((s) => s.previewData)
  const previewNodeId = usePipelineStore((s) => s.previewNodeId)
  const previewLoading = usePipelineStore((s) => s.previewLoading)
  const previewError = usePipelineStore((s) => s.previewError)
  const nodes = usePipelineStore((s) => s.nodes)
  const loadTablePreview = usePipelineStore((s) => s.loadTablePreview)

  const previewNode = nodes.find((n) => n.id === previewNodeId)
  const previewNodeData = (previewNode?.data as Record<string, unknown> | undefined) ?? {}
  const tableName = typeof previewNodeData.tableName === 'string' ? previewNodeData.tableName : null
  const config = (previewNodeData.config as Record<string, unknown> | undefined) ?? {}
  const filename = typeof config.original_filename === 'string' ? config.original_filename : null

  if (previewLoading) {
    return (
      <div className="h-48 border-t border-gray-200 bg-white flex items-center justify-center text-gray-400 text-sm">
        Loading preview...
      </div>
    )
  }

  if (previewError) {
    return (
      <div className="h-48 border-t border-red-200 bg-red-50 flex items-center justify-center px-4 text-sm text-red-700">
        {previewError}
      </div>
    )
  }

  if (!previewData) {
    return (
      <div className="h-48 border-t border-gray-200 bg-white flex items-center justify-center text-gray-400 text-sm">
        Click "Preview data" on a node to see its contents
      </div>
    )
  }

  if (previewData.kind === 'csv_text') {
    const isPreprocessed = previewData.csv_stage === 'preprocessed'
    return (
      <div className="h-64 border-t border-gray-200 bg-white flex flex-col">
        <div className="flex items-center justify-between px-4 py-2 border-b border-gray-100 bg-gray-50">
          <div className="text-sm text-gray-700 font-medium">
            <span className="font-mono text-blue-600">{filename || tableName || 'CSV Source'}</span>
            <span className="ml-2 text-gray-400">
              {isPreprocessed ? 'Preprocessed CSV preview' : 'Raw CSV preview'} · first {previewData.limit} rows
            </span>
          </div>
          <div className="text-xs text-gray-500">
            {isPreprocessed && previewData.artifact_ready
              ? 'Reviewed output ready for load'
              : previewData.truncated
                ? 'Truncated to preview limit'
                : 'Entire file fits in preview'}
          </div>
        </div>
        <div className="flex-1 overflow-auto bg-stone-50 font-mono text-xs">
          {previewData.rows.length === 0 ? (
            <div className="px-4 py-3 text-gray-400">This CSV file is empty.</div>
          ) : (
            <div className="min-w-full divide-y divide-stone-200">
              {previewData.rows.map((row, rowIndex) => (
                <div key={rowIndex} className="flex gap-3 px-4 py-1.5 hover:bg-stone-100">
                  <span className="w-10 shrink-0 text-right text-stone-400">{rowIndex + 1}</span>
                  <div className="min-w-0 flex-1 overflow-x-auto whitespace-nowrap">
                    {row.map((cell, cellIndex) => (
                      <span key={`${rowIndex}-${cellIndex}`}>
                        <span className={csvCellColors[cellIndex % csvCellColors.length]}>
                          {cell.length === 0 ? '""' : cell}
                        </span>
                        {cellIndex < row.length - 1 && <span className="text-stone-300">, </span>}
                      </span>
                    ))}
                    {row.length === 0 && <span className="text-stone-300 italic">(empty row)</span>}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    )
  }

  if (!tableName) return null

  const totalPages = Math.ceil(previewData.total_rows / previewData.limit)
  const currentPage = Math.floor(previewData.offset / previewData.limit) + 1

  return (
    <div className="h-64 border-t border-gray-200 bg-white flex flex-col">
      <div className="flex items-center justify-between px-4 py-2 border-b border-gray-100 bg-gray-50">
        <div className="text-sm text-gray-700 font-medium">
          <span className="font-mono text-purple-600">{tableName}</span>
          <span className="ml-2 text-gray-400">
            {previewData.total_rows.toLocaleString()} rows × {previewData.columns.length} cols
          </span>
        </div>
        <div className="flex items-center gap-2 text-xs">
          <button
            disabled={currentPage <= 1}
            onClick={() => loadTablePreview(previewNodeId!, tableName, previewData.offset - previewData.limit)}
            className="px-2 py-1 border border-gray-300 rounded disabled:opacity-30 hover:bg-gray-100"
          >
            Prev
          </button>
          <span className="text-gray-500">Page {currentPage} of {totalPages}</span>
          <button
            disabled={currentPage >= totalPages}
            onClick={() => loadTablePreview(previewNodeId!, tableName, previewData.offset + previewData.limit)}
            className="px-2 py-1 border border-gray-300 rounded disabled:opacity-30 hover:bg-gray-100"
          >
            Next
          </button>
        </div>
      </div>
      <div className="flex-1 overflow-auto">
        <table className="w-full text-xs">
          <thead className="bg-gray-50 sticky top-0">
            <tr>
              {previewData.columns.map((col, i) => (
                <th key={i} className="text-left px-3 py-1.5 font-medium text-gray-600 border-b border-gray-200 whitespace-nowrap">
                  {col}
                  <span className="ml-1 text-gray-400 font-normal">{previewData.column_types[i]}</span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {previewData.rows.map((row, ri) => (
              <tr key={ri} className="hover:bg-blue-50">
                {row.map((cell, ci) => (
                  <td key={ci} className="px-3 py-1 border-b border-gray-100 whitespace-nowrap max-w-[200px] truncate">
                    {cell === null ? <span className="text-gray-300 italic">NULL</span> : String(cell)}
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
