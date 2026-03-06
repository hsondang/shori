import { usePipelineStore } from '../../store/pipelineStore'

export default function DataPreviewPanel() {
  const previewData = usePipelineStore((s) => s.previewData)
  const previewNodeId = usePipelineStore((s) => s.previewNodeId)
  const previewLoading = usePipelineStore((s) => s.previewLoading)
  const nodes = usePipelineStore((s) => s.nodes)
  const loadPreview = usePipelineStore((s) => s.loadPreview)

  const previewNode = nodes.find((n) => n.id === previewNodeId)
  const tableName = previewNode ? (previewNode.data as Record<string, unknown>).tableName as string : null

  if (!previewData && !previewLoading) {
    return (
      <div className="h-48 border-t border-gray-200 bg-white flex items-center justify-center text-gray-400 text-sm">
        Click "Preview data" on a node to see its contents
      </div>
    )
  }

  if (previewLoading) {
    return (
      <div className="h-48 border-t border-gray-200 bg-white flex items-center justify-center text-gray-400 text-sm">
        Loading preview...
      </div>
    )
  }

  if (!previewData || !tableName) return null

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
            onClick={() => loadPreview(previewNodeId!, tableName, previewData.offset - previewData.limit)}
            className="px-2 py-1 border border-gray-300 rounded disabled:opacity-30 hover:bg-gray-100"
          >
            Prev
          </button>
          <span className="text-gray-500">Page {currentPage} of {totalPages}</span>
          <button
            disabled={currentPage >= totalPages}
            onClick={() => loadPreview(previewNodeId!, tableName, previewData.offset + previewData.limit)}
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
                {(row as unknown[]).map((cell, ci) => (
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
