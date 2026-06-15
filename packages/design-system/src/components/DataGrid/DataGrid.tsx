import { useRef, type UIEvent, type ReactElement } from 'react'

export interface DataGridColumn {
  name: string
  type?: string
}

export interface DataGridProps {
  columns: DataGridColumn[]
  rows: unknown[][]
  loading?: boolean
  loadingMore?: boolean
  emptyMessage?: string
  nullLabel?: string
  onScrollNearBottom?: () => void
  scrollThreshold?: number
}

function formatCell(cell: unknown, nullLabel: string): string | ReactElement {
  if (cell === null || cell === undefined) {
    return <span className="ds-grid__null">{nullLabel}</span>
  }
  return String(cell)
}

export function DataGrid({
  columns,
  rows,
  loading = false,
  loadingMore = false,
  emptyMessage = 'No data',
  nullLabel = 'NULL',
  onScrollNearBottom,
  scrollThreshold = 200,
}: DataGridProps) {
  const guardRef = useRef(false)

  const handleScroll = (e: UIEvent<HTMLDivElement>) => {
    if (!onScrollNearBottom || guardRef.current) return
    const el = e.currentTarget
    if (el.scrollHeight - el.scrollTop - el.clientHeight < scrollThreshold) {
      guardRef.current = true
      onScrollNearBottom()
      requestAnimationFrame(() => { guardRef.current = false })
    }
  }

  if (loading) {
    return (
      <div className="ds-grid ds-grid--loading">
        <span className="ds-grid__empty-msg">Loading…</span>
      </div>
    )
  }

  if (columns.length === 0) {
    return (
      <div className="ds-grid ds-grid--empty">
        <span className="ds-grid__empty-msg">{emptyMessage}</span>
      </div>
    )
  }

  return (
    <div className="ds-grid" onScroll={handleScroll}>
      <table className="ds-grid__table">
        <thead className="ds-grid__head">
          <tr>
            {columns.map((col, i) => (
              <th key={i} className="ds-grid__th">
                {col.name}
                {col.type && <span className="ds-grid__col-type">{col.type}</span>}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.length === 0 ? (
            <tr>
              <td colSpan={columns.length} className="ds-grid__empty-cell">
                {emptyMessage}
              </td>
            </tr>
          ) : (
            rows.map((row, ri) => (
              <tr key={ri} className="ds-grid__row">
                {row.map((cell, ci) => (
                  <td key={ci} className="ds-grid__td">
                    {formatCell(cell, nullLabel)}
                  </td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
      {loadingMore && (
        <div className="ds-grid__load-more">Loading more…</div>
      )}
    </div>
  )
}
