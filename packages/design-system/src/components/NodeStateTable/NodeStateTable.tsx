import { useMemo, useState } from 'react'
import { StatusBadge } from '../StatusBadge/StatusBadge'
import type { NodeResultLike } from '../StatusBadge/status'
import { DataStateDots, type LocationDot, type PythonDot } from '../DataStateDots/DataStateDots'

export type NodeStateKind = 'csv' | 'excel' | 'db' | 'transform' | 'export'

export interface NodeStateRow {
  id: string
  kind: NodeStateKind
  name: string
  result?: NodeResultLike | null
  /** Omit entirely for node types with no live preview (Excel; plain CSV with no preprocessing). */
  python?: PythonDot
  memory: LocationDot
  disk: LocationDot
  schema?: string | null
  table?: string | null
  rowCount?: number | null
  createdAtLabel?: string | null
  updatedAtLabel?: string | null
  lastRunLabel?: string | null
}

export interface NodeStateTableProps {
  rows: NodeStateRow[]
  emptyMessage?: string
  onSelectRow?: (nodeId: string) => void
}

type SortKey = 'name' | 'kind' | 'rowCount'
type SortDir = 'asc' | 'desc'

const KIND_ICON: Record<NodeStateKind, string> = {
  csv: '📄',
  excel: '▦',
  db: '🗄️',
  transform: '⚙️',
  export: '📤',
}

const KIND_LABEL: Record<NodeStateKind, string> = {
  csv: 'CSV',
  excel: 'Excel',
  db: 'Postgres',
  transform: 'Transform',
  export: 'Export',
}

function isPresent(dot: LocationDot | undefined): boolean {
  return dot === 'fresh' || dot === 'stale' || dot === 'loading'
}

function isStale(dot: LocationDot | undefined): boolean {
  return dot === 'stale'
}

function compareRows(a: NodeStateRow, b: NodeStateRow, key: SortKey): number {
  if (key === 'rowCount') return (a.rowCount ?? -1) - (b.rowCount ?? -1)
  if (key === 'kind') return KIND_LABEL[a.kind].localeCompare(KIND_LABEL[b.kind])
  return a.name.localeCompare(b.name)
}

/**
 * The node-state overview (docs/node-state-model.md): one row per node, its run
 * status, and its three-location data state side by side, plus a summary legend
 * derived from the rows. Read-only — this is a status view, not an editor.
 */
export function NodeStateTable({ rows, emptyMessage = 'No nodes yet', onSelectRow }: NodeStateTableProps) {
  const [sort, setSort] = useState<{ key: SortKey; dir: SortDir }>({ key: 'name', dir: 'asc' })

  const sorted = useMemo(() => {
    const copy = [...rows]
    copy.sort((a, b) => compareRows(a, b, sort.key) * (sort.dir === 'asc' ? 1 : -1))
    return copy
  }, [rows, sort])

  const summary = useMemo(() => {
    const total = rows.length
    let inDuckdb = 0
    let inMemory = 0
    let livePreview = 0
    let noData = 0
    let succeeded = 0
    let stale = 0
    for (const row of rows) {
      const hasMemory = isPresent(row.memory)
      const hasDisk = isPresent(row.disk)
      if (hasMemory || hasDisk) inDuckdb++
      if (hasMemory) inMemory++
      if (row.python === 'live') livePreview++
      if (!hasMemory && !hasDisk && row.python !== 'live') noData++
      if (row.result?.status === 'success') succeeded++
      if (isStale(row.memory) || isStale(row.disk)) stale++
    }
    return { total, inDuckdb, inMemory, livePreview, noData, succeeded, stale }
  }, [rows])

  const toggleSort = (key: SortKey) => {
    setSort((current) =>
      current.key === key
        ? { key, dir: current.dir === 'asc' ? 'desc' : 'asc' }
        : { key, dir: 'asc' }
    )
  }

  const sortIndicator = (key: SortKey) => (sort.key === key ? (sort.dir === 'asc' ? ' ▲' : ' ▼') : '')

  return (
    <div className="ds-node-state">
      <div className="ds-node-state__summary">
        <span className="ds-node-state__summary-item ds-node-state__summary-item--strong">
          {summary.total} node{summary.total === 1 ? '' : 's'}
        </span>
        <span className="ds-node-state__summary-dot" />
        <span className="ds-node-state__summary-item">
          <span className="ds-datadots__dot ds-datadots__dot--memory is-fresh" /> {summary.inMemory} in memory
        </span>
        <span className="ds-node-state__summary-item">
          <span className="ds-datadots__dot ds-datadots__dot--disk is-fresh" /> {summary.inDuckdb} loaded in DuckDB
        </span>
        <span className="ds-node-state__summary-item">
          <span className="ds-datadots__dot ds-datadots__dot--python is-live" /> {summary.livePreview} live preview
        </span>
        <span className="ds-node-state__summary-item">
          <span className="ds-datadots__dot is-empty" /> {summary.noData} no data
        </span>
        <span className="ds-node-state__summary-dot" />
        <span className="ds-node-state__summary-item">{summary.succeeded} succeeded</span>
        {summary.stale > 0 && (
          <span className="ds-node-state__summary-item ds-node-state__summary-item--stale">{summary.stale} stale</span>
        )}
      </div>

      <div className="ds-node-state__scroll">
        <table className="ds-node-state__table">
          <thead>
            <tr>
              <th onClick={() => toggleSort('kind')}>Type{sortIndicator('kind')}</th>
              <th onClick={() => toggleSort('name')}>Node{sortIndicator('name')}</th>
              <th>Run status</th>
              <th>Data state</th>
              <th>Schema</th>
              <th>Table</th>
              <th onClick={() => toggleSort('rowCount')} className="ds-node-state__num">Rows{sortIndicator('rowCount')}</th>
              <th>Created</th>
              <th>Updated</th>
              <th>Last run</th>
            </tr>
          </thead>
          <tbody>
            {sorted.length === 0 ? (
              <tr>
                <td colSpan={10} className="ds-node-state__empty">{emptyMessage}</td>
              </tr>
            ) : (
              sorted.map((row) => (
                <tr
                  key={row.id}
                  className={onSelectRow ? 'is-clickable' : undefined}
                  onClick={onSelectRow ? () => onSelectRow(row.id) : undefined}
                >
                  <td>
                    <span className="ds-node-state__type" title={KIND_LABEL[row.kind]}>
                      {KIND_ICON[row.kind]}
                    </span>
                  </td>
                  <td className="ds-node-state__name">{row.name}</td>
                  <td><StatusBadge result={row.result} showMeta={false} /></td>
                  <td><DataStateDots python={row.python} memory={row.memory} disk={row.disk} /></td>
                  <td className="ds-node-state__mono">{row.schema ?? '—'}</td>
                  <td className="ds-node-state__mono">{row.table ?? '—'}</td>
                  <td className="ds-node-state__num">{row.rowCount != null ? row.rowCount.toLocaleString() : '—'}</td>
                  <td>{row.createdAtLabel ?? '—'}</td>
                  <td>{row.updatedAtLabel ?? '—'}</td>
                  <td>{row.lastRunLabel ?? '—'}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
      <div className="ds-node-state__footnote">
        Read-only. Status polls in near-real-time; running nodes show a ticking elapsed timer.
      </div>
    </div>
  )
}
