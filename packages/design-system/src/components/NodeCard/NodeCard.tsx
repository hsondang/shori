import type { ReactNode } from 'react'
import { StatusBadge } from '../StatusBadge/StatusBadge'
import type { NodeResultLike } from '../StatusBadge/status'

export type NodeKind = 'csv' | 'excel' | 'db' | 'transform' | 'export'
export type DbAccent = 'oracle' | 'postgres'

export interface NodeAction {
  label: string
  onClick?: () => void
  /** `muted` actions read as secondary (e.g. "View table"). */
  tone?: 'default' | 'muted'
}

export interface NodeCardProps {
  kind: NodeKind
  /** Required when `kind === 'db'` to pick the per-engine accent. */
  accent?: DbAccent
  title: string
  tableName?: string
  subtitle?: ReactNode
  icon?: ReactNode
  result?: NodeResultLike | null
  /**
   * One action list shared by the card and the config panel (F2). The same
   * verbs render in both places instead of "Materialize" here / "Execute" there.
   */
  actions?: NodeAction[]
  selected?: boolean
  onSelect?: () => void
  onViewError?: () => void
  /** Arbitrary content inserted after subtitle and before the status badge. */
  children?: ReactNode
}

const KIND_LABEL: Record<NodeKind, string> = {
  csv: 'CSV Source',
  excel: 'Excel Source',
  db: 'Database Source',
  transform: 'Transform',
  export: 'Export',
}

/**
 * The graph node card across all five kinds and the full status range
 * (idle → connecting → running → success → cached → error → cancelled). The
 * accent (header colour + action-link colour) is driven by CSS variables set
 * per `--<kind|accent>` modifier, so colour is themeable, not inlined (F6).
 */
export function NodeCard({
  kind,
  accent,
  title,
  tableName,
  subtitle,
  icon,
  result,
  actions = [],
  selected = false,
  onSelect,
  onViewError,
  children,
}: NodeCardProps) {
  const variant = kind === 'db' ? (accent ?? 'postgres') : kind
  const isError = result?.status === 'error'

  const classes = [
    'ds-node-card',
    `ds-node-card--${variant}`,
    selected ? 'is-selected' : '',
    isError ? 'is-error' : '',
  ]
    .filter(Boolean)
    .join(' ')

  return (
    <div
      className={classes}
      role="button"
      tabIndex={0}
      aria-label={`${KIND_LABEL[kind]}: ${title}`}
      onClick={onSelect}
      onKeyDown={(event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault()
          onSelect?.()
        }
      }}
    >
      <div className="ds-node-card__header">
        {icon != null && <span className="ds-node-card__icon">{icon}</span>}
        <span className="ds-node-card__title">{title}</span>
      </div>

      <div className="ds-node-card__body">
        {tableName && <div className="ds-node-card__table">{tableName}</div>}
        {subtitle != null && <div className="ds-node-card__subtitle">{subtitle}</div>}
        {children}

        {result && <StatusBadge result={result} />}

        {isError && onViewError && (
          <button
            type="button"
            className="ds-node-card__error"
            onClick={(event) => {
              event.stopPropagation()
              onViewError()
            }}
          >
            View error
          </button>
        )}

        {actions.length > 0 && (
          <div className="ds-node-card__actions">
            {actions.map((action, index) => (
              <button
                key={`${action.label}-${index}`}
                type="button"
                className={['ds-node-card__action', action.tone === 'muted' ? 'is-muted' : '']
                  .filter(Boolean)
                  .join(' ')}
                onClick={(event) => {
                  event.stopPropagation()
                  action.onClick?.()
                }}
              >
                {action.label}
              </button>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
