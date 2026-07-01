import { useEffect, useRef, useState, type ReactNode } from 'react'
import { createPortal } from 'react-dom'
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
 * A GitHub-style split button for a node's actions: the first action is the
 * default primary button; the rest live behind a caret dropdown. The menu is
 * portaled to <body> so it escapes the card's `overflow: hidden` and React
 * Flow's transformed viewport, and closes on any outside interaction.
 */
function NodeActions({ actions }: { actions: NodeAction[] }) {
  const [open, setOpen] = useState(false)
  const [pos, setPos] = useState<{ top: number; left: number; minWidth: number } | null>(null)
  const groupRef = useRef<HTMLDivElement>(null)
  const menuRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!open) return
    const onDown = (event: MouseEvent) => {
      const target = event.target as Node
      if (groupRef.current?.contains(target) || menuRef.current?.contains(target)) return
      setOpen(false)
    }
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setOpen(false)
    }
    document.addEventListener('mousedown', onDown)
    document.addEventListener('keydown', onKey)
    return () => {
      document.removeEventListener('mousedown', onDown)
      document.removeEventListener('keydown', onKey)
    }
  }, [open])

  if (actions.length === 0) return null

  const [primary, ...rest] = actions

  const runPrimary = (event: { stopPropagation: () => void }) => {
    event.stopPropagation()
    primary.onClick?.()
  }

  const toggleMenu = (event: { stopPropagation: () => void }) => {
    event.stopPropagation()
    if (!open && groupRef.current) {
      const rect = groupRef.current.getBoundingClientRect()
      // Clamp into the viewport so a card near an edge doesn't push the menu off-screen.
      const menuWidth = Math.max(rect.width, 160)
      const left = Math.max(8, Math.min(rect.left, window.innerWidth - menuWidth - 8))
      setPos({ top: rect.bottom + 4, left, minWidth: rect.width })
    }
    setOpen((value) => !value)
  }

  return (
    <div className="ds-node-card__actions">
      <div className="ds-node-card__split" ref={groupRef}>
        <button type="button" className="ds-node-card__btn ds-node-card__btn--primary" onClick={runPrimary}>
          {primary.label}
        </button>
        {rest.length > 0 && (
          <button
            type="button"
            className="ds-node-card__caret"
            aria-haspopup="menu"
            aria-expanded={open}
            aria-label="More actions"
            onClick={toggleMenu}
          >
            <span aria-hidden="true">▾</span>
          </button>
        )}
      </div>

      {open && pos && rest.length > 0 && createPortal(
        <div
          ref={menuRef}
          role="menu"
          className="ds-node-card__menu"
          style={{ position: 'fixed', top: pos.top, left: pos.left, minWidth: pos.minWidth }}
          onClick={(event) => event.stopPropagation()}
        >
          {rest.map((action, index) => (
            <button
              key={`${action.label}-${index}`}
              type="button"
              role="menuitem"
              className={['ds-node-card__menu-item', action.tone === 'muted' ? 'is-muted' : '']
                .filter(Boolean)
                .join(' ')}
              onClick={(event) => {
                event.stopPropagation()
                setOpen(false)
                action.onClick?.()
              }}
            >
              {action.label}
            </button>
          ))}
        </div>,
        document.body,
      )}
    </div>
  )
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

        <NodeActions actions={actions} />
      </div>
    </div>
  )
}
