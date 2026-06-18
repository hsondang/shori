import {
  useCallback,
  useEffect,
  useRef,
  type MouseEvent as ReactMouseEvent,
  type ReactNode,
} from 'react'

export type DockSide = 'right' | 'left' | 'bottom' | 'top'

export interface DockPanelProps {
  /** Which edge the panel is docked to. Determines resize axis + border side. */
  side: DockSide
  /** Width (left/right) or height (top/bottom) in px when expanded. */
  size: number
  onResize?: (size: number) => void
  minSize?: number
  maxSize?: number
  resizable?: boolean
  collapsible?: boolean
  collapsed?: boolean
  onToggleCollapsed?: () => void
  /** Size (px) when collapsed — typically just the header strip. */
  collapsedSize?: number
  title?: ReactNode
  /** Header right-slot, before the collapse control. */
  actions?: ReactNode
  children: ReactNode
  className?: string
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max)
}

/**
 * One dockable panel for both the right config pane and the bottom data preview.
 * Both are collapsible *and* resizable the same way — closing the gap the audit
 * flagged (F3): the right panel could not be hidden while the bottom one could.
 *
 * `collapsed` is controlled (consumer owns the state, as the app already does).
 * Resize is delegated through `onResize` with built-in min/max clamping.
 */
export function DockPanel({
  side,
  size,
  onResize,
  minSize = 0,
  maxSize = Number.POSITIVE_INFINITY,
  resizable = true,
  collapsible = true,
  collapsed = false,
  onToggleCollapsed,
  collapsedSize = 40,
  title,
  actions,
  children,
  className,
}: DockPanelProps) {
  const isHorizontalAxis = side === 'left' || side === 'right'
  const handleFirst = side === 'right' || side === 'bottom'
  const dragRef = useRef<{ start: number; startSize: number } | null>(null)

  const handleMove = useCallback(
    (event: MouseEvent) => {
      const drag = dragRef.current
      if (!drag || !onResize) return
      const pos = isHorizontalAxis ? event.clientX : event.clientY
      const delta = handleFirst ? drag.start - pos : pos - drag.start
      onResize(clamp(drag.startSize + delta, minSize, maxSize))
    },
    [handleFirst, isHorizontalAxis, maxSize, minSize, onResize],
  )

  const stop = useCallback(() => {
    dragRef.current = null
    document.body.style.removeProperty('user-select')
  }, [])

  useEffect(() => {
    window.addEventListener('mousemove', handleMove)
    window.addEventListener('mouseup', stop)
    return () => {
      window.removeEventListener('mousemove', handleMove)
      window.removeEventListener('mouseup', stop)
      stop()
    }
  }, [handleMove, stop])

  const startResize = (event: ReactMouseEvent<HTMLDivElement>) => {
    if (!resizable || collapsed) return
    dragRef.current = {
      start: isHorizontalAxis ? event.clientX : event.clientY,
      startSize: size,
    }
    document.body.style.userSelect = 'none'
    event.preventDefault()
  }

  const effectiveSize = collapsed ? collapsedSize : size
  const style = isHorizontalAxis ? { width: effectiveSize } : { height: effectiveSize }

  const handle =
    resizable && !collapsed ? (
      <div
        role="separator"
        aria-orientation={isHorizontalAxis ? 'vertical' : 'horizontal'}
        aria-label="Resize panel"
        className={`ds-dock__handle ds-dock__handle--${isHorizontalAxis ? 'v' : 'h'}`}
        onMouseDown={startResize}
      >
        <span className="ds-dock__grip" />
      </div>
    ) : null

  const showHeader = title != null || actions != null || collapsible

  return (
    <div
      className={['ds-dock', `ds-dock--${side}`, collapsed ? 'is-collapsed' : '', className]
        .filter(Boolean)
        .join(' ')}
      style={style}
    >
      {handleFirst && handle}
      <div className="ds-dock__inner">
        {showHeader && (
          <div className="ds-dock__header">
            <div className="ds-dock__title">{title}</div>
            <div className="ds-dock__header-right">
              {actions}
              {collapsible && (
                <button
                  type="button"
                  className="ds-dock__collapse"
                  aria-label={collapsed ? 'Expand panel' : 'Collapse panel'}
                  aria-expanded={!collapsed}
                  onClick={onToggleCollapsed}
                >
                  {collapsed ? 'Expand' : 'Collapse'}
                </button>
              )}
            </div>
          </div>
        )}
        {!collapsed && <div className="ds-dock__body">{children}</div>}
      </div>
      {!handleFirst && handle}
    </div>
  )
}
