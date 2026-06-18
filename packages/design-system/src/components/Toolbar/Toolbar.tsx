import type { ReactNode, DragEvent } from 'react'

export type ToolbarChipAccent = 'csv' | 'excel' | 'transform' | 'export' | 'db'

export interface ToolbarChipProps {
  accent: ToolbarChipAccent
  label: string
  draggable?: boolean
  onDragStart?: (e: DragEvent<HTMLDivElement>) => void
  onClick?: () => void
  children?: ReactNode
}

export interface ToolbarProps {
  children: ReactNode
  className?: string
}

export function Toolbar({ children, className }: ToolbarProps) {
  return (
    <div className={`ds-toolbar${className ? ` ${className}` : ''}`}>
      {children}
    </div>
  )
}

export function ToolbarChip({ accent, label, draggable, onDragStart, onClick, children }: ToolbarChipProps) {
  return (
    <div
      className={`ds-toolbar-chip ds-toolbar-chip--${accent}`}
      draggable={draggable}
      onDragStart={onDragStart}
      onClick={onClick}
      role={onClick ? 'button' : undefined}
      tabIndex={onClick ? 0 : undefined}
    >
      {children ?? label}
    </div>
  )
}

export function ToolbarSeparator() {
  return <div className="ds-toolbar-sep" aria-hidden="true" />
}
