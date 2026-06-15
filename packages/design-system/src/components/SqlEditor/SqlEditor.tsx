import type { ReactNode, CSSProperties } from 'react'

export interface SqlEditorProps {
  children: ReactNode
  minHeight?: number | string
  style?: CSSProperties
  className?: string
}

export function SqlEditor({ children, minHeight, style, className }: SqlEditorProps) {
  return (
    <div
      className={`ds-sql-editor${className ? ` ${className}` : ''}`}
      style={minHeight !== undefined ? { minHeight, ...style } : style}
    >
      {children}
    </div>
  )
}
