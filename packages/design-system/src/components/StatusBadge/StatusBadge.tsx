import { formatExecutionMs, statusPresentation, type NodeResultLike } from './status'

export interface StatusBadgeProps {
  result?: NodeResultLike | null
  /** Show execution time and the "N rows × M cols" summary line. */
  showMeta?: boolean
}

/**
 * Renders a node's status from the shared `statusPresentation` model. Use this
 * everywhere a status is shown — node card, config panel header, anywhere — so
 * label and colour never diverge (F5).
 */
export function StatusBadge({ result, showMeta = true }: StatusBadgeProps) {
  const presentation = statusPresentation(result)
  const showTiming = showMeta && presentation.tone !== 'cached' && result?.executionTimeMs != null

  return (
    <div className="ds-status">
      <span className={`ds-status__badge ds-status__badge--${presentation.tone}`}>
        {presentation.dotAnimated && <span className="ds-status__dot" />}
        {presentation.label}
        {showTiming ? ` (${formatExecutionMs(result!.executionTimeMs!)})` : ''}
      </span>
      {showMeta && result?.rowCount != null && (
        <span className="ds-status__meta">
          {result.rowCount.toLocaleString()} rows × {result.columnCount ?? '?'} cols
        </span>
      )}
    </div>
  )
}
