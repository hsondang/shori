import { Modal } from '../Modal/Modal'
import { Button } from '../Button/Button'

export type LoadDestination = 'in_memory' | 'materialized'

export interface LoadDestinationCandidate {
  nodeId: string
  /** Node display label (falls back to table name if unset). */
  label: string
  tableName: string
}

export interface LoadDestinationDialogProps {
  open: boolean
  /** Nodes with no data in either DuckDB location — each needs a destination pick. */
  candidates: LoadDestinationCandidate[]
  /** Per-node choice; defaults to 'in_memory' when a candidate has no entry yet. */
  choices: Record<string, LoadDestination>
  onChoiceChange: (nodeId: string, mode: LoadDestination) => void
  onApplyToAll: (mode: LoadDestination) => void
  onConfirm: () => void
  onCancel: () => void
  confirmLabel?: string
  busy?: boolean
}

/**
 * The batched load/materialize prompt (docs/node-state-model.md §6): when a run
 * needs upstream data that doesn't exist in memory or on disk, this collects a
 * destination choice for every such node in one confirmation window instead of
 * one dialog per node.
 */
export function LoadDestinationDialog({
  open,
  candidates,
  choices,
  onChoiceChange,
  onApplyToAll,
  onConfirm,
  onCancel,
  confirmLabel = 'Load and continue',
  busy = false,
}: LoadDestinationDialogProps) {
  return (
    <Modal
      open={open}
      onClose={onCancel}
      size="md"
      title="Choose where to load upstream data"
      description="These nodes have no data yet in memory or on disk. Pick a destination for each before continuing."
      footer={
        <>
          <Button variant="ghost" onClick={onCancel} disabled={busy}>Cancel</Button>
          <Button variant="primary" onClick={onConfirm} disabled={busy || candidates.length === 0}>
            {busy ? 'Loading…' : confirmLabel}
          </Button>
        </>
      }
    >
      <div className="ds-load-dest">
        <div className="ds-load-dest__all">
          <span className="ds-load-dest__all-label">Apply to all:</span>
          <button type="button" className="ds-load-dest__all-btn" onClick={() => onApplyToAll('in_memory')}>In memory</button>
          <button type="button" className="ds-load-dest__all-btn" onClick={() => onApplyToAll('materialized')}>Materialize</button>
        </div>
        <ul className="ds-load-dest__list">
          {candidates.map((candidate) => {
            const choice = choices[candidate.nodeId] ?? 'in_memory'
            return (
              <li key={candidate.nodeId} className="ds-load-dest__row">
                <div className="ds-load-dest__row-info">
                  <div className="ds-load-dest__row-label">{candidate.label}</div>
                  <div className="ds-load-dest__row-table">{candidate.tableName}</div>
                </div>
                <div className="ds-load-dest__segment" role="radiogroup" aria-label={`Destination for ${candidate.label}`}>
                  <button
                    type="button"
                    role="radio"
                    aria-checked={choice === 'in_memory'}
                    className={`ds-load-dest__seg-btn ${choice === 'in_memory' ? 'is-active' : ''}`}
                    onClick={() => onChoiceChange(candidate.nodeId, 'in_memory')}
                  >
                    In memory
                  </button>
                  <button
                    type="button"
                    role="radio"
                    aria-checked={choice === 'materialized'}
                    className={`ds-load-dest__seg-btn ${choice === 'materialized' ? 'is-active' : ''}`}
                    onClick={() => onChoiceChange(candidate.nodeId, 'materialized')}
                  >
                    Materialize
                  </button>
                </div>
              </li>
            )
          })}
        </ul>
      </div>
    </Modal>
  )
}
