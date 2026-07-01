// Three dots for a node's data state, one per storage location (spec:
// docs/node-state-model.md §1.3, §7). Position encodes the location; colour
// encodes presence (green/yellow/orange, grey when empty); a dashed ring — not
// opacity — encodes stale (accessibility). The live (Python-memory) dot is
// sampled, so it carries a soft ring, and is omitted as "—" for node types that
// have no preview.

export type LocationDot = 'empty' | 'fresh' | 'stale' | 'loading'
export type PythonDot = 'empty' | 'live'

export interface DataStateDotsProps {
  /** Live preview (Python memory). Omit entirely when the node has no preview → renders "—". */
  python?: PythonDot
  /** DuckDB in-memory copy (RAM scratch catalog). */
  memory: LocationDot
  /** DuckDB materialized copy (persisted in the project file). */
  disk: LocationDot
  /** Render a short text label beside each dot (used in wider layouts / legends). */
  showLabels?: boolean
}

type Slot = { kind: 'python' | 'memory' | 'disk'; label: string; state: LocationDot | PythonDot | 'na'; title: string }

const STATE_WORD: Record<string, string> = {
  na: 'not applicable',
  empty: 'no data',
  live: 'live (sampled)',
  fresh: 'fresh',
  stale: 'stale',
  loading: 'loading',
}

export function DataStateDots({ python, memory, disk, showLabels = false }: DataStateDotsProps) {
  const slots: Slot[] = [
    { kind: 'python', label: 'Live', state: python ?? 'na', title: `Live preview — ${STATE_WORD[python ?? 'na']}` },
    { kind: 'memory', label: 'In memory', state: memory, title: `In memory (RAM) — ${STATE_WORD[memory]}` },
    { kind: 'disk', label: 'Materialized', state: disk, title: `Materialized (on disk) — ${STATE_WORD[disk]}` },
  ]

  return (
    <span
      className="ds-datadots"
      role="img"
      aria-label={`Data state: live ${STATE_WORD[python ?? 'na']}, in memory ${STATE_WORD[memory]}, materialized ${STATE_WORD[disk]}`}
    >
      {slots.map((slot) => (
        <span key={slot.kind} className="ds-datadots__slot">
          {slot.state === 'na' ? (
            <span className="ds-datadots__na" title={slot.title}>—</span>
          ) : (
            <span
              className={`ds-datadots__dot ds-datadots__dot--${slot.kind} is-${slot.state}`}
              title={slot.title}
            />
          )}
          {showLabels && <span className="ds-datadots__label">{slot.label}</span>}
        </span>
      ))}
    </span>
  )
}
