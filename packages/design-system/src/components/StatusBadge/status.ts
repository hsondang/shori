// The single source of truth for node execution status presentation.
//
// The audit (F1, F5) found "is this node busy?" and "what label/colour does this
// status get?" computed independently in at least three places (NodeStatusBadge,
// the panel's `nodeStatusLabel`, and the Execute button's label ternary), which
// let them drift (badge says "Cancelled", panel says "Status: cancelled"). Every
// surface should derive presentation from `statusPresentation()` instead.

export type NodeStatus = 'idle' | 'connecting' | 'running' | 'success' | 'error' | 'cancelled'

/** What a run is producing — specialises the `running` verb (spec §1.1). */
export type RunMode = 'preview' | 'load' | 'materialize'

export interface NodeResultLike {
  status: NodeStatus
  /** A successful result served from cache rather than re-executed. */
  cached?: boolean
  rowCount?: number | null
  columnCount?: number | null
  executionTimeMs?: number | null
  /** Human elapsed string for in-flight runs, e.g. "3s". */
  elapsedLabel?: string | null
  /** Drives the running verb: "Loading…" (load) / "Materializing…" (materialize) / "Live preview" (preview). */
  mode?: RunMode | null
}

/** `cached` is a presentational tone even though it is not a raw `NodeStatus`. */
export type StatusTone = 'idle' | 'connecting' | 'running' | 'success' | 'cached' | 'error' | 'cancelled'

export interface StatusPresentation {
  tone: StatusTone
  label: string
  /** Whether the leading status dot should pulse (connecting/running). */
  dotAnimated: boolean
  /** Whether controls (Execute/Run/Preview) should reflect a working node. */
  isBusy: boolean
}

export function statusPresentation(result?: NodeResultLike | null): StatusPresentation {
  if (!result) {
    return { tone: 'idle', label: 'Idle', dotAnimated: false, isBusy: false }
  }

  switch (result.status) {
    case 'connecting':
      return { tone: 'connecting', label: 'Connecting', dotAnimated: true, isBusy: true }
    case 'running': {
      const verb =
        result.mode === 'load'
          ? 'Loading'
          : result.mode === 'materialize'
            ? 'Materializing'
            : result.mode === 'preview'
              ? 'Live preview'
              : 'Running'
      return {
        tone: 'running',
        label: result.elapsedLabel ? `${verb} · ${result.elapsedLabel}` : verb,
        dotAnimated: true,
        isBusy: true,
      }
    }
    case 'success':
      return result.cached
        ? { tone: 'cached', label: 'Cached', dotAnimated: false, isBusy: false }
        : { tone: 'success', label: 'Success', dotAnimated: false, isBusy: false }
    case 'error':
      return { tone: 'error', label: 'Error', dotAnimated: false, isBusy: false }
    case 'cancelled':
      return { tone: 'cancelled', label: 'Cancelled', dotAnimated: false, isBusy: false }
    default:
      return { tone: 'idle', label: 'Idle', dotAnimated: false, isBusy: false }
  }
}

export function formatExecutionMs(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)}ms`
  return `${(ms / 1000).toFixed(1)}s`
}
