import type { LocationDot, PythonDot } from '@shori/design-system'
import type {
  LivePreviewState,
  NodeCacheStatus,
  NodeLocationStatus,
  NodeType,
} from '../types/pipeline'

export interface NodeDataState {
  /** `undefined` means "not applicable" (renders "—") — distinct from an empty-but-fillable dot. */
  python?: PythonDot
  memory: LocationDot
  disk: LocationDot
}

function locationDot(status: NodeLocationStatus | undefined): LocationDot {
  if (!status || !status.present) return 'empty'
  if (status.state === 'loading') return 'loading'
  if (status.state === 'stale') return 'stale'
  return 'fresh'
}

/**
 * Derives the three-location data state (node-state-model.md §1.3, §7) from the
 * pieces already tracked in the store. Pure so it's easy to unit test and reuse
 * across the canvas chip and the node-state table.
 */
export function computeNodeDataState(params: {
  nodeType: NodeType
  cacheStatus: NodeCacheStatus | undefined
  livePreview: LivePreviewState | undefined
  /** CSV only: whether preprocessing is turned on for this node. */
  csvPreprocessingEnabled?: boolean
  /** CSV only: whether a reviewed preprocessed artifact exists for this node. */
  csvPreprocessArtifactReady?: boolean
}): NodeDataState {
  const { nodeType, cacheStatus, livePreview, csvPreprocessingEnabled, csvPreprocessArtifactReady } = params
  const memory = locationDot(cacheStatus?.locations.in_memory)
  const disk = locationDot(cacheStatus?.locations.materialized)

  let python: PythonDot | undefined
  if (nodeType === 'db_source' || nodeType === 'transform') {
    // Live preview is a held session — 'live' the moment one is open (sampled, non-consumable).
    python = livePreview?.sessionId ? 'live' : 'empty'
  } else if (nodeType === 'csv_source') {
    // Plain CSV registers straight into DuckDB (no Python-memory hop) → not applicable.
    python = csvPreprocessingEnabled ? (csvPreprocessArtifactReady ? 'live' : 'empty') : undefined
  }
  // excel_source, export: preview deferred / not applicable — python stays undefined.

  return { python, memory, disk }
}
