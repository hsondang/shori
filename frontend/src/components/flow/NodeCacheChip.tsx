import { usePipelineStore } from '../../store/pipelineStore'

/**
 * Shows the persisted cache state for a table-producing node and a Refresh
 * action. Driven by the project DuckDB metadata (cacheStatusByNodeId), so it
 * survives reloads — unlike the per-run execution result. Hidden while the
 * node is actively running (the status badge covers that).
 */
export default function NodeCacheChip({ nodeId }: { nodeId: string }) {
  const status = usePipelineStore((s) => s.cacheStatusByNodeId[nodeId])
  const result = usePipelineStore((s) => s.nodeResults[nodeId])
  const executeSingleNode = usePipelineStore((s) => s.executeSingleNode)

  const isRunning = result?.status === 'running' || result?.status === 'connecting'
  if (isRunning || !status) return null

  // While the live execution result is showing success/error, the badge is
  // already informative; only surface the chip when it adds a "stale" or a
  // standalone "cached" signal (e.g. after a page reload with no result yet).
  if (status.state === 'stale') {
    return (
      <div className="flex items-center gap-1.5">
        <span className="inline-block rounded-full bg-amber-100 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-amber-700">
          Stale
        </span>
        <button
          type="button"
          className="text-[10px] font-semibold text-amber-700 hover:underline"
          onClick={(e) => { e.stopPropagation(); void executeSingleNode(nodeId, { force: true, loadPreviewOnSuccess: true }) }}
        >
          Refresh
        </button>
      </div>
    )
  }

  if (status.state === 'fresh' && !result) {
    return (
      <span className="inline-block rounded-full bg-sky-100 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-sky-700">
        Cached
      </span>
    )
  }

  return null
}
