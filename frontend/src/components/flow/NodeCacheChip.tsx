import { DataStateDots } from '@shori/design-system'
import { usePipelineStore } from '../../store/pipelineStore'
import { computeNodeDataState } from '../../lib/nodeDataState'
import type { CsvPreprocessingConfig, NodeType } from '../../types/pipeline'

/**
 * Shows the node's three-location data state (docs/node-state-model.md §1.3) as
 * compact dots, plus a Refresh action when any location is stale. Driven by the
 * project DuckDB metadata (cacheStatusByNodeId) and live-preview session state,
 * so it survives reloads — unlike the per-run execution result. Hidden while
 * the node is actively running (the status badge covers that).
 */
export default function NodeCacheChip({ nodeId }: { nodeId: string }) {
  const node = usePipelineStore((s) => s.nodes.find((candidate) => candidate.id === nodeId))
  const cacheStatus = usePipelineStore((s) => s.cacheStatusByNodeId[nodeId])
  const livePreview = usePipelineStore((s) => s.livePreviewsByNodeId[nodeId])
  const result = usePipelineStore((s) => s.nodeResults[nodeId])
  const artifactReady = usePipelineStore((s) => Boolean(s.csvPreprocessArtifacts[nodeId]))
  const executeSingleNode = usePipelineStore((s) => s.executeSingleNode)

  const isRunning = result?.status === 'running' || result?.status === 'connecting'
  if (isRunning || !node) return null

  const config = (node.data as Record<string, unknown>).config as Record<string, unknown>
  const preprocessing = config.preprocessing as CsvPreprocessingConfig | undefined

  const dataState = computeNodeDataState({
    nodeType: node.type as NodeType,
    cacheStatus,
    livePreview,
    csvPreprocessingEnabled: Boolean(preprocessing?.enabled),
    csvPreprocessArtifactReady: artifactReady,
  })
  const isStale = dataState.memory === 'stale' || dataState.disk === 'stale'

  return (
    <div className="flex items-center gap-1.5">
      <DataStateDots python={dataState.python} memory={dataState.memory} disk={dataState.disk} />
      {isStale && (
        <button
          type="button"
          className="text-[10px] font-semibold text-amber-700 hover:underline"
          onClick={(e) => { e.stopPropagation(); void executeSingleNode(nodeId, { force: true, loadPreviewOnSuccess: true }) }}
        >
          Refresh
        </button>
      )}
    </div>
  )
}
