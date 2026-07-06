import { useEffect, useState } from 'react'
import { useParams } from 'react-router-dom'
import { NodeStateTable, formatExecutionMs, type NodeStateKind, type NodeStateRow } from '@shori/design-system'
import { usePipelineStore } from '../../store/pipelineStore'
import { toResultLike, deriveRunMode } from '../../lib/dsStatus'
import { computeNodeDataState } from '../../lib/nodeDataState'
import { getResultElapsedLabel } from '../../lib/executionTiming'
import type { CsvPreprocessingConfig, NodeLoadMode, NodeType } from '../../types/pipeline'

const KIND_BY_NODE_TYPE: Record<NodeType, NodeStateKind> = {
  csv_source: 'csv',
  excel_source: 'excel',
  // Hubs are excluded from the rows below; the entry only satisfies the Record type.
  excel_workbook: 'excel',
  db_source: 'db',
  transform: 'transform',
  export: 'export',
}

function formatRelativeTime(iso: string | null | undefined): string | null {
  if (!iso) return null
  const then = Date.parse(iso)
  if (Number.isNaN(then)) return null
  const diffMs = Date.now() - then
  const diffSeconds = Math.round(diffMs / 1000)
  if (diffSeconds < 60) return 'just now'
  const diffMinutes = Math.round(diffSeconds / 60)
  if (diffMinutes < 60) return `${diffMinutes}m ago`
  const diffHours = Math.round(diffMinutes / 60)
  if (diffHours < 24) return `${diffHours}h ago`
  const diffDays = Math.round(diffHours / 24)
  return `${diffDays}d ago`
}

export default function NodeStatePage() {
  const { projectId } = useParams()
  const pipelineId = usePipelineStore((s) => s.pipelineId)
  const loadPipeline = usePipelineStore((s) => s.loadPipeline)
  const refreshCacheStatus = usePipelineStore((s) => s.refreshCacheStatus)
  const [status, setStatus] = useState<'loading' | 'ready' | 'missing'>('loading')

  const nodes = usePipelineStore((s) => s.nodes)
  const nodeResults = usePipelineStore((s) => s.nodeResults)
  const cacheStatusByNodeId = usePipelineStore((s) => s.cacheStatusByNodeId)
  const livePreviewsByNodeId = usePipelineStore((s) => s.livePreviewsByNodeId)
  const csvPreprocessArtifacts = usePipelineStore((s) => s.csvPreprocessArtifacts)
  const executionClockNow = usePipelineStore((s) => s.executionClockNow)
  const setSelectedNodeId = usePipelineStore((s) => s.setSelectedNodeId)

  useEffect(() => {
    if (!projectId) {
      setStatus('missing')
      return
    }
    if (pipelineId === projectId) {
      setStatus('ready')
      void refreshCacheStatus()
      return
    }
    let cancelled = false
    setStatus('loading')
    void loadPipeline(projectId)
      .then(() => { if (!cancelled) setStatus('ready') })
      .catch(() => { if (!cancelled) setStatus('missing') })
    return () => { cancelled = true }
  }, [loadPipeline, pipelineId, projectId, refreshCacheStatus])

  if (status === 'loading') {
    return (
      <div className="flex h-full items-center justify-center text-sm text-stone-500">
        Loading project...
      </div>
    )
  }

  if (status === 'missing') {
    return (
      <div className="flex h-full items-center justify-center p-8">
        <div className="rounded-3xl border border-stone-200 bg-white px-8 py-10 text-center shadow-sm">
          <h2 className="font-serif text-3xl text-stone-900">Project not found</h2>
          <p className="mt-3 text-sm text-stone-600">
            The selected project could not be loaded from the local project catalog.
          </p>
        </div>
      </div>
    )
  }

  const rows: NodeStateRow[] = nodes
    // Exports produce no table; workbook hubs have no data state at all
    // (docs/excel-node-model.md §6) — neither belongs in this table.
    .filter((node) => node.type !== 'export' && node.type !== 'excel_workbook')
    .map((node) => {
      const d = node.data as Record<string, unknown>
      const nodeType = node.type as NodeType
      const config = (d.config as Record<string, unknown>) ?? {}
      const cacheStatus = cacheStatusByNodeId[node.id]
      const live = livePreviewsByNodeId[node.id]
      const result = nodeResults[node.id]
      const elapsed = result ? getResultElapsedLabel(result, executionClockNow) : null
      const isLivePreviewOpening = Boolean(live?.loading && !live?.sessionId)
      const mode = deriveRunMode({ isLivePreviewOpening, loadMode: config.load_mode as NodeLoadMode | undefined })
      const preprocessing = config.preprocessing as CsvPreprocessingConfig | undefined

      const dataState = computeNodeDataState({
        nodeType,
        cacheStatus,
        livePreview: live,
        csvPreprocessingEnabled: Boolean(preprocessing?.enabled),
        csvPreprocessArtifactReady: Boolean(csvPreprocessArtifacts[node.id]),
      })

      const diskPresent = dataState.disk === 'fresh' || dataState.disk === 'stale' || dataState.disk === 'loading'
      const memoryPresent = dataState.memory === 'fresh' || dataState.memory === 'stale' || dataState.memory === 'loading'
      const schema = diskPresent ? 'main' : memoryPresent ? 'memory' : null
      const tableName = (d.tableName as string) || null
      const rowCount = cacheStatus?.row_count ?? result?.row_count ?? null
      const updatedAtLabel = formatRelativeTime(cacheStatus?.finished_at ?? result?.finished_at ?? null)
      const lastRunLabel = result?.execution_time_ms != null
        ? formatExecutionMs(result.execution_time_ms)
        : updatedAtLabel

      return {
        id: node.id,
        kind: KIND_BY_NODE_TYPE[nodeType],
        name: (d.label as string) || tableName || node.id,
        result: result ? toResultLike(result, elapsed, mode) : null,
        python: dataState.python,
        memory: dataState.memory,
        disk: dataState.disk,
        schema,
        table: diskPresent || memoryPresent ? tableName : null,
        rowCount,
        // The backend doesn't track a separate "first created" timestamp yet —
        // this approximates it with the most recent load/materialize time.
        createdAtLabel: updatedAtLabel,
        updatedAtLabel,
        lastRunLabel,
      }
    })

  return (
    <div className="flex h-full min-w-0 flex-col gap-4 overflow-hidden p-4">
      <div>
        <h1 className="text-lg font-semibold text-stone-900">Node state</h1>
        <p className="text-sm text-stone-500">
          Live state of every node in the pipeline. Run status and data state are independent — a node can be
          Success while its data is In memory · stale.
        </p>
      </div>
      <div className="min-h-0 flex-1">
        <NodeStateTable rows={rows} onSelectRow={setSelectedNodeId} />
      </div>
    </div>
  )
}
