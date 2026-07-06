import { create } from 'zustand'
import {
  type Node,
  type Edge,
  type OnNodesChange,
  type OnEdgesChange,
  type Connection,
  applyNodeChanges,
  applyEdgeChanges,
  addEdge,
} from '@xyflow/react'
import type {
  ActivePreviewTarget,
  CsvPreprocessingConfig,
  CsvSourceConfig,
  CsvTextPreviewData,
  DatabaseSourceConfig,
  ExcelSourceConfig,
  ExecutionRunStatus,
  LivePreviewState,
  LoadDestinationCandidate,
  LoadDestinationPromptState,
  MaterializedPreviewTab,
  NodeCacheStatus,
  NodeEditorDraft,
  NodeEditorMode,
  NodeLabelMode,
  NodeLoadMode,
  NodeType,
  NodeExecutionResult,
  PipelineDefinition,
  ProjectSettings,
  SavedDatabaseConnection,
  SavedDatabaseConnectionInput,
  TablePreviewData,
  TransientPreviewState,
} from '../types/pipeline'
import { DEFAULT_PROJECT_SETTINGS } from '../types/pipeline'
import * as api from '../api/client'
import { getCsvPreprocessFingerprint } from '../lib/csvPreprocessing'
import {
  defaultDatabaseSourceConfig,
  defaultOracleFetchConfig,
  makeGlobalDatabaseSourceConfig,
} from '../lib/databaseConnections'
import {
  createBlankPipelineDefinition,
  snapshotPipelineDefinition,
} from '../lib/pipelineDefinitions'
import { buildNodesById, dataEdges, isStructuralEdge } from '../lib/structuralEdges'

interface PipelineState {
  // React Flow
  nodes: Node[]
  edges: Edge[]
  onNodesChange: OnNodesChange
  onEdgesChange: OnEdgesChange
  onConnect: (connection: Connection) => void

  // Pipeline metadata
  pipelineId: string
  pipelineName: string
  databaseConnections: SavedDatabaseConnection[]
  projectSettings: ProjectSettings
  savedPipelineSnapshot: string
  hasUnsavedChanges: boolean
  projectListRevision: number
  setPipelineName: (name: string) => void
  updateProjectSettings: (patch: Partial<ProjectSettings>) => void

  // Execution results
  nodeResults: Record<string, NodeExecutionResult>
  activeExecutions: Record<string, ExecutionRunStatus>
  activeExecutionIdByNodeId: Record<string, string>
  activePipelineExecutionId: string | null
  executionClockNow: number
  errorDialogNodeId: string | null

  // Selected node
  selectedNodeId: string | null
  setSelectedNodeId: (id: string | null) => void
  openNodeError: (nodeId: string) => void
  closeNodeError: () => void

  // Data preview
  previewTabsByNodeId: Record<string, MaterializedPreviewTab>
  previewTabOrder: string[]
  activePreviewTarget: ActivePreviewTarget | null
  transientPreview: TransientPreviewState
  csvPreprocessArtifacts: Record<string, string>
  selectPreviewTab: (nodeId: string) => void

  // Persisted cache status (per node, from the project DuckDB metadata)
  cacheStatusByNodeId: Record<string, NodeCacheStatus>
  refreshCacheStatus: () => Promise<void>

  // Live preview sessions (DBeaver-style, no table created)
  livePreviewsByNodeId: Record<string, LivePreviewState>
  startLivePreview: (nodeId: string) => Promise<void>
  loadMoreLivePreview: (nodeId: string) => Promise<void>
  materializeLivePreview: (nodeId: string, intoMemory?: boolean) => Promise<void>
  closeLivePreview: (nodeId: string) => Promise<void>

  // Batched load/materialize prompt (node-state-model.md §6): shown when a run
  // needs upstream data that has no copy in either DuckDB location yet.
  loadDestinationPrompt: LoadDestinationPromptState | null
  setLoadDestinationChoice: (nodeId: string, mode: NodeLoadMode) => void
  applyLoadDestinationChoiceToAll: (mode: NodeLoadMode) => void
  confirmLoadDestinationPrompt: () => Promise<void>
  cancelLoadDestinationPrompt: () => void

  // Node editor
  nodeEditorMode: NodeEditorMode
  nodeEditorDraft: NodeEditorDraft | null
  editingNodeId: string | null

  // Actions
  addNode: (type: NodeType, position: { x: number; y: number }) => void
  addDatabaseConnection: (connection: SavedDatabaseConnectionInput) => string
  updateDatabaseConnection: (id: string, connection: SavedDatabaseConnectionInput) => void
  deleteDatabaseConnection: (id: string) => void
  addDatabaseSourceFromConnection: (connectionId: string, position: { x: number; y: number }) => string | null
  openCreateNodeEditor: (draft: NodeEditorDraft) => void
  openEditNodeEditor: (nodeId: string) => void
  updateNodeEditorDraft: (patch: Partial<NodeEditorDraft>) => void
  closeNodeEditor: () => void
  commitNodeEditor: () => string | null
  updateNodeData: (nodeId: string, data: Record<string, unknown>) => void
  deleteNode: (nodeId: string) => void
  executePipeline: (force?: boolean) => Promise<void>
  executeSingleNode: (nodeId: string, options?: { loadPreviewOnSuccess?: boolean; force?: boolean }) => Promise<void>
  runNodeWithLoadMode: (nodeId: string, loadMode: NodeLoadMode, options?: { loadPreviewOnSuccess?: boolean; force?: boolean }) => Promise<void>
  runTransformPreview: (nodeId: string) => Promise<void>
  loadCsvPreview: (nodeId: string, filePath: string) => Promise<void>
  loadPreprocessedCsvPreview: (nodeId: string, filePath: string, preprocessing: CsvPreprocessingConfig) => Promise<void>
  loadTablePreview: (nodeId: string, tableName: string, offset?: number, options?: { forceReload?: boolean }) => Promise<void>
  loadMoreTablePreview: (nodeId: string) => Promise<void>
  savePipeline: () => Promise<void>
  loadPipeline: (id: string) => Promise<void>
  newPipeline: () => void
  markProjectCatalogChanged: () => void
  confirmDiscardChanges: (nextProjectName?: string) => boolean
  abortDatabaseNodeExecution: (nodeId: string) => Promise<void>
}

type StoreSet = (
  partial: Partial<PipelineState> | ((state: PipelineState) => Partial<PipelineState>)
) => void

let nodeCounter = 0
const initialPipeline = createBlankPipelineDefinition()

function generateNodeId(): string {
  nodeCounter++
  return `node_${nodeCounter}_${Date.now().toString(36)}`
}

function cloneValue<T>(value: T): T {
  if (typeof structuredClone === 'function') {
    return structuredClone(value)
  }

  return JSON.parse(JSON.stringify(value)) as T
}

function defaultLabel(type: NodeType): string {
  switch (type) {
    case 'csv_source': return 'CSV Source'
    case 'excel_source': return 'Excel Source'
    case 'excel_workbook': return 'Excel Workbook'
    case 'db_source': return 'Database Source'
    case 'transform': return 'Transform'
    case 'export': return 'Export'
  }
}

function defaultConfig(type: NodeType): Record<string, unknown> {
  switch (type) {
    case 'csv_source': return {
      file_path: '',
      original_filename: '',
      load_mode: 'in_memory',
      preprocessing: {
        enabled: false,
        script_path: '',
      },
    }
    case 'excel_source': return {
      file_path: '',
      original_filename: '',
      sheet_names: [],
      selected_sheet: '',
      load_mode: 'in_memory',
      header: true,
      all_varchar: false,
    }
    case 'excel_workbook': return {
      file_path: '',
      original_filename: '',
      sheet_names: [],
    }
    case 'db_source': return defaultDatabaseSourceConfig('postgres') as unknown as Record<string, unknown>
    case 'transform': return { sql: '', load_mode: 'in_memory' }
    case 'export': return { format: 'csv', destination: 'local', output_path: '' }
  }
}

function defaultAutoLabel(type: NodeType): string {
  return defaultLabel(type)
}

export function buildNodeDraft(
  type: NodeType,
  position: { x: number; y: number },
  overrides: Partial<NodeEditorDraft> = {},
): NodeEditorDraft {
  const id = overrides.id ?? generateNodeId()
  const autoLabel = overrides.autoLabel ?? defaultAutoLabel(type)
  const label = overrides.label ?? autoLabel

  return {
    id,
    type,
    position: overrides.position ?? position,
    label,
    autoLabel,
    labelMode: overrides.labelMode ?? deriveLabelMode(label, autoLabel),
    tableName: overrides.tableName ?? (type === 'excel_workbook' ? '' : id),
    config: cloneValue(overrides.config ?? defaultConfig(type)),
  }
}

function buildNodeFromDraft(draft: NodeEditorDraft): Node {
  return {
    id: draft.id,
    type: draft.type,
    position: draft.position,
    data: {
      label: draft.label,
      description: draft.description ?? '',
      autoLabel: draft.autoLabel,
      labelMode: draft.labelMode,
      tableName: draft.tableName,
      config: cloneValue(draft.config),
    },
  }
}

function deriveLabelMode(label: string, autoLabel: string): NodeLabelMode {
  return label === autoLabel ? 'auto' : 'custom'
}

function inferLegacyLabelMetadata(type: NodeType, label: string) {
  if (type === 'db_source') {
    return {
      autoLabel: label || defaultAutoLabel(type),
      labelMode: 'auto' as const,
    }
  }

  const autoLabel = defaultAutoLabel(type)
  return {
    autoLabel,
    labelMode: deriveLabelMode(label, autoLabel),
  }
}

function getNodeLabelMetadata(node: Node): { label: string; autoLabel: string; labelMode: NodeLabelMode } {
  const data = (node.data as Record<string, unknown> | undefined) ?? {}
  const label = typeof data.label === 'string' ? data.label : ''
  const autoLabel = typeof data.autoLabel === 'string' ? data.autoLabel : null
  const labelMode = data.labelMode === 'custom' || data.labelMode === 'auto'
    ? data.labelMode
    : null

  if (autoLabel && labelMode) {
    return { label, autoLabel, labelMode }
  }

  const inferred = inferLegacyLabelMetadata(node.type as NodeType, label)
  return {
    label,
    autoLabel: autoLabel ?? inferred.autoLabel,
    labelMode: labelMode ?? inferred.labelMode,
  }
}

function nodeToDraft(node: Node): NodeEditorDraft {
  const { label, autoLabel, labelMode } = getNodeLabelMetadata(node)

  return {
    id: node.id,
    type: node.type as NodeType,
    position: cloneValue(node.position),
    label,
    description: ((node.data as Record<string, unknown>).description as string | undefined) ?? '',
    autoLabel,
    labelMode,
    tableName: getTableName(node),
    config: cloneValue(getNodeConfig(node)),
  }
}

function normalizeHydratedNode(nodeDef: PipelineDefinition['nodes'][number]): Node {
  const label = nodeDef.label
  const inferred = inferLegacyLabelMetadata(nodeDef.type, label)

  return {
    id: nodeDef.id,
    type: nodeDef.type,
    position: nodeDef.position,
    data: {
      label,
      description: nodeDef.description ?? '',
      autoLabel: nodeDef.auto_label ?? inferred.autoLabel,
      labelMode: nodeDef.label_mode ?? inferred.labelMode,
      tableName: nodeDef.table_name ?? '',
      config: nodeDef.config,
    },
  }
}

function getFallbackActivePreviewTarget(previewTabOrder: string[]): ActivePreviewTarget | null {
  const nodeId = previewTabOrder[previewTabOrder.length - 1]
  return nodeId ? { kind: 'tab', nodeId } : null
}

function getEmptyTransientPreview(): TransientPreviewState {
  return {
    nodeId: null,
    data: null,
    loading: false,
    error: null,
  }
}

function serializeNode(node: Node): PipelineDefinition['nodes'][number] {
  const { autoLabel, labelMode } = getNodeLabelMetadata(node)
  return {
    id: node.id,
    type: node.type as NodeType,
    // Hubs produce no table; omit rather than send an empty string.
    table_name: node.type === 'excel_workbook'
      ? undefined
      : (node.data as Record<string, unknown>).tableName as string,
    label: (node.data as Record<string, unknown>).label as string,
    description: ((node.data as Record<string, unknown>).description as string | undefined) || undefined,
    auto_label: autoLabel,
    label_mode: labelMode,
    position: node.position,
    config: (node.data as Record<string, unknown>).config as Record<string, unknown>,
  }
}

function buildPipelineDefinitionFromState(state: Pick<PipelineState, 'nodes' | 'edges' | 'pipelineId' | 'pipelineName' | 'databaseConnections' | 'projectSettings'>): PipelineDefinition {
  return {
    id: state.pipelineId,
    name: state.pipelineName,
    database_connections: state.databaseConnections,
    nodes: state.nodes.map(serializeNode),
    edges: state.edges.map((e) => ({ id: e.id, source: e.source, target: e.target })),
    settings: state.projectSettings,
  }
}

function hydratePipelineState(pipeline: PipelineDefinition) {
  const nodes: Node[] = pipeline.nodes.map(normalizeHydratedNode)
  const edges: Edge[] = pipeline.edges.map((e) => ({
    id: e.id,
    source: e.source,
    target: e.target,
  }))
  return {
    nodes,
    edges,
    pipelineId: pipeline.id,
    pipelineName: pipeline.name,
    databaseConnections: pipeline.database_connections || [],
    projectSettings: pipeline.settings ?? { ...DEFAULT_PROJECT_SETTINGS },
    nodeResults: {},
    activeExecutions: {},
    activeExecutionIdByNodeId: {},
    activePipelineExecutionId: null,
    executionClockNow: Date.now(),
    errorDialogNodeId: null,
    selectedNodeId: null,
    previewTabsByNodeId: {},
    previewTabOrder: [],
    activePreviewTarget: null,
    transientPreview: getEmptyTransientPreview(),
    csvPreprocessArtifacts: {},
    cacheStatusByNodeId: {},
    livePreviewsByNodeId: {},
    loadDestinationPrompt: null,
    nodeEditorMode: 'closed' as const,
    nodeEditorDraft: null,
    editingNodeId: null,
    savedPipelineSnapshot: snapshotPipelineDefinition(pipeline),
    hasUnsavedChanges: false,
  }
}

function getTableName(node: Node): string {
  return (node.data as Record<string, unknown>).tableName as string
}

function getNodeConfig(node: Node): Record<string, unknown> {
  return (node.data as Record<string, unknown>).config as Record<string, unknown>
}

function getRequestErrorMessage(error: unknown, fallback: string): string {
  if (typeof error === 'object' && error !== null) {
    const response = 'response' in error ? error.response : undefined
    if (typeof response === 'object' && response !== null) {
      const data = 'data' in response ? response.data : undefined
      if (typeof data === 'object' && data !== null && 'detail' in data && typeof data.detail === 'string') {
        return data.detail
      }
    }
  }

  return error instanceof Error ? error.message : fallback
}

// Defensive fallback for the transform live-preview gate: the client checks
// cache status before starting a session, but if that check was stale, the
// backend still rejects with 409 { detail: { missing_tables } }.
function getMissingUpstreamTables(error: unknown): string[] | null {
  if (typeof error !== 'object' || error === null || !('response' in error)) return null
  const response = (error as { response?: unknown }).response
  if (typeof response !== 'object' || response === null) return null
  const { status, data } = response as { status?: number; data?: unknown }
  if (status !== 409 || typeof data !== 'object' || data === null) return null
  const detail = (data as { detail?: unknown }).detail
  if (typeof detail !== 'object' || detail === null) return null
  const missingTables = (detail as { missing_tables?: unknown }).missing_tables
  return Array.isArray(missingTables) ? missingTables.filter((t): t is string => typeof t === 'string') : null
}

// Cache-status refreshes are cheap but fire from several actions in a row
// (edit, connect, delete) — coalesce them.
let cacheStatusRefreshTimeout: ReturnType<typeof setTimeout> | null = null
function scheduleCacheStatusRefresh(get: () => PipelineState) {
  if (cacheStatusRefreshTimeout != null) clearTimeout(cacheStatusRefreshTimeout)
  cacheStatusRefreshTimeout = setTimeout(() => {
    cacheStatusRefreshTimeout = null
    void get().refreshCacheStatus()
  }, 300)
}

function invalidateCsvPreprocessArtifact(nodeId: string | undefined) {
  if (!nodeId) return
  void api.deletePreprocessedCsvArtifact(nodeId).catch(() => {})
}

function getExcelLoadFingerprint(config: ExcelSourceConfig): string {
  return JSON.stringify({
    file_path: config.file_path ?? '',
    selected_sheet: config.selected_sheet ?? '',
    cell_range: config.cell_range ?? '',
    header: config.header ?? true,
    all_varchar: config.all_varchar ?? false,
  })
}

export function buildDatabaseSourceDraftFromConnection(
  connection: SavedDatabaseConnection,
  position: { x: number; y: number },
): NodeEditorDraft {
  const config: DatabaseSourceConfig = connection.db_type === 'oracle'
    ? {
        connection_mode: 'local',
        db_type: 'oracle',
        connection: {
          host: connection.host,
          port: connection.port,
          service_name: connection.service_name,
          user: connection.user,
          password: connection.password,
        },
        query: '',
        fetch_config: defaultOracleFetchConfig(),
      }
    : {
        connection_mode: 'local',
        db_type: 'postgres',
        connection: {
          host: connection.host,
          port: connection.port,
          database: connection.database,
          user: connection.user,
          password: connection.password,
        },
        query: '',
      }

  return buildNodeDraft('db_source', position, {
    label: connection.name,
    autoLabel: connection.name,
    labelMode: 'auto',
    config: config as unknown as Record<string, unknown>,
  })
}

export function buildDatabaseSourceDraftFromGlobalConnection(
  connection: SavedDatabaseConnection,
  position: { x: number; y: number },
): NodeEditorDraft {
  return buildNodeDraft('db_source', position, {
    label: connection.name,
    autoLabel: connection.name,
    labelMode: 'auto',
    config: makeGlobalDatabaseSourceConfig(connection) as unknown as Record<string, unknown>,
  })
}

function hasCsvLoadInputsChanged(
  node: Node,
  nextConfig?: Record<string, unknown>,
): boolean {
  if ((node.type !== 'csv_source' && node.type !== 'excel_source') || !nextConfig) return false
  const currentConfig = getNodeConfig(node)
  const mergedConfig = { ...currentConfig, ...nextConfig }

  if (node.type === 'excel_source') {
    const currentExcel = currentConfig as unknown as ExcelSourceConfig
    const mergedExcel = mergedConfig as unknown as ExcelSourceConfig
    return getExcelLoadFingerprint(currentExcel) !== getExcelLoadFingerprint(mergedExcel)
  }

  const currentCsvConfig = currentConfig as unknown as CsvSourceConfig
  const mergedCsvConfig = mergedConfig as unknown as CsvSourceConfig

  return currentCsvConfig.file_path !== mergedCsvConfig.file_path
    || getCsvPreprocessFingerprint(currentCsvConfig) !== getCsvPreprocessFingerprint(mergedCsvConfig)
}

function hasDbSourceInputsChanged(
  node: Node,
  nextConfig?: Record<string, unknown>,
): boolean {
  if (node.type !== 'db_source' || !nextConfig) return false

  // load_mode picks where the result lands (RAM vs disk), not what the query
  // returns, so changing it must not invalidate the node's cache/result.
  const stripLoadMode = ({ load_mode: _ignored, ...rest }: Record<string, unknown>) => rest
  const currentConfig = stripLoadMode(getNodeConfig(node))
  const mergedConfig = stripLoadMode({ ...getNodeConfig(node), ...nextConfig })

  return JSON.stringify(currentConfig) !== JSON.stringify(mergedConfig)
}

function hasTransformInputsChanged(
  node: Node,
  nextConfig?: Record<string, unknown>,
): boolean {
  if (node.type !== 'transform' || !nextConfig) return false

  const currentSql = getNodeConfig(node).sql
  const nextSql = ({ ...getNodeConfig(node), ...nextConfig }).sql

  return currentSql !== nextSql
}

function hasExecutionInputsChanged(
  node: Node,
  nextConfig?: Record<string, unknown>,
): boolean {
  return hasCsvLoadInputsChanged(node, nextConfig)
    || hasDbSourceInputsChanged(node, nextConfig)
    || hasTransformInputsChanged(node, nextConfig)
}

function collectAncestorNodeIds(nodeId: string, edges: Edge[], nodes: Node[]): string[] {
  const parentsByTarget = new Map<string, string[]>()
  // Structural workbook→sheet edges are not data dependencies: a hub has no
  // data and must never be walked into (or flagged as needing a destination).
  dataEdges(edges, nodes).forEach((edge) => {
    const parents = parentsByTarget.get(edge.target) ?? []
    parents.push(edge.source)
    parentsByTarget.set(edge.target, parents)
  })

  const visited = new Set<string>()
  const stack = [...(parentsByTarget.get(nodeId) ?? [])]

  while (stack.length > 0) {
    const current = stack.pop()
    if (!current || visited.has(current)) continue
    visited.add(current)
    stack.push(...(parentsByTarget.get(current) ?? []))
  }

  return [...visited]
}

// A node "needs a destination" only when it has no data in EITHER DuckDB
// location — matching the backend's consumable_location gate (stale-but-present
// copies are read as-is, no prompt; see node-state-model.md §6). Walks the full
// ancestor chain (not just direct upstreams) because a run of `nodeId` may need
// to build several missing links in the chain in one go.
function nodesNeedingDestination(
  nodeId: string,
  nodes: Node[],
  edges: Edge[],
  cacheStatusByNodeId: Record<string, NodeCacheStatus>,
): LoadDestinationCandidate[] {
  const ancestorIds = collectAncestorNodeIds(nodeId, edges, nodes)
  const nodeMap = new Map(nodes.map((candidate) => [candidate.id, candidate]))
  return ancestorIds
    .map((ancestorId) => nodeMap.get(ancestorId))
    .filter((candidate): candidate is Node => Boolean(candidate))
    .filter((candidate) => {
      const locations = cacheStatusByNodeId[candidate.id]?.locations
      const hasAnyData = Boolean(locations?.in_memory?.present || locations?.materialized?.present)
      return !hasAnyData
    })
    .map((candidate) => ({
      nodeId: candidate.id,
      label: ((candidate.data as Record<string, unknown>).label as string) || getTableName(candidate),
      tableName: getTableName(candidate),
    }))
}

const EXECUTION_INITIAL_POLL_INTERVAL_MS = 100
const EXECUTION_POLL_INTERVAL_MS = 500
const EXECUTION_CLOCK_INTERVAL_MS = 1000
const EXECUTION_TRACKING_ERROR = 'Execution status unavailable. The backend may have restarted or the run expired.'

const executionPollTimeouts = new Map<string, ReturnType<typeof setTimeout>>()
const executionPollAbortControllers = new Map<string, AbortController>()
const executionPreviewTargets = new Map<string, { nodeId: string; tableName: string }>()
const executionTrackedNodeIds = new Map<string, string[]>()
const abortedExecutionIds = new Set<string>()
// After a "load missing ancestors, then start a (view-only) live preview" run
// finishes, resume by starting the live preview for this node — mirrors
// executionPreviewTargets but for the live-preview flow instead of a table tab.
const executionLivePreviewResumeIds = new Map<string, string>()
let executionClockInterval: ReturnType<typeof setInterval> | null = null

function isNotFoundError(error: unknown): boolean {
  if (typeof error !== 'object' || error === null) return false
  const response = 'response' in error ? error.response : undefined
  return typeof response === 'object' && response !== null && 'status' in response && response.status === 404
}

function isRequestAbortedError(error: unknown): boolean {
  if (typeof error !== 'object' || error === null) return false
  const code = 'code' in error ? error.code : undefined
  const name = 'name' in error ? error.name : undefined
  const message = 'message' in error ? error.message : undefined
  return code === 'ERR_CANCELED' || name === 'CanceledError' || message === 'canceled'
}

function clearExecutionPoll(executionId: string) {
  const timeoutId = executionPollTimeouts.get(executionId)
  if (timeoutId != null) {
    clearTimeout(timeoutId)
    executionPollTimeouts.delete(executionId)
  }
  const controller = executionPollAbortControllers.get(executionId)
  if (controller != null) {
    controller.abort()
    executionPollAbortControllers.delete(executionId)
  }
}

function clearAllExecutionTracking() {
  executionPollTimeouts.forEach((timeoutId) => clearTimeout(timeoutId))
  executionPollTimeouts.clear()
  executionPollAbortControllers.forEach((controller) => controller.abort())
  executionPollAbortControllers.clear()
  executionPreviewTargets.clear()
  executionTrackedNodeIds.clear()
  abortedExecutionIds.clear()
  if (executionClockInterval != null) {
    clearInterval(executionClockInterval)
    executionClockInterval = null
  }
}

function syncExecutionClock(
  set: StoreSet,
  get: () => PipelineState,
) {
  const hasRunningExecutions = Object.values(get().activeExecutions).some((run) => run.status === 'running')
  if (hasRunningExecutions) {
    if (executionClockInterval == null) {
      executionClockInterval = setInterval(() => {
        usePipelineStore.setState({ executionClockNow: Date.now() })
      }, EXECUTION_CLOCK_INTERVAL_MS)
    }
    set({ executionClockNow: Date.now() })
    return
  }

  if (executionClockInterval != null) {
    clearInterval(executionClockInterval)
    executionClockInterval = null
  }
  set({ executionClockNow: Date.now() })
}

function applyExecutionRunSnapshot(
  run: ExecutionRunStatus,
  set: StoreSet,
  get: () => PipelineState,
) {
  if (abortedExecutionIds.has(run.execution_id) && run.status !== 'cancelled') {
    return
  }

  set((state) => {
    const nodeResults = { ...state.nodeResults }
    const activeExecutions = { ...state.activeExecutions }
    const activeExecutionIdByNodeId = { ...state.activeExecutionIdByNodeId }

    Object.entries(run.node_results).forEach(([nodeId, result]) => {
      nodeResults[nodeId] = result
      if (result.status === 'connecting' || result.status === 'running') {
        activeExecutionIdByNodeId[nodeId] = run.execution_id
      } else if (activeExecutionIdByNodeId[nodeId] === run.execution_id) {
        delete activeExecutionIdByNodeId[nodeId]
      }
    })

    if (run.status === 'running') {
      activeExecutions[run.execution_id] = run
    } else {
      delete activeExecutions[run.execution_id]
      for (const nodeId of executionTrackedNodeIds.get(run.execution_id) ?? []) {
        if (activeExecutionIdByNodeId[nodeId] === run.execution_id) {
          delete activeExecutionIdByNodeId[nodeId]
        }
      }
    }

    return {
      nodeResults,
      activeExecutions,
      activeExecutionIdByNodeId,
      activePipelineExecutionId: state.activePipelineExecutionId === run.execution_id && run.status !== 'running'
        ? null
        : state.activePipelineExecutionId,
      errorDialogNodeId: state.errorDialogNodeId && run.node_results[state.errorDialogNodeId]?.status !== 'error'
        ? null
        : state.errorDialogNodeId,
    }
  })
  syncExecutionClock(set, get)
}

function failExecutionTracking(
  executionId: string,
  message: string,
  set: StoreSet,
  get: () => PipelineState,
) {
  clearExecutionPoll(executionId)
  executionPreviewTargets.delete(executionId)

  set((state) => {
    const nodeResults = { ...state.nodeResults }
    const activeExecutions = { ...state.activeExecutions }
    const activeExecutionIdByNodeId = { ...state.activeExecutionIdByNodeId }
    const trackedNodeIds = executionTrackedNodeIds.get(executionId)
      ?? Object.keys(activeExecutions[executionId]?.node_results ?? {})

    trackedNodeIds.forEach((nodeId) => {
      const existing = nodeResults[nodeId]
      nodeResults[nodeId] = {
        node_id: nodeId,
        status: 'error',
        error: message,
        started_at: existing?.started_at,
        finished_at: new Date().toISOString(),
      }
      if (activeExecutionIdByNodeId[nodeId] === executionId) {
        delete activeExecutionIdByNodeId[nodeId]
      }
    })

    delete activeExecutions[executionId]

    return {
      nodeResults,
      activeExecutions,
      activeExecutionIdByNodeId,
      activePipelineExecutionId: state.activePipelineExecutionId === executionId ? null : state.activePipelineExecutionId,
      errorDialogNodeId: null,
    }
  })

  executionTrackedNodeIds.delete(executionId)
  syncExecutionClock(set, get)
}

async function finalizeExecutionRun(
  run: ExecutionRunStatus,
  set: StoreSet,
  get: () => PipelineState,
) {
  if (run.status === 'cancelled') {
    abortedExecutionIds.add(run.execution_id)
  } else if (abortedExecutionIds.has(run.execution_id)) {
    return
  }

  clearExecutionPoll(run.execution_id)
  applyExecutionRunSnapshot(run, set, get)

  const trackedNodeIds = executionTrackedNodeIds.get(run.execution_id) ?? []
  const previewTarget = executionPreviewTargets.get(run.execution_id)
  const livePreviewResumeNodeId = executionLivePreviewResumeIds.get(run.execution_id)
  executionPreviewTargets.delete(run.execution_id)
  executionTrackedNodeIds.delete(run.execution_id)
  executionLivePreviewResumeIds.delete(run.execution_id)

  if (previewTarget && run.node_results[previewTarget.nodeId]?.status === 'success') {
    await get().loadTablePreview(previewTarget.nodeId, previewTarget.tableName, 0, { forceReload: true })
  }

  // Resume a live preview that was waiting on missing upstreams, but only once
  // every one of them actually finished (a partial failure leaves the chain
  // still incomplete, so starting the preview would just hit the gate again).
  if (livePreviewResumeNodeId && trackedNodeIds.length > 0 && trackedNodeIds.every((id) => run.node_results[id]?.status === 'success')) {
    await get().startLivePreview(livePreviewResumeNodeId)
  }

  // A finished run changes which nodes are fresh/stale (descendants too).
  void get().refreshCacheStatus()
}

function scheduleExecutionPoll(
  executionId: string,
  set: StoreSet,
  get: () => PipelineState,
  delayMs = EXECUTION_INITIAL_POLL_INTERVAL_MS,
) {
  clearExecutionPoll(executionId)
  const timeoutId = setTimeout(async () => {
    const controller = new AbortController()
    executionPollAbortControllers.set(executionId, controller)
    try {
      const run = await api.getExecutionRunStatus(executionId, controller.signal)
      executionPollAbortControllers.delete(executionId)
      if (run.status === 'running') {
        applyExecutionRunSnapshot(run, set, get)
        // Keep visible status latency low; execution_time_ms still comes from the backend node runtime.
        scheduleExecutionPoll(executionId, set, get, EXECUTION_POLL_INTERVAL_MS)
        return
      }

      await finalizeExecutionRun(run, set, get)
    } catch (error) {
      executionPollAbortControllers.delete(executionId)
      if (isRequestAbortedError(error)) {
        return
      }
      if (isNotFoundError(error)) {
        failExecutionTracking(executionId, EXECUTION_TRACKING_ERROR, set, get)
        return
      }
      scheduleExecutionPoll(executionId, set, get)
    }
  }, delayMs)

  executionPollTimeouts.set(executionId, timeoutId)
}

export const usePipelineStore = create<PipelineState>((set, get) => ({
  nodes: [],
  edges: [],
  pipelineId: initialPipeline.id,
  pipelineName: initialPipeline.name,
  databaseConnections: [],
  projectSettings: initialPipeline.settings ?? { ...DEFAULT_PROJECT_SETTINGS },
  savedPipelineSnapshot: snapshotPipelineDefinition(initialPipeline),
  hasUnsavedChanges: false,
  projectListRevision: 0,
  nodeResults: {},
  activeExecutions: {},
  activeExecutionIdByNodeId: {},
  activePipelineExecutionId: null,
  executionClockNow: Date.now(),
  errorDialogNodeId: null,
  selectedNodeId: null,
  previewTabsByNodeId: {},
  previewTabOrder: [],
  activePreviewTarget: null,
  transientPreview: getEmptyTransientPreview(),
  csvPreprocessArtifacts: {},
  cacheStatusByNodeId: {},
  livePreviewsByNodeId: {},
  loadDestinationPrompt: null,
  nodeEditorMode: 'closed',
  nodeEditorDraft: null,
  editingNodeId: null,

  setPipelineName: (name) => {
    set({ pipelineName: name })
    const state = get()
    set({
      hasUnsavedChanges: snapshotPipelineDefinition(buildPipelineDefinitionFromState(state)) !== state.savedPipelineSnapshot,
    })
  },

  updateProjectSettings: (patch) => {
    set((state) => ({ projectSettings: { ...state.projectSettings, ...patch } }))
    const state = get()
    set({
      hasUnsavedChanges: snapshotPipelineDefinition(buildPipelineDefinitionFromState(state)) !== state.savedPipelineSnapshot,
    })
  },

  refreshCacheStatus: async () => {
    const state = get()
    if (state.nodes.length === 0) {
      set({ cacheStatusByNodeId: {} })
      return
    }
    try {
      const response = await api.getCacheStatus(buildPipelineDefinitionFromState(state))
      set({ cacheStatusByNodeId: response.nodes })
    } catch {
      // Keep the last known statuses; a transient failure shouldn't flicker badges.
    }
  },

  onNodesChange: (changes) => {
    set({ nodes: applyNodeChanges(changes, get().nodes) })
    const state = get()
    set({
      hasUnsavedChanges: snapshotPipelineDefinition(buildPipelineDefinitionFromState(state)) !== state.savedPipelineSnapshot,
    })
  },

  onEdgesChange: (changes) => {
    // Structural workbook→sheet edges live and die with their sheet node —
    // they are never removable on their own (docs/excel-node-model.md §5).
    const nodesById = buildNodesById(get().nodes)
    const edgesById = new Map(get().edges.map((edge) => [edge.id, edge]))
    const allowed = changes.filter((change) => {
      if (change.type !== 'remove') return true
      const edge = edgesById.get(change.id)
      return !edge || !isStructuralEdge(edge, nodesById)
    })
    if (allowed.length === 0) return
    set({ edges: applyEdgeChanges(allowed, get().edges) })
    const state = get()
    set({
      hasUnsavedChanges: snapshotPipelineDefinition(buildPipelineDefinitionFromState(state)) !== state.savedPipelineSnapshot,
    })
  },

  onConnect: (connection) => {
    if (!connection.source || !connection.target || connection.source === connection.target) {
      return
    }
    // Hubs never take part in user-drawn connections: structural edges are
    // created only by the sheet picker, and a hub has no data to consume.
    const isHub = (id: string | null) =>
      get().nodes.find((node) => node.id === id)?.type === 'excel_workbook'
    if (isHub(connection.source) || isHub(connection.target)) {
      return
    }
    set({ edges: addEdge({ ...connection, id: `edge_${Date.now()}` }, get().edges) })
    const state = get()
    set({
      hasUnsavedChanges: snapshotPipelineDefinition(buildPipelineDefinitionFromState(state)) !== state.savedPipelineSnapshot,
    })
    scheduleCacheStatusRefresh(get)
  },

  setSelectedNodeId: (id) => set({ selectedNodeId: id }),
  openNodeError: (nodeId) => set({ errorDialogNodeId: nodeId }),
  closeNodeError: () => set({ errorDialogNodeId: null }),
  selectPreviewTab: (nodeId) => {
    if (!get().previewTabsByNodeId[nodeId]) return
    set({ activePreviewTarget: { kind: 'tab', nodeId } })
  },

  openCreateNodeEditor: (draft) => {
    set({
      nodeEditorMode: 'create',
      nodeEditorDraft: cloneValue(draft),
      editingNodeId: null,
      selectedNodeId: null,
    })
  },

  openEditNodeEditor: (nodeId) => {
    const node = get().nodes.find((candidate) => candidate.id === nodeId)
    if (!node) return

    set({
      nodeEditorMode: 'edit',
      nodeEditorDraft: nodeToDraft(node),
      editingNodeId: nodeId,
      selectedNodeId: nodeId,
    })
  },

  updateNodeEditorDraft: (patch) => {
    set((state) => {
      if (!state.nodeEditorDraft) return state

      const nextDraft = {
        ...state.nodeEditorDraft,
        ...patch,
      }
      const label = typeof patch.label === 'string' ? patch.label : nextDraft.label
      const autoLabel = typeof patch.autoLabel === 'string' ? patch.autoLabel : nextDraft.autoLabel
      const labelMode = patch.labelMode === 'auto' || patch.labelMode === 'custom'
        ? patch.labelMode
        : deriveLabelMode(label, autoLabel)

      return {
        nodeEditorDraft: {
          ...nextDraft,
          label,
          autoLabel,
          labelMode,
          position: patch.position ? cloneValue(patch.position) : nextDraft.position,
          config: patch.config ? cloneValue(patch.config) : nextDraft.config,
        },
      }
    })
  },

  closeNodeEditor: () => {
    set({
      nodeEditorMode: 'closed',
      nodeEditorDraft: null,
      editingNodeId: null,
    })
  },

  commitNodeEditor: () => {
    const { nodeEditorMode, nodeEditorDraft, editingNodeId } = get()
    if (!nodeEditorDraft || nodeEditorMode === 'closed') return null

    if (nodeEditorMode === 'create') {
      const newNode = buildNodeFromDraft(nodeEditorDraft)
      set((state) => ({
        nodes: [...state.nodes, newNode],
        selectedNodeId: newNode.id,
        nodeEditorMode: 'closed',
        nodeEditorDraft: null,
        editingNodeId: null,
      }))
      const state = get()
      set({
        hasUnsavedChanges: snapshotPipelineDefinition(buildPipelineDefinitionFromState(state)) !== state.savedPipelineSnapshot,
      })
      return newNode.id
    }

    if (!editingNodeId) return null

    get().updateNodeData(editingNodeId, {
      label: nodeEditorDraft.label,
      autoLabel: nodeEditorDraft.autoLabel,
      labelMode: nodeEditorDraft.labelMode,
      tableName: nodeEditorDraft.tableName,
      config: cloneValue(nodeEditorDraft.config),
    })
    get().closeNodeEditor()
    set({ selectedNodeId: editingNodeId })
    return editingNodeId
  },

  addNode: (type, position) => {
    const newNode = buildNodeFromDraft(buildNodeDraft(type, position))
    set({ nodes: [...get().nodes, newNode], selectedNodeId: newNode.id })
    const state = get()
    set({
      hasUnsavedChanges: snapshotPipelineDefinition(buildPipelineDefinitionFromState(state)) !== state.savedPipelineSnapshot,
    })
  },

  addDatabaseConnection: (connection) => {
    const id = crypto.randomUUID()
    set({ databaseConnections: [...get().databaseConnections, { ...connection, id } as SavedDatabaseConnection] })
    const state = get()
    set({
      hasUnsavedChanges: snapshotPipelineDefinition(buildPipelineDefinitionFromState(state)) !== state.savedPipelineSnapshot,
    })
    return id
  },

  updateDatabaseConnection: (id, connection) => {
    set({
      databaseConnections: get().databaseConnections.map((item) =>
        item.id === id ? ({ ...connection, id } as SavedDatabaseConnection) : item
      ),
    })
    const state = get()
    set({
      hasUnsavedChanges: snapshotPipelineDefinition(buildPipelineDefinitionFromState(state)) !== state.savedPipelineSnapshot,
    })
  },

  deleteDatabaseConnection: (id) => {
    set({
      databaseConnections: get().databaseConnections.filter((item) => item.id !== id),
    })
    const state = get()
    set({
      hasUnsavedChanges: snapshotPipelineDefinition(buildPipelineDefinitionFromState(state)) !== state.savedPipelineSnapshot,
    })
  },

  addDatabaseSourceFromConnection: (connectionId, position) => {
    const savedConnection = get().databaseConnections.find((item) => item.id === connectionId)
    if (!savedConnection) return null

    const draft = buildDatabaseSourceDraftFromConnection(savedConnection, position)
    const newNode = buildNodeFromDraft(draft)
    set({ nodes: [...get().nodes, newNode], selectedNodeId: newNode.id })
    const state = get()
    set({
      hasUnsavedChanges: snapshotPipelineDefinition(buildPipelineDefinitionFromState(state)) !== state.savedPipelineSnapshot,
    })
    return newNode.id
  },

  updateNodeData: (nodeId, data) => {
    const currentNode = get().nodes.find((candidate) => candidate.id === nodeId)
    if (!currentNode) return

    const currentLabelMetadata = getNodeLabelMetadata(currentNode)
    const previousTableName = getTableName(currentNode)
    const nextTableName = typeof data.tableName === 'string' ? data.tableName : previousTableName
    const tableNameChanged = previousTableName !== nextTableName
    const csvLoadInputsChanged = hasCsvLoadInputsChanged(currentNode, data.config as Record<string, unknown> | undefined)
    const shouldInvalidateExecution = tableNameChanged || hasExecutionInputsChanged(currentNode, data.config as Record<string, unknown> | undefined)

    const nextLabel = typeof data.label === 'string'
      ? data.label
      : currentLabelMetadata.label
    const nextAutoLabel = typeof data.autoLabel === 'string'
      ? data.autoLabel
      : currentLabelMetadata.autoLabel
    const nextLabelMode = data.labelMode === 'auto' || data.labelMode === 'custom'
      ? data.labelMode
      : currentLabelMetadata.labelMode

    // Config changes no longer drop the persisted table: the cache-key
    // system marks it stale instead, and the data stays queryable until the
    // node reruns. Renames are reconciled server-side on save.
    if (csvLoadInputsChanged) {
      invalidateCsvPreprocessArtifact(nodeId)
    }

    set((state) => {
      const nodeResults = { ...state.nodeResults }
      const csvPreprocessArtifacts = { ...state.csvPreprocessArtifacts }
      const previewTabsByNodeId = { ...state.previewTabsByNodeId }
      const transientPreview = state.transientPreview.nodeId === nodeId && (shouldInvalidateExecution || csvLoadInputsChanged)
        ? getEmptyTransientPreview()
        : state.transientPreview

      if (shouldInvalidateExecution) {
        delete nodeResults[nodeId]
      }
      if (csvLoadInputsChanged) {
        delete csvPreprocessArtifacts[nodeId]
      }
      if (previewTabsByNodeId[nodeId] && shouldInvalidateExecution) {
        previewTabsByNodeId[nodeId] = {
          ...previewTabsByNodeId[nodeId],
          loading: false,
          error: null,
          isStale: true,
        }
      }

      return {
        nodes: state.nodes.map((n) =>
          n.id === nodeId
            ? {
                ...n,
                data: {
                  ...n.data,
                  ...data,
                  label: nextLabel,
                  autoLabel: nextAutoLabel,
                  labelMode: nextLabelMode,
                },
              }
            : n
        ),
        nodeResults,
        previewTabsByNodeId,
        transientPreview,
        csvPreprocessArtifacts,
      }
    })
    const state = get()
    set({
      hasUnsavedChanges: snapshotPipelineDefinition(buildPipelineDefinitionFromState(state)) !== state.savedPipelineSnapshot,
    })
    // A result-affecting edit changes this node's cache key (and every
    // descendant's), so the data-state dots need a fresh fetch to show stale.
    if (shouldInvalidateExecution) {
      scheduleCacheStatusRefresh(get)
    }
  },

  deleteNode: (nodeId) => {
    const node = get().nodes.find((candidate) => candidate.id === nodeId)
    if (node) {
      // The persisted table drops when the deletion is saved (server-side
      // reconcile), so discarding unsaved changes keeps the data.
      if (node.type === 'csv_source' || node.type === 'excel_source') {
        invalidateCsvPreprocessArtifact(nodeId)
      }
    }
    const liveSession = get().livePreviewsByNodeId[nodeId]
    if (liveSession?.sessionId) {
      void api.closePreviewSession(liveSession.sessionId).catch(() => {})
    }

    set((state) => {
      const nodeResults = { ...state.nodeResults }
      const csvPreprocessArtifacts = { ...state.csvPreprocessArtifacts }
      const previewTabsByNodeId = { ...state.previewTabsByNodeId }
      const livePreviewsByNodeId = { ...state.livePreviewsByNodeId }
      const cacheStatusByNodeId = { ...state.cacheStatusByNodeId }
      const previewTabOrder = state.previewTabOrder.filter((id) => id !== nodeId)
      const activePreviewTarget = state.activePreviewTarget?.nodeId === nodeId
        ? getFallbackActivePreviewTarget(previewTabOrder)
        : state.activePreviewTarget
      const transientPreview = state.transientPreview.nodeId === nodeId
        ? getEmptyTransientPreview()
        : state.transientPreview
      delete nodeResults[nodeId]
      delete csvPreprocessArtifacts[nodeId]
      delete previewTabsByNodeId[nodeId]
      delete livePreviewsByNodeId[nodeId]
      delete cacheStatusByNodeId[nodeId]

      return {
        nodes: state.nodes.filter((n) => n.id !== nodeId),
        edges: state.edges.filter((e) => e.source !== nodeId && e.target !== nodeId),
        nodeResults,
        previewTabsByNodeId,
        livePreviewsByNodeId,
        cacheStatusByNodeId,
        previewTabOrder,
        activePreviewTarget,
        transientPreview,
        csvPreprocessArtifacts,
        selectedNodeId: state.selectedNodeId === nodeId ? null : state.selectedNodeId,
        errorDialogNodeId: state.errorDialogNodeId === nodeId ? null : state.errorDialogNodeId,
        nodeEditorMode: state.editingNodeId === nodeId ? 'closed' : state.nodeEditorMode,
        nodeEditorDraft: state.editingNodeId === nodeId ? null : state.nodeEditorDraft,
        editingNodeId: state.editingNodeId === nodeId ? null : state.editingNodeId,
      }
    })
    const state = get()
    set({
      hasUnsavedChanges: snapshotPipelineDefinition(buildPipelineDefinitionFromState(state)) !== state.savedPipelineSnapshot,
    })
  },

  executePipeline: async (force = false) => {
    const { nodes } = get()
    const pipeline = buildPipelineDefinitionFromState(get())

    try {
      const run = await api.startPipelineExecution(pipeline, force)
      // Hubs never execute and never get a result — don't track or error them.
      const executableNodes = nodes.filter((node) => node.type !== 'excel_workbook')
      executionTrackedNodeIds.set(run.execution_id, executableNodes.map((node) => node.id))
      set({ activePipelineExecutionId: run.execution_id, errorDialogNodeId: null })
      applyExecutionRunSnapshot(run, set, get)
      if (run.status === 'running') {
        scheduleExecutionPoll(run.execution_id, set, get)
      } else {
        await finalizeExecutionRun(run, set, get)
      }
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : 'Unknown error'
      const errorResults: Record<string, NodeExecutionResult> = {}
      nodes.filter((n) => n.type !== 'excel_workbook').forEach((n) => {
        errorResults[n.id] = { node_id: n.id, status: 'error', error: message }
      })
      set({
        nodeResults: errorResults,
        activePipelineExecutionId: null,
        errorDialogNodeId: null,
      })
    }
  },

  executeSingleNode: async (nodeId, options) => {
    const node = get().nodes.find((candidate) => candidate.id === nodeId)
    if (!node) return

    const tableName = (node.data as Record<string, unknown>).tableName as string
    const pipeline = buildPipelineDefinitionFromState(get())

    try {
      const run = await api.startNodeExecution(pipeline, nodeId, options?.force ?? false)
      executionTrackedNodeIds.set(run.execution_id, [nodeId])
      if (options?.loadPreviewOnSuccess) {
        executionPreviewTargets.set(run.execution_id, { nodeId, tableName })
      }
      set({
        errorDialogNodeId: get().errorDialogNodeId === nodeId ? null : get().errorDialogNodeId,
      })
      applyExecutionRunSnapshot(run, set, get)
      if (run.status === 'running') {
        scheduleExecutionPoll(run.execution_id, set, get)
      } else {
        await finalizeExecutionRun(run, set, get)
      }
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : 'Unknown error'
      set({
        nodeResults: {
          ...get().nodeResults,
          [nodeId]: { node_id: nodeId, status: 'error', error: message },
        },
        errorDialogNodeId: null,
      })
    }
  },

  runNodeWithLoadMode: async (nodeId, loadMode, options) => {
    // Persist the chosen location, then run; the engine reads load_mode from config.
    const node = get().nodes.find((candidate) => candidate.id === nodeId)
    if (node) {
      const config = { ...((node.data as Record<string, unknown>).config as Record<string, unknown>), load_mode: loadMode }
      get().updateNodeData(nodeId, { config })
    }
    const live = get().livePreviewsByNodeId[nodeId]
    if (live?.sessionId && !live.materializing) {
      if (node?.type === 'db_source') {
        // The DB preview holds a remote cursor; draining it promotes the buffered
        // + remaining rows into the chosen catalog instead of re-querying the source.
        await get().materializeLivePreview(nodeId, loadMode === 'in_memory')
        return
      }
      // Any other (view-only) live preview, e.g. a transform's streaming preview:
      // promoting re-runs the node through the normal path instead of draining —
      // local re-execution is cheap, and the session has no drain support.
      await get().closeLivePreview(nodeId)
    }
    await get().executeSingleNode(nodeId, { loadPreviewOnSuccess: true, ...options })
  },

  abortDatabaseNodeExecution: async (nodeId) => {
    const executionId = get().activeExecutionIdByNodeId[nodeId]
    if (!executionId) return

    abortedExecutionIds.add(executionId)
    clearExecutionPoll(executionId)

    try {
      const run = await api.abortExecutionRun(executionId)
      await finalizeExecutionRun(run, set, get)
    } catch (err) {
      abortedExecutionIds.delete(executionId)
      const activeRun = get().activeExecutions[executionId]
      if (activeRun?.status === 'running') {
        scheduleExecutionPoll(executionId, set, get)
        return
      }

      const message = getRequestErrorMessage(err, 'Unable to abort execution')
      set({
        nodeResults: {
          ...get().nodeResults,
          [nodeId]: {
            ...(get().nodeResults[nodeId] ?? { node_id: nodeId }),
            status: 'error',
            error: message,
            finished_at: new Date().toISOString(),
          },
        },
      })
    }
  },

  runTransformPreview: async (nodeId) => {
    const node = get().nodes.find((candidate) => candidate.id === nodeId)
    if (!node || node.type !== 'transform') return

    const sql = (getNodeConfig(node).sql as string | undefined) ?? ''
    if (!sql.trim()) return

    try {
      // Cache status drives the gate below; make sure it reflects the current graph.
      await get().refreshCacheStatus()
      const candidates = nodesNeedingDestination(nodeId, get().nodes, get().edges, get().cacheStatusByNodeId)
      if (candidates.length > 0) {
        set({
          loadDestinationPrompt: {
            targetNodeId: nodeId,
            resumeKind: 'materialize',
            candidates,
            choices: Object.fromEntries(candidates.map((c) => [c.nodeId, 'in_memory' as NodeLoadMode])),
          },
        })
        return
      }

      await get().executeSingleNode(nodeId, { loadPreviewOnSuccess: true })
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : 'Unknown error'
      set({
        nodeResults: {
          ...get().nodeResults,
          [nodeId]: { node_id: nodeId, status: 'error', error: message },
        },
        errorDialogNodeId: null,
      })
    }
  },

  loadCsvPreview: async (nodeId, filePath) => {
    set({
      activePreviewTarget: { kind: 'transient', nodeId },
      transientPreview: {
        nodeId,
        data: null,
        loading: true,
        error: null,
      },
    })
    try {
      const data = await api.previewCsvSource(filePath) as CsvTextPreviewData
      set({
        activePreviewTarget: { kind: 'transient', nodeId },
        transientPreview: {
          nodeId,
          data,
          loading: false,
          error: null,
        },
      })
    } catch (err) {
      set({
        activePreviewTarget: { kind: 'transient', nodeId },
        transientPreview: {
          nodeId,
          data: null,
          loading: false,
          error: getRequestErrorMessage(err, 'Unable to preview CSV'),
        },
      })
    }
  },

  loadPreprocessedCsvPreview: async (nodeId, filePath, preprocessing) => {
    set({
      activePreviewTarget: { kind: 'transient', nodeId },
      transientPreview: {
        nodeId,
        data: null,
        loading: true,
        error: null,
      },
    })
    const fingerprint = getCsvPreprocessFingerprint({
      file_path: filePath,
      original_filename: '',
      preprocessing,
    })
    try {
      const data = await api.previewPreprocessedCsvSource(nodeId, filePath, preprocessing) as CsvTextPreviewData
      set((state) => ({
        activePreviewTarget: { kind: 'transient', nodeId },
        transientPreview: {
          nodeId,
          data,
          loading: false,
          error: null,
        },
        csvPreprocessArtifacts: fingerprint
          ? { ...state.csvPreprocessArtifacts, [nodeId]: fingerprint }
          : state.csvPreprocessArtifacts,
      }))
    } catch (err) {
      set((state) => {
        const csvPreprocessArtifacts = { ...state.csvPreprocessArtifacts }
        delete csvPreprocessArtifacts[nodeId]
        return {
          activePreviewTarget: { kind: 'transient', nodeId },
          transientPreview: {
            nodeId,
            data: null,
            loading: false,
            error: getRequestErrorMessage(err, 'Unable to preview CSV'),
          },
          csvPreprocessArtifacts,
        }
      })
    }
  },

  loadTablePreview: async (nodeId, tableName, offset = 0, options) => {
    const existingTab = get().previewTabsByNodeId[nodeId]
    const shouldReuseCachedPage = !options?.forceReload
      && offset === 0
      && Boolean(existingTab?.data)

    if (shouldReuseCachedPage && existingTab) {
      set({ activePreviewTarget: { kind: 'tab', nodeId } })
      return
    }

    set((state) => {
      const previewTabsByNodeId = { ...state.previewTabsByNodeId }
      const previewTabOrder = previewTabsByNodeId[nodeId]
        ? state.previewTabOrder
        : [...state.previewTabOrder, nodeId]
      previewTabsByNodeId[nodeId] = {
        nodeId,
        tableNameAtLoad: previewTabsByNodeId[nodeId]?.tableNameAtLoad ?? tableName,
        data: previewTabsByNodeId[nodeId]?.data ?? null,
        loading: true,
        error: null,
        isStale: previewTabsByNodeId[nodeId]?.isStale ?? false,
      }

      return {
        previewTabsByNodeId,
        previewTabOrder,
        activePreviewTarget: { kind: 'tab', nodeId },
      }
    })
    try {
      const data = await api.previewData(get().pipelineId, tableName, offset) as TablePreviewData
      set((state) => ({
        previewTabsByNodeId: {
          ...state.previewTabsByNodeId,
          [nodeId]: {
            nodeId,
            tableNameAtLoad: tableName,
            data,
            loading: false,
            error: null,
            isStale: false,
          },
        },
        previewTabOrder: state.previewTabOrder.includes(nodeId)
          ? state.previewTabOrder
          : [...state.previewTabOrder, nodeId],
        activePreviewTarget: { kind: 'tab', nodeId },
      }))
    } catch (err) {
      set((state) => ({
        previewTabsByNodeId: {
          ...state.previewTabsByNodeId,
          [nodeId]: {
            nodeId,
            tableNameAtLoad: state.previewTabsByNodeId[nodeId]?.tableNameAtLoad ?? tableName,
            data: state.previewTabsByNodeId[nodeId]?.data ?? null,
            loading: false,
            error: getRequestErrorMessage(err, 'Unable to preview data'),
            isStale: state.previewTabsByNodeId[nodeId]?.isStale ?? false,
          },
        },
        previewTabOrder: state.previewTabOrder.includes(nodeId)
          ? state.previewTabOrder
          : [...state.previewTabOrder, nodeId],
        activePreviewTarget: { kind: 'tab', nodeId },
      }))
    }
  },

  loadMoreTablePreview: async (nodeId) => {
    const tab = get().previewTabsByNodeId[nodeId]
    if (!tab || tab.loading || !tab.data) return
    const { rows, total_rows, limit, offset } = tab.data
    const nextOffset = offset + rows.length
    // All rows already loaded.
    if (nextOffset >= total_rows) return

    set((state) => ({
      previewTabsByNodeId: {
        ...state.previewTabsByNodeId,
        [nodeId]: { ...state.previewTabsByNodeId[nodeId], loading: true },
      },
    }))
    try {
      const page = await api.previewData(get().pipelineId, tab.tableNameAtLoad, nextOffset, limit) as TablePreviewData
      set((state) => {
        const current = state.previewTabsByNodeId[nodeId]
        if (!current?.data) return {}
        return {
          previewTabsByNodeId: {
            ...state.previewTabsByNodeId,
            [nodeId]: {
              ...current,
              loading: false,
              data: {
                ...page,
                // Keep the original window offset; rows accumulate for the grid.
                offset: current.data.offset,
                rows: [...current.data.rows, ...page.rows],
              },
            },
          },
        }
      })
    } catch (err) {
      set((state) => ({
        previewTabsByNodeId: {
          ...state.previewTabsByNodeId,
          [nodeId]: {
            ...state.previewTabsByNodeId[nodeId],
            loading: false,
            error: getRequestErrorMessage(err, 'Unable to load more rows'),
          },
        },
      }))
    }
  },

  savePipeline: async () => {
    const pipeline = buildPipelineDefinitionFromState(get())
    await api.savePipeline(pipeline)
    set((state) => ({
      savedPipelineSnapshot: snapshotPipelineDefinition(pipeline),
      hasUnsavedChanges: false,
      projectListRevision: state.projectListRevision + 1,
    }))
    // Save reconciles server-side storage (drops/renames), so fresh/stale
    // status can shift.
    void get().refreshCacheStatus()
  },

  loadPipeline: async (id) => {
    get().nodes
      .filter((node) => node.type === 'csv_source' || node.type === 'excel_source')
      .forEach((node) => invalidateCsvPreprocessArtifact(node.id))

    clearAllExecutionTracking()
    const pipeline = await api.loadPipeline(id)
    set(hydratePipelineState(pipeline))
    void get().refreshCacheStatus()
  },

  newPipeline: () => {
    get().nodes
      .filter((node) => node.type === 'csv_source' || node.type === 'excel_source')
      .forEach((node) => invalidateCsvPreprocessArtifact(node.id))

    clearAllExecutionTracking()
    nodeCounter = 0
    set(hydratePipelineState(createBlankPipelineDefinition()))
  },

  markProjectCatalogChanged: () => {
    set((state) => ({ projectListRevision: state.projectListRevision + 1 }))
  },

  confirmDiscardChanges: (nextProjectName) => {
    if (!get().hasUnsavedChanges) return true
    const suffix = nextProjectName ? ` and open "${nextProjectName}"` : ''
    return window.confirm(`You have unsaved changes. Discard them${suffix}?`)
  },

  startLivePreview: async (nodeId) => {
    const node = get().nodes.find((candidate) => candidate.id === nodeId)
    if (!node || (node.type !== 'db_source' && node.type !== 'transform')) return

    if (node.type === 'transform') {
      // Cache status drives the gate below; make sure it reflects the current graph.
      await get().refreshCacheStatus()
      const candidates = nodesNeedingDestination(nodeId, get().nodes, get().edges, get().cacheStatusByNodeId)
      if (candidates.length > 0) {
        set({
          loadDestinationPrompt: {
            targetNodeId: nodeId,
            resumeKind: 'live-preview',
            candidates,
            choices: Object.fromEntries(candidates.map((c) => [c.nodeId, 'in_memory' as NodeLoadMode])),
          },
        })
        return
      }
    }

    // Close any prior session for this node before starting a new one.
    const existing = get().livePreviewsByNodeId[nodeId]
    if (existing?.sessionId) {
      void api.closePreviewSession(existing.sessionId).catch(() => {})
    }

    // Snapshot the current result so we can restore it when the preview ends.
    // This is the F1 fix: mirror the live-preview loading state into nodeResults
    // so canvas node badges reflect "Running" while the preview is in flight.
    const prevNodeResult = get().nodeResults[nodeId] ?? null

    set((state) => ({
      activePreviewTarget: { kind: 'live', nodeId },
      nodeResults: {
        ...state.nodeResults,
        [nodeId]: { ...(prevNodeResult ?? { node_id: nodeId }), node_id: nodeId, status: 'running' },
      },
      livePreviewsByNodeId: {
        ...state.livePreviewsByNodeId,
        [nodeId]: {
          nodeId,
          sessionId: null,
          columns: [],
          columnTypes: [],
          rows: [],
          hasMore: false,
          bufferCapped: false,
          loading: true,
          materializing: false,
          error: null,
        },
      },
    }))

    try {
      const pipeline = buildPipelineDefinitionFromState(get())
      const result = await api.startPreviewSession(pipeline, nodeId)
      set((state) => ({
        // Restore the pre-preview result; the live preview doesn't change materialized state.
        nodeResults: prevNodeResult
          ? { ...state.nodeResults, [nodeId]: prevNodeResult }
          : (() => { const r = { ...state.nodeResults }; delete r[nodeId]; return r })(),
        livePreviewsByNodeId: {
          ...state.livePreviewsByNodeId,
          [nodeId]: {
            nodeId,
            sessionId: result.session_id,
            columns: result.columns,
            columnTypes: result.column_types,
            rows: result.rows,
            hasMore: result.has_more,
            bufferCapped: result.buffer_capped,
            loading: false,
            materializing: false,
            error: null,
          },
        },
      }))
    } catch (err) {
      const missingTables = node.type === 'transform' ? getMissingUpstreamTables(err) : null
      const restorePrePreviewState = () => set((state) => {
        const nodeResults = prevNodeResult
          ? { ...state.nodeResults, [nodeId]: prevNodeResult }
          : (() => { const r = { ...state.nodeResults }; delete r[nodeId]; return r })()
        const livePreviewsByNodeId = { ...state.livePreviewsByNodeId }
        delete livePreviewsByNodeId[nodeId]
        return { nodeResults, livePreviewsByNodeId }
      })

      if (missingTables && missingTables.length > 0) {
        // The cache-status pre-check was stale; the backend still says these
        // upstreams have no data. Gate here too instead of surfacing an error.
        restorePrePreviewState()
        const ancestorIds = collectAncestorNodeIds(nodeId, get().edges, get().nodes)
        const nodeMap = new Map(get().nodes.map((candidate) => [candidate.id, candidate]))
        const candidates: LoadDestinationCandidate[] = ancestorIds
          .map((id) => nodeMap.get(id))
          .filter((candidate): candidate is Node => Boolean(candidate))
          .filter((candidate) => missingTables.includes(getTableName(candidate)))
          .map((candidate) => ({
            nodeId: candidate.id,
            label: ((candidate.data as Record<string, unknown>).label as string) || getTableName(candidate),
            tableName: getTableName(candidate),
          }))
        if (candidates.length > 0) {
          set({
            loadDestinationPrompt: {
              targetNodeId: nodeId,
              resumeKind: 'live-preview',
              candidates,
              choices: Object.fromEntries(candidates.map((c) => [c.nodeId, 'in_memory' as NodeLoadMode])),
            },
          })
          return
        }
      }

      set((state) => ({
        // Restore the pre-preview result on error too.
        nodeResults: prevNodeResult
          ? { ...state.nodeResults, [nodeId]: prevNodeResult }
          : (() => { const r = { ...state.nodeResults }; delete r[nodeId]; return r })(),
        livePreviewsByNodeId: {
          ...state.livePreviewsByNodeId,
          [nodeId]: {
            ...(state.livePreviewsByNodeId[nodeId] ?? { nodeId, sessionId: null, columns: [], columnTypes: [], rows: [], hasMore: false, bufferCapped: false, materializing: false }),
            loading: false,
            error: getRequestErrorMessage(err, 'Unable to start preview'),
          },
        },
      }))
    }
  },

  loadMoreLivePreview: async (nodeId) => {
    const live = get().livePreviewsByNodeId[nodeId]
    if (!live?.sessionId || live.loading || live.materializing || !live.hasMore || live.bufferCapped) return

    set((state) => ({
      livePreviewsByNodeId: {
        ...state.livePreviewsByNodeId,
        [nodeId]: { ...state.livePreviewsByNodeId[nodeId], loading: true },
      },
    }))
    try {
      const chunk = await api.fetchPreviewSessionRows(live.sessionId)
      set((state) => {
        const current = state.livePreviewsByNodeId[nodeId]
        if (!current) return {}
        return {
          livePreviewsByNodeId: {
            ...state.livePreviewsByNodeId,
            [nodeId]: {
              ...current,
              rows: [...current.rows, ...chunk.rows],
              hasMore: chunk.has_more,
              bufferCapped: chunk.buffer_capped,
              loading: false,
            },
          },
        }
      })
    } catch (err) {
      set((state) => ({
        livePreviewsByNodeId: {
          ...state.livePreviewsByNodeId,
          [nodeId]: {
            ...state.livePreviewsByNodeId[nodeId],
            loading: false,
            error: getRequestErrorMessage(err, 'Unable to load more rows'),
          },
        },
      }))
    }
  },

  materializeLivePreview: async (nodeId, intoMemory = false) => {
    const live = get().livePreviewsByNodeId[nodeId]
    if (!live?.sessionId || live.materializing) return
    const sessionId = live.sessionId
    const node = get().nodes.find((candidate) => candidate.id === nodeId)
    const tableName = node ? getTableName(node) : ''

    set((state) => ({
      livePreviewsByNodeId: {
        ...state.livePreviewsByNodeId,
        [nodeId]: { ...state.livePreviewsByNodeId[nodeId], materializing: true, error: null },
      },
    }))

    try {
      const run = await api.materializePreviewSession(sessionId, intoMemory)
      // The session is consumed once materialize starts; drop the live tab and
      // switch focus to the (forthcoming) materialized table tab.
      executionTrackedNodeIds.set(run.execution_id, [nodeId])
      executionPreviewTargets.set(run.execution_id, { nodeId, tableName })
      set((state) => {
        const livePreviewsByNodeId = { ...state.livePreviewsByNodeId }
        delete livePreviewsByNodeId[nodeId]
        return { livePreviewsByNodeId }
      })
      applyExecutionRunSnapshot(run, set, get)
      if (run.status === 'running') {
        scheduleExecutionPoll(run.execution_id, set, get)
      } else {
        await finalizeExecutionRun(run, set, get)
      }
    } catch (err) {
      set((state) => ({
        livePreviewsByNodeId: {
          ...state.livePreviewsByNodeId,
          [nodeId]: {
            ...state.livePreviewsByNodeId[nodeId],
            materializing: false,
            error: getRequestErrorMessage(err, 'Unable to materialize preview'),
          },
        },
      }))
    }
  },

  closeLivePreview: async (nodeId) => {
    const live = get().livePreviewsByNodeId[nodeId]
    if (live?.sessionId) {
      void api.closePreviewSession(live.sessionId).catch(() => {})
    }
    set((state) => {
      const livePreviewsByNodeId = { ...state.livePreviewsByNodeId }
      delete livePreviewsByNodeId[nodeId]
      const activePreviewTarget = state.activePreviewTarget?.kind === 'live' && state.activePreviewTarget.nodeId === nodeId
        ? getFallbackActivePreviewTarget(state.previewTabOrder)
        : state.activePreviewTarget
      return { livePreviewsByNodeId, activePreviewTarget }
    })
  },

  setLoadDestinationChoice: (nodeId, mode) => {
    set((state) => {
      if (!state.loadDestinationPrompt) return state
      return {
        loadDestinationPrompt: {
          ...state.loadDestinationPrompt,
          choices: { ...state.loadDestinationPrompt.choices, [nodeId]: mode },
        },
      }
    })
  },

  applyLoadDestinationChoiceToAll: (mode) => {
    set((state) => {
      if (!state.loadDestinationPrompt) return state
      const choices = Object.fromEntries(state.loadDestinationPrompt.candidates.map((c) => [c.nodeId, mode]))
      return { loadDestinationPrompt: { ...state.loadDestinationPrompt, choices } }
    })
  },

  cancelLoadDestinationPrompt: () => set({ loadDestinationPrompt: null }),

  confirmLoadDestinationPrompt: async () => {
    const prompt = get().loadDestinationPrompt
    if (!prompt) return
    set({ loadDestinationPrompt: null })

    // Persist each candidate's chosen destination onto its node config; the
    // engine reads load_mode from config when it runs that node.
    prompt.candidates.forEach((candidate) => {
      const candidateNode = get().nodes.find((n) => n.id === candidate.nodeId)
      if (!candidateNode) return
      const config = { ...getNodeConfig(candidateNode), load_mode: prompt.choices[candidate.nodeId] ?? 'in_memory' }
      get().updateNodeData(candidate.nodeId, { config })
    })

    const state = get()
    const candidateIds = new Set(prompt.candidates.map((c) => c.nodeId))
    // 'materialize': run the missing ancestors + the target together (writes the
    // target's table). 'live-preview': run only the ancestors — the target is a
    // view-only preview and must not be written as a table.
    const executingIds = prompt.resumeKind === 'materialize'
      ? new Set([...candidateIds, prompt.targetNodeId])
      : candidateIds

    const subpipeline: PipelineDefinition = {
      id: state.pipelineId,
      name: state.pipelineName,
      database_connections: state.databaseConnections,
      settings: state.projectSettings,
      nodes: state.nodes.filter((candidate) => executingIds.has(candidate.id)).map(serializeNode),
      edges: state.edges
        .filter((edge) => executingIds.has(edge.source) && executingIds.has(edge.target))
        .map((edge) => ({ id: edge.id, source: edge.source, target: edge.target })),
    }

    try {
      const run = await api.startPipelineExecution(subpipeline, true)
      executionTrackedNodeIds.set(run.execution_id, [...executingIds])
      if (prompt.resumeKind === 'materialize') {
        const targetNode = state.nodes.find((n) => n.id === prompt.targetNodeId)
        if (targetNode) {
          executionPreviewTargets.set(run.execution_id, { nodeId: prompt.targetNodeId, tableName: getTableName(targetNode) })
        }
      } else {
        executionLivePreviewResumeIds.set(run.execution_id, prompt.targetNodeId)
      }
      set({
        activePipelineExecutionId: run.execution_id,
        errorDialogNodeId: get().errorDialogNodeId === prompt.targetNodeId ? null : get().errorDialogNodeId,
      })
      applyExecutionRunSnapshot(run, set, get)
      if (run.status === 'running') {
        scheduleExecutionPoll(run.execution_id, set, get)
      } else {
        await finalizeExecutionRun(run, set, get)
      }
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : 'Unknown error'
      set({
        nodeResults: {
          ...get().nodeResults,
          [prompt.targetNodeId]: { node_id: prompt.targetNodeId, status: 'error', error: message },
        },
      })
    }
  },
}))
