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
  ExecutionRunStatus,
  MaterializedPreviewTab,
  NodeEditorDraft,
  NodeEditorMode,
  NodeLabelMode,
  NodeType,
  NodeExecutionResult,
  PipelineDefinition,
  SavedDatabaseConnection,
  SavedDatabaseConnectionInput,
  TablePreviewData,
  TransientPreviewState,
} from '../types/pipeline'
import * as api from '../api/client'
import { getCsvPreprocessFingerprint } from '../lib/csvPreprocessing'
import { defaultConnectionConfig } from '../lib/databaseConnections'
import {
  createBlankPipelineDefinition,
  snapshotPipelineDefinition,
} from '../lib/pipelineDefinitions'

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
  savedPipelineSnapshot: string
  hasUnsavedChanges: boolean
  projectListRevision: number
  setPipelineName: (name: string) => void

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
  executeSingleNode: (nodeId: string, options?: { loadPreviewOnSuccess?: boolean }) => Promise<void>
  runTransformPreview: (nodeId: string) => Promise<void>
  loadCsvPreview: (nodeId: string, filePath: string) => Promise<void>
  loadPreprocessedCsvPreview: (nodeId: string, filePath: string, preprocessing: CsvPreprocessingConfig) => Promise<void>
  loadTablePreview: (nodeId: string, tableName: string, offset?: number, options?: { forceReload?: boolean }) => Promise<void>
  savePipeline: () => Promise<void>
  loadPipeline: (id: string) => Promise<void>
  newPipeline: () => void
  markProjectCatalogChanged: () => void
  confirmDiscardChanges: (nextProjectName?: string) => boolean
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
      preprocessing: {
        enabled: false,
        runtime: 'python',
        script: '',
      },
    }
    case 'db_source': return { db_type: 'postgres', connection: defaultConnectionConfig('postgres'), query: '' }
    case 'transform': return { sql: '' }
    case 'export': return { format: 'csv' }
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
    tableName: overrides.tableName ?? id,
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
      autoLabel: nodeDef.auto_label ?? inferred.autoLabel,
      labelMode: nodeDef.label_mode ?? inferred.labelMode,
      tableName: nodeDef.table_name,
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
    table_name: (node.data as Record<string, unknown>).tableName as string,
    label: (node.data as Record<string, unknown>).label as string,
    auto_label: autoLabel,
    label_mode: labelMode,
    position: node.position,
    config: (node.data as Record<string, unknown>).config as Record<string, unknown>,
  }
}

function buildPipelineDefinitionFromState(state: Pick<PipelineState, 'nodes' | 'edges' | 'pipelineId' | 'pipelineName' | 'databaseConnections'>): PipelineDefinition {
  return {
    id: state.pipelineId,
    name: state.pipelineName,
    database_connections: state.databaseConnections,
    nodes: state.nodes.map(serializeNode),
    edges: state.edges.map((e) => ({ id: e.id, source: e.source, target: e.target })),
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

function dropMaterializedTable(tableName: string | undefined) {
  if (!tableName) return
  void api.deleteTable(tableName).catch(() => {})
}

function invalidateCsvPreprocessArtifact(nodeId: string | undefined) {
  if (!nodeId) return
  void api.deletePreprocessedCsvArtifact(nodeId).catch(() => {})
}

function getCsvConfig(node: Node): CsvSourceConfig | null {
  if (node.type !== 'csv_source') return null
  return getNodeConfig(node) as unknown as CsvSourceConfig
}

export function buildDatabaseSourceDraftFromConnection(
  connection: SavedDatabaseConnection,
  position: { x: number; y: number },
): NodeEditorDraft {
  const config = connection.db_type === 'oracle'
    ? {
        db_type: 'oracle',
        connection: {
          host: connection.host,
          port: connection.port,
          service_name: connection.service_name,
          user: connection.user,
          password: connection.password,
        },
        query: '',
      }
    : {
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
    config,
  })
}

function hasCsvLoadInputsChanged(
  node: Node,
  nextConfig?: Record<string, unknown>,
): boolean {
  if (node.type !== 'csv_source' || !nextConfig) return false
  const currentConfig = getCsvConfig(node)
  const mergedConfig = { ...(currentConfig ?? {}), ...nextConfig } as CsvSourceConfig

  return currentConfig?.file_path !== mergedConfig.file_path
    || getCsvPreprocessFingerprint(currentConfig) !== getCsvPreprocessFingerprint(mergedConfig)
}

function hasDbSourceInputsChanged(
  node: Node,
  nextConfig?: Record<string, unknown>,
): boolean {
  if (node.type !== 'db_source' || !nextConfig) return false

  const currentConfig = getNodeConfig(node)
  const mergedConfig = { ...currentConfig, ...nextConfig }

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

function collectAncestorNodeIds(nodeId: string, edges: Edge[]): string[] {
  const parentsByTarget = new Map<string, string[]>()
  edges.forEach((edge) => {
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

const EXECUTION_POLL_INTERVAL_MS = 2000
const EXECUTION_CLOCK_INTERVAL_MS = 1000
const EXECUTION_TRACKING_ERROR = 'Execution status unavailable. The backend may have restarted or the run expired.'

const executionPollTimeouts = new Map<string, ReturnType<typeof setTimeout>>()
const executionPreviewTargets = new Map<string, { nodeId: string; tableName: string }>()
const executionTrackedNodeIds = new Map<string, string[]>()
let executionClockInterval: ReturnType<typeof setInterval> | null = null

function isNotFoundError(error: unknown): boolean {
  if (typeof error !== 'object' || error === null) return false
  const response = 'response' in error ? error.response : undefined
  return typeof response === 'object' && response !== null && 'status' in response && response.status === 404
}

function clearExecutionPoll(executionId: string) {
  const timeoutId = executionPollTimeouts.get(executionId)
  if (timeoutId != null) {
    clearTimeout(timeoutId)
    executionPollTimeouts.delete(executionId)
  }
}

function clearAllExecutionTracking() {
  executionPollTimeouts.forEach((timeoutId) => clearTimeout(timeoutId))
  executionPollTimeouts.clear()
  executionPreviewTargets.clear()
  executionTrackedNodeIds.clear()
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
  set((state) => {
    const nodeResults = { ...state.nodeResults }
    const activeExecutions = { ...state.activeExecutions }
    const activeExecutionIdByNodeId = { ...state.activeExecutionIdByNodeId }

    Object.entries(run.node_results).forEach(([nodeId, result]) => {
      nodeResults[nodeId] = result
      if (result.status === 'running') {
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
  clearExecutionPoll(run.execution_id)
  applyExecutionRunSnapshot(run, set, get)

  const previewTarget = executionPreviewTargets.get(run.execution_id)
  executionPreviewTargets.delete(run.execution_id)
  executionTrackedNodeIds.delete(run.execution_id)

  if (previewTarget && run.node_results[previewTarget.nodeId]?.status === 'success') {
    await get().loadTablePreview(previewTarget.nodeId, previewTarget.tableName, 0, { forceReload: true })
  }
}

function scheduleExecutionPoll(
  executionId: string,
  set: StoreSet,
  get: () => PipelineState,
) {
  clearExecutionPoll(executionId)
  const timeoutId = setTimeout(async () => {
    try {
      const run = await api.getExecutionRunStatus(executionId)
      if (run.status === 'running') {
        applyExecutionRunSnapshot(run, set, get)
        scheduleExecutionPoll(executionId, set, get)
        return
      }

      await finalizeExecutionRun(run, set, get)
    } catch (error) {
      if (isNotFoundError(error)) {
        failExecutionTracking(executionId, EXECUTION_TRACKING_ERROR, set, get)
        return
      }
      scheduleExecutionPoll(executionId, set, get)
    }
  }, EXECUTION_POLL_INTERVAL_MS)

  executionPollTimeouts.set(executionId, timeoutId)
}

export const usePipelineStore = create<PipelineState>((set, get) => ({
  nodes: [],
  edges: [],
  pipelineId: initialPipeline.id,
  pipelineName: initialPipeline.name,
  databaseConnections: [],
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

  onNodesChange: (changes) => {
    set({ nodes: applyNodeChanges(changes, get().nodes) })
    const state = get()
    set({
      hasUnsavedChanges: snapshotPipelineDefinition(buildPipelineDefinitionFromState(state)) !== state.savedPipelineSnapshot,
    })
  },

  onEdgesChange: (changes) => {
    set({ edges: applyEdgeChanges(changes, get().edges) })
    const state = get()
    set({
      hasUnsavedChanges: snapshotPipelineDefinition(buildPipelineDefinitionFromState(state)) !== state.savedPipelineSnapshot,
    })
  },

  onConnect: (connection) => {
    if (!connection.source || !connection.target || connection.source === connection.target) {
      return
    }
    set({ edges: addEdge({ ...connection, id: `edge_${Date.now()}` }, get().edges) })
    const state = get()
    set({
      hasUnsavedChanges: snapshotPipelineDefinition(buildPipelineDefinitionFromState(state)) !== state.savedPipelineSnapshot,
    })
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

    if (shouldInvalidateExecution) {
      dropMaterializedTable(previousTableName)
    }
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
  },

  deleteNode: (nodeId) => {
    const node = get().nodes.find((candidate) => candidate.id === nodeId)
    if (node) {
      dropMaterializedTable(getTableName(node))
      if (node.type === 'csv_source') {
        invalidateCsvPreprocessArtifact(nodeId)
      }
    }

    set((state) => {
      const nodeResults = { ...state.nodeResults }
      const csvPreprocessArtifacts = { ...state.csvPreprocessArtifacts }
      const previewTabsByNodeId = { ...state.previewTabsByNodeId }
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

      return {
        nodes: state.nodes.filter((n) => n.id !== nodeId),
        edges: state.edges.filter((e) => e.source !== nodeId && e.target !== nodeId),
        nodeResults,
        previewTabsByNodeId,
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
    const { nodes, edges, pipelineId, pipelineName } = get()

    const pipeline = {
      id: pipelineId,
      name: pipelineName,
      database_connections: get().databaseConnections,
      nodes: nodes.map(serializeNode),
      edges: edges.map((e) => ({ id: e.id, source: e.source, target: e.target })),
    }

    try {
      const run = await api.startPipelineExecution(pipeline, force)
      executionTrackedNodeIds.set(run.execution_id, nodes.map((node) => node.id))
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
      nodes.forEach((n) => {
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

    try {
      const run = await api.startNodeExecution(serializeNode(node))
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

  runTransformPreview: async (nodeId) => {
    const state = get()
    const node = state.nodes.find((candidate) => candidate.id === nodeId)
    if (!node || node.type !== 'transform') return

    const sql = (getNodeConfig(node).sql as string | undefined) ?? ''
    if (!sql.trim()) return

    try {
      const tableName = getTableName(node)
      const ancestorIds = collectAncestorNodeIds(nodeId, state.edges)
      const nodeMap = new Map(state.nodes.map((candidate) => [candidate.id, candidate]))
      const ancestorNodes = ancestorIds
        .map((ancestorId) => nodeMap.get(ancestorId))
        .filter((candidate): candidate is Node => Boolean(candidate))

      const materializationChecks = await Promise.all(
        ancestorNodes.map(async (ancestorNode) => ({
          node: ancestorNode,
          materialized: (await api.getTableSchema(getTableName(ancestorNode))) !== null,
        }))
      )
      const missingAncestorNodes = materializationChecks
        .filter((entry) => !entry.materialized)
        .map((entry) => entry.node)

      if (missingAncestorNodes.length > 0) {
        const missingNames = missingAncestorNodes.map((ancestorNode) => getTableName(ancestorNode)).join(', ')
        const shouldRunUpstream = window.confirm(
          `This transform depends on upstream tables that are not materialized yet: ${missingNames}. Run the missing upstream nodes first?`
        )
        if (!shouldRunUpstream) return

        const executingIds = new Set([...missingAncestorNodes.map((ancestorNode) => ancestorNode.id), nodeId])
        const subpipeline: PipelineDefinition = {
          id: state.pipelineId,
          name: state.pipelineName,
          database_connections: state.databaseConnections,
          nodes: state.nodes.filter((candidate) => executingIds.has(candidate.id)).map(serializeNode),
          edges: state.edges
            .filter((edge) => executingIds.has(edge.source) && executingIds.has(edge.target))
            .map((edge) => ({ id: edge.id, source: edge.source, target: edge.target })),
        }

        const run = await api.startPipelineExecution(subpipeline, true)
        executionTrackedNodeIds.set(run.execution_id, [...executingIds])
        executionPreviewTargets.set(run.execution_id, { nodeId, tableName })
        set({
          activePipelineExecutionId: run.execution_id,
          errorDialogNodeId: get().errorDialogNodeId === nodeId ? null : get().errorDialogNodeId,
        })
        applyExecutionRunSnapshot(run, set, get)
        if (run.status === 'running') {
          scheduleExecutionPoll(run.execution_id, set, get)
        } else {
          await finalizeExecutionRun(run, set, get)
        }
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
      const data = await api.previewData(tableName, offset) as TablePreviewData
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

  savePipeline: async () => {
    const pipeline = buildPipelineDefinitionFromState(get())
    await api.savePipeline(pipeline)
    set((state) => ({
      savedPipelineSnapshot: snapshotPipelineDefinition(pipeline),
      hasUnsavedChanges: false,
      projectListRevision: state.projectListRevision + 1,
    }))
  },

  loadPipeline: async (id) => {
    get().nodes
      .filter((node) => node.type === 'csv_source')
      .forEach((node) => invalidateCsvPreprocessArtifact(node.id))

    clearAllExecutionTracking()
    const pipeline = await api.loadPipeline(id)
    set(hydratePipelineState(pipeline))
  },

  newPipeline: () => {
    get().nodes
      .filter((node) => node.type === 'csv_source')
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
}))
