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
  MaterializedPreviewTab,
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

  // Actions
  addNode: (type: NodeType, position: { x: number; y: number }) => void
  addDatabaseConnection: (connection: SavedDatabaseConnectionInput) => string
  updateDatabaseConnection: (id: string, connection: SavedDatabaseConnectionInput) => void
  deleteDatabaseConnection: (id: string) => void
  addDatabaseSourceFromConnection: (connectionId: string, position: { x: number; y: number }) => string | null
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

let nodeCounter = 0
const initialPipeline = createBlankPipelineDefinition()

function generateNodeId(): string {
  nodeCounter++
  return `node_${nodeCounter}_${Date.now().toString(36)}`
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
    errorDialogNodeId: null,
    selectedNodeId: null,
    previewTabsByNodeId: {},
    previewTabOrder: [],
    activePreviewTarget: null,
    transientPreview: getEmptyTransientPreview(),
    csvPreprocessArtifacts: {},
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
  errorDialogNodeId: null,
  selectedNodeId: null,
  previewTabsByNodeId: {},
  previewTabOrder: [],
  activePreviewTarget: null,
  transientPreview: getEmptyTransientPreview(),
  csvPreprocessArtifacts: {},

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

  addNode: (type, position) => {
    const id = generateNodeId()
    const tableName = id
    const autoLabel = defaultAutoLabel(type)
    const newNode: Node = {
      id,
      type,
      position,
      data: {
        label: autoLabel,
        autoLabel,
        labelMode: 'auto',
        tableName,
        config: defaultConfig(type),
      },
    }
    set({ nodes: [...get().nodes, newNode] })
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

    const id = generateNodeId()
    const connection = savedConnection.db_type === 'oracle'
      ? {
          host: savedConnection.host,
          port: savedConnection.port,
          service_name: savedConnection.service_name,
          user: savedConnection.user,
          password: savedConnection.password,
        }
      : {
          host: savedConnection.host,
          port: savedConnection.port,
          database: savedConnection.database,
          user: savedConnection.user,
          password: savedConnection.password,
        }
    const newNode: Node = {
      id,
      type: 'db_source',
      position,
      data: {
        label: savedConnection.name,
        autoLabel: savedConnection.name,
        labelMode: 'auto',
        tableName: id,
        config: {
          db_type: savedConnection.db_type,
          connection,
          query: '',
        },
      },
    }
    set({ nodes: [...get().nodes, newNode] })
    const state = get()
    set({
      hasUnsavedChanges: snapshotPipelineDefinition(buildPipelineDefinitionFromState(state)) !== state.savedPipelineSnapshot,
    })
    return id
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
      }
    })
    const state = get()
    set({
      hasUnsavedChanges: snapshotPipelineDefinition(buildPipelineDefinitionFromState(state)) !== state.savedPipelineSnapshot,
    })
  },

  executePipeline: async (force = false) => {
    const { nodes, edges, pipelineId, pipelineName } = get()

    // Mark all nodes as running
    const runningResults: Record<string, NodeExecutionResult> = {}
    nodes.forEach((n) => {
      runningResults[n.id] = { node_id: n.id, status: 'running' }
    })
    set({
      nodeResults: runningResults,
      errorDialogNodeId: null,
    })

    const pipeline = {
      id: pipelineId,
      name: pipelineName,
      database_connections: get().databaseConnections,
      nodes: nodes.map(serializeNode),
      edges: edges.map((e) => ({ id: e.id, source: e.source, target: e.target })),
    }

    try {
      const results = await api.executePipeline(pipeline, force)
      set({ nodeResults: results })
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : 'Unknown error'
      const errorResults: Record<string, NodeExecutionResult> = {}
      nodes.forEach((n) => {
        errorResults[n.id] = { node_id: n.id, status: 'error', error: message }
      })
      set({
        nodeResults: errorResults,
        errorDialogNodeId: null,
      })
    }
  },

  executeSingleNode: async (nodeId, options) => {
    const node = get().nodes.find((candidate) => candidate.id === nodeId)
    if (!node) return

    const tableName = (node.data as Record<string, unknown>).tableName as string
    set({
      nodeResults: {
        ...get().nodeResults,
        [nodeId]: { node_id: nodeId, status: 'running' },
      },
      errorDialogNodeId: get().errorDialogNodeId === nodeId ? null : get().errorDialogNodeId,
    })

    try {
      const result = await api.executeNode(serializeNode(node))
      set({
        nodeResults: {
          ...get().nodeResults,
          [nodeId]: result,
        },
        errorDialogNodeId: get().errorDialogNodeId === nodeId && result.status !== 'error'
          ? null
          : get().errorDialogNodeId,
      })

      if (options?.loadPreviewOnSuccess && result.status === 'success') {
        await get().loadTablePreview(nodeId, tableName, 0, { forceReload: true })
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
        const runningResults = { ...get().nodeResults }
        executingIds.forEach((executingId) => {
          runningResults[executingId] = { node_id: executingId, status: 'running' }
        })
        set({
          nodeResults: runningResults,
          errorDialogNodeId: get().errorDialogNodeId === nodeId ? null : get().errorDialogNodeId,
        })

        const subpipeline: PipelineDefinition = {
          id: state.pipelineId,
          name: state.pipelineName,
          database_connections: state.databaseConnections,
          nodes: state.nodes.filter((candidate) => executingIds.has(candidate.id)).map(serializeNode),
          edges: state.edges
            .filter((edge) => executingIds.has(edge.source) && executingIds.has(edge.target))
            .map((edge) => ({ id: edge.id, source: edge.source, target: edge.target })),
        }

        const results = await api.executePipeline(subpipeline, true)
        set({
          nodeResults: {
            ...get().nodeResults,
            ...results,
          },
          errorDialogNodeId: get().errorDialogNodeId === nodeId && results[nodeId]?.status !== 'error'
            ? null
            : get().errorDialogNodeId,
        })

        if (results[nodeId]?.status === 'success') {
          await get().loadTablePreview(nodeId, tableName, 0, { forceReload: true })
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

    const pipeline = await api.loadPipeline(id)
    set(hydratePipelineState(pipeline))
  },

  newPipeline: () => {
    get().nodes
      .filter((node) => node.type === 'csv_source')
      .forEach((node) => invalidateCsvPreprocessArtifact(node.id))

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
