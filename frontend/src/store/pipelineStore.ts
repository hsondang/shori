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
  NodeType,
  NodeExecutionResult,
  DataPreview,
  PipelineDefinition,
  SavedDatabaseConnection,
  SavedDatabaseConnectionInput,
} from '../types/pipeline'
import * as api from '../api/client'
import { defaultConnectionConfig } from '../lib/databaseConnections'

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
  previewData: DataPreview | null
  previewNodeId: string | null
  previewLoading: boolean

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
  loadPreview: (nodeId: string, tableName: string, offset?: number) => Promise<void>
  savePipeline: () => Promise<void>
  loadPipeline: (id: string) => Promise<void>
  newPipeline: () => void
}

let nodeCounter = 0

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
    case 'csv_source': return { file_path: '', original_filename: '' }
    case 'db_source': return { db_type: 'postgres', connection: defaultConnectionConfig('postgres'), query: '' }
    case 'transform': return { sql: '' }
    case 'export': return { format: 'csv' }
  }
}

function serializeNode(node: Node): PipelineDefinition['nodes'][number] {
  return {
    id: node.id,
    type: node.type as NodeType,
    table_name: (node.data as Record<string, unknown>).tableName as string,
    label: (node.data as Record<string, unknown>).label as string,
    position: node.position,
    config: (node.data as Record<string, unknown>).config as Record<string, unknown>,
  }
}

function getTableName(node: Node): string {
  return (node.data as Record<string, unknown>).tableName as string
}

function getNodeConfig(node: Node): Record<string, unknown> {
  return (node.data as Record<string, unknown>).config as Record<string, unknown>
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
  pipelineId: crypto.randomUUID(),
  pipelineName: 'Untitled Pipeline',
  databaseConnections: [],
  nodeResults: {},
  errorDialogNodeId: null,
  selectedNodeId: null,
  previewData: null,
  previewNodeId: null,
  previewLoading: false,

  setPipelineName: (name) => set({ pipelineName: name }),

  onNodesChange: (changes) => {
    set({ nodes: applyNodeChanges(changes, get().nodes) })
  },

  onEdgesChange: (changes) => {
    set({ edges: applyEdgeChanges(changes, get().edges) })
  },

  onConnect: (connection) => {
    set({ edges: addEdge({ ...connection, id: `edge_${Date.now()}` }, get().edges) })
  },

  setSelectedNodeId: (id) => set({ selectedNodeId: id }),
  openNodeError: (nodeId) => set({ errorDialogNodeId: nodeId }),
  closeNodeError: () => set({ errorDialogNodeId: null }),

  addNode: (type, position) => {
    const id = generateNodeId()
    const tableName = id
    const newNode: Node = {
      id,
      type,
      position,
      data: {
        label: defaultLabel(type),
        tableName,
        config: defaultConfig(type),
      },
    }
    set({ nodes: [...get().nodes, newNode] })
  },

  addDatabaseConnection: (connection) => {
    const id = crypto.randomUUID()
    set({ databaseConnections: [...get().databaseConnections, { ...connection, id } as SavedDatabaseConnection] })
    return id
  },

  updateDatabaseConnection: (id, connection) => {
    set({
      databaseConnections: get().databaseConnections.map((item) =>
        item.id === id ? ({ ...connection, id } as SavedDatabaseConnection) : item
      ),
    })
  },

  deleteDatabaseConnection: (id) => {
    set({
      databaseConnections: get().databaseConnections.filter((item) => item.id !== id),
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
        tableName: id,
        config: {
          db_type: savedConnection.db_type,
          connection,
          query: '',
        },
      },
    }
    set({ nodes: [...get().nodes, newNode] })
    return id
  },

  updateNodeData: (nodeId, data) => {
    set({
      nodes: get().nodes.map((n) =>
        n.id === nodeId ? { ...n, data: { ...n.data, ...data } } : n
      ),
    })
  },

  deleteNode: (nodeId) => {
    set({
      nodes: get().nodes.filter((n) => n.id !== nodeId),
      edges: get().edges.filter((e) => e.source !== nodeId && e.target !== nodeId),
      selectedNodeId: get().selectedNodeId === nodeId ? null : get().selectedNodeId,
      errorDialogNodeId: get().errorDialogNodeId === nodeId ? null : get().errorDialogNodeId,
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
        await get().loadPreview(nodeId, tableName)
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
          await get().loadPreview(nodeId, tableName)
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

  loadPreview: async (nodeId, tableName, offset = 0) => {
    set({ previewLoading: true, previewNodeId: nodeId })
    try {
      const data = await api.previewData(tableName, offset)
      set({ previewData: data, previewLoading: false })
    } catch {
      set({ previewData: null, previewLoading: false })
    }
  },

  savePipeline: async () => {
    const { nodes, edges, pipelineId, pipelineName, databaseConnections } = get()
    const pipeline = {
      id: pipelineId,
      name: pipelineName,
      database_connections: databaseConnections,
      nodes: nodes.map(serializeNode),
      edges: edges.map((e) => ({ id: e.id, source: e.source, target: e.target })),
    }
    await api.savePipeline(pipeline)
  },

  loadPipeline: async (id) => {
    const pipeline = await api.loadPipeline(id)
    const nodes: Node[] = pipeline.nodes.map((n) => ({
      id: n.id,
      type: n.type,
      position: n.position,
      data: {
        label: n.label,
        tableName: n.table_name,
        config: n.config,
      },
    }))
    const edges: Edge[] = pipeline.edges.map((e) => ({
      id: e.id,
      source: e.source,
      target: e.target,
    }))
    set({
      nodes,
      edges,
      pipelineId: pipeline.id,
      pipelineName: pipeline.name,
      databaseConnections: pipeline.database_connections || [],
      nodeResults: {},
      errorDialogNodeId: null,
      selectedNodeId: null,
      previewData: null,
      previewNodeId: null,
    })
  },

  newPipeline: () => {
    nodeCounter = 0
    set({
      nodes: [],
      edges: [],
      pipelineId: crypto.randomUUID(),
      pipelineName: 'Untitled Pipeline',
      databaseConnections: [],
      nodeResults: {},
      errorDialogNodeId: null,
      selectedNodeId: null,
      previewData: null,
      previewNodeId: null,
    })
  },
}))
