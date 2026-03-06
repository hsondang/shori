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
import type { NodeType, NodeExecutionResult, DataPreview } from '../types/pipeline'
import * as api from '../api/client'

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
  setPipelineName: (name: string) => void

  // Execution results
  nodeResults: Record<string, NodeExecutionResult>

  // Selected node
  selectedNodeId: string | null
  setSelectedNodeId: (id: string | null) => void

  // Data preview
  previewData: DataPreview | null
  previewNodeId: string | null
  previewLoading: boolean

  // Actions
  addNode: (type: NodeType, position: { x: number; y: number }) => void
  updateNodeData: (nodeId: string, data: Record<string, unknown>) => void
  deleteNode: (nodeId: string) => void
  executePipeline: (force?: boolean) => Promise<void>
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
    case 'db_source': return { db_type: 'postgres', connection: { host: '', port: 5432, database: '', user: '', password: '' }, query: '' }
    case 'transform': return { sql: '' }
    case 'export': return { format: 'csv' }
  }
}

export const usePipelineStore = create<PipelineState>((set, get) => ({
  nodes: [],
  edges: [],
  pipelineId: crypto.randomUUID(),
  pipelineName: 'Untitled Pipeline',
  nodeResults: {},
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
    })
  },

  executePipeline: async (force = false) => {
    const { nodes, edges, pipelineId, pipelineName } = get()

    // Mark all nodes as running
    const runningResults: Record<string, NodeExecutionResult> = {}
    nodes.forEach((n) => {
      runningResults[n.id] = { node_id: n.id, status: 'running' }
    })
    set({ nodeResults: runningResults })

    const pipeline = {
      id: pipelineId,
      name: pipelineName,
      nodes: nodes.map((n) => ({
        id: n.id,
        type: n.type as NodeType,
        table_name: (n.data as Record<string, unknown>).tableName as string,
        label: (n.data as Record<string, unknown>).label as string,
        position: n.position,
        config: (n.data as Record<string, unknown>).config as Record<string, unknown>,
      })),
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
      set({ nodeResults: errorResults })
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
    const { nodes, edges, pipelineId, pipelineName } = get()
    const pipeline = {
      id: pipelineId,
      name: pipelineName,
      nodes: nodes.map((n) => ({
        id: n.id,
        type: n.type as NodeType,
        table_name: (n.data as Record<string, unknown>).tableName as string,
        label: (n.data as Record<string, unknown>).label as string,
        position: n.position,
        config: (n.data as Record<string, unknown>).config as Record<string, unknown>,
      })),
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
      nodeResults: {},
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
      nodeResults: {},
      selectedNodeId: null,
      previewData: null,
      previewNodeId: null,
    })
  },
}))
