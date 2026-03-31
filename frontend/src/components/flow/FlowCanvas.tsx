import { useCallback, useRef } from 'react'
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  type ReactFlowInstance,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import {
  buildDatabaseSourceDraftFromConnection,
  buildNodeDraft,
  usePipelineStore,
} from '../../store/pipelineStore'
import CsvSourceNode from './nodes/CsvSourceNode'
import DatabaseSourceNode from './nodes/DatabaseSourceNode'
import TransformNode from './nodes/TransformNode'
import ExportNode from './nodes/ExportNode'
import type { NodeType, SavedDatabaseConnection } from '../../types/pipeline'
import { DATABASE_CONNECTION_MIME, NODE_TYPE_MIME } from '../../lib/dragData'

const nodeTypes = {
  csv_source: CsvSourceNode,
  db_source: DatabaseSourceNode,
  transform: TransformNode,
  export: ExportNode,
}

export default function FlowCanvas() {
  const nodes = usePipelineStore((s) => s.nodes)
  const edges = usePipelineStore((s) => s.edges)
  const onNodesChange = usePipelineStore((s) => s.onNodesChange)
  const onEdgesChange = usePipelineStore((s) => s.onEdgesChange)
  const onConnect = usePipelineStore((s) => s.onConnect)
  const databaseConnections = usePipelineStore((s) => s.databaseConnections)
  const openCreateNodeEditor = usePipelineStore((s) => s.openCreateNodeEditor)
  const setSelectedNodeId = usePipelineStore((s) => s.setSelectedNodeId)
  const rfInstance = useRef<ReactFlowInstance | null>(null)

  const onDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    e.dataTransfer.dropEffect = 'move'
  }, [])

  const onDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault()
      if (!rfInstance.current) return

      const position = rfInstance.current.screenToFlowPosition({
        x: e.clientX,
        y: e.clientY,
      })
      const connectionId = e.dataTransfer.getData(DATABASE_CONNECTION_MIME)
      if (connectionId) {
        const savedConnection = databaseConnections.find((item) => item.id === connectionId) as SavedDatabaseConnection | undefined
        if (!savedConnection) return
        openCreateNodeEditor(buildDatabaseSourceDraftFromConnection(savedConnection, position))
        return
      }

      const type = e.dataTransfer.getData(NODE_TYPE_MIME) as NodeType
      if (!type) return
      openCreateNodeEditor(buildNodeDraft(type, position))
    },
    [databaseConnections, openCreateNodeEditor]
  )

  const onPaneClick = useCallback(() => {
    setSelectedNodeId(null)
  }, [setSelectedNodeId])

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      onConnect={onConnect}
      onInit={(instance) => { rfInstance.current = instance }}
      onDragOver={onDragOver}
      onDrop={onDrop}
      onPaneClick={onPaneClick}
      nodeTypes={nodeTypes}
      fitView
      deleteKeyCode="Delete"
      className="bg-gray-50"
    >
      <Background />
      <Controls />
      <MiniMap />
    </ReactFlow>
  )
}
