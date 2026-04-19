import { useCallback, useEffect, useRef } from 'react'
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  type ReactFlowInstance,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import {
  buildDatabaseSourceDraftFromGlobalConnection,
  buildDatabaseSourceDraftFromConnection,
  buildNodeDraft,
  usePipelineStore,
} from '../../store/pipelineStore'
import { useSettingsStore } from '../../store/settingsStore'
import CsvSourceNode from './nodes/CsvSourceNode'
import DatabaseSourceNode from './nodes/DatabaseSourceNode'
import TransformNode from './nodes/TransformNode'
import ExportNode from './nodes/ExportNode'
import type { NodeType, SavedDatabaseConnection } from '../../types/pipeline'
import {
  DATABASE_CONNECTION_MIME,
  DATABASE_CONNECTION_SCOPE_MIME,
  NODE_TYPE_MIME,
} from '../../lib/dragData'

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
  const globalDatabaseConnections = useSettingsStore((s) => s.globalDatabaseConnections)
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
        const connectionScope = e.dataTransfer.getData(DATABASE_CONNECTION_SCOPE_MIME)
        const connectionPool = connectionScope === 'global' ? globalDatabaseConnections : databaseConnections
        const savedConnection = connectionPool.find((item) => item.id === connectionId) as SavedDatabaseConnection | undefined
        if (!savedConnection) return
        openCreateNodeEditor(
          connectionScope === 'global'
            ? buildDatabaseSourceDraftFromGlobalConnection(savedConnection, position)
            : buildDatabaseSourceDraftFromConnection(savedConnection, position)
        )
        return
      }

      const type = e.dataTransfer.getData(NODE_TYPE_MIME) as NodeType
      if (!type) return
      openCreateNodeEditor(buildNodeDraft(type, position))
    },
    [databaseConnections, globalDatabaseConnections, openCreateNodeEditor]
  )

  const onPaneClick = useCallback(() => {
    setSelectedNodeId(null)
  }, [setSelectedNodeId])

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key !== 'Delete' && event.key !== 'Backspace') return
      const activeElement = document.activeElement
      if (
        activeElement instanceof HTMLInputElement
        || activeElement instanceof HTMLTextAreaElement
        || activeElement instanceof HTMLSelectElement
        || activeElement?.getAttribute('contenteditable') === 'true'
      ) {
        return
      }

      const selectedEdges = edges.filter((edge) => edge.selected)
      if (selectedEdges.length === 0) return
      event.preventDefault()
      onEdgesChange(selectedEdges.map((edge) => ({ id: edge.id, type: 'remove' })))
    }

    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [edges, onEdgesChange])

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
      deleteKeyCode={['Delete', 'Backspace']}
      className="bg-gray-50"
    >
      <Background />
      <Controls />
      <MiniMap />
    </ReactFlow>
  )
}
