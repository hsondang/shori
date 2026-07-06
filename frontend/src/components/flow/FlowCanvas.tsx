import { useCallback, useEffect, useMemo, useRef } from 'react'
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  type IsValidConnection,
  type ReactFlowInstance,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { NODE_CARD_DISMISS_EVENT } from '@shori/design-system'
import {
  buildDatabaseSourceDraftFromGlobalConnection,
  buildDatabaseSourceDraftFromConnection,
  buildNodeDraft,
  usePipelineStore,
} from '../../store/pipelineStore'
import { useSettingsStore } from '../../store/settingsStore'
import CsvSourceNode from './nodes/CsvSourceNode'
import ExcelSourceNode from './nodes/ExcelSourceNode'
import ExcelWorkbookNode from './nodes/ExcelWorkbookNode'
import DatabaseSourceNode from './nodes/DatabaseSourceNode'
import TransformNode from './nodes/TransformNode'
import ExportNode from './nodes/ExportNode'
import { buildNodesById, isStructuralEdge } from '../../lib/structuralEdges'
import type { NodeType, SavedDatabaseConnection } from '../../types/pipeline'
import {
  DATABASE_CONNECTION_MIME,
  DATABASE_CONNECTION_SCOPE_MIME,
  NODE_TYPE_MIME,
} from '../../lib/dragData'

const nodeTypes = {
  csv_source: CsvSourceNode,
  excel_source: ExcelSourceNode,
  excel_workbook: ExcelWorkbookNode,
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

  // A node-card action menu is portaled to <body> at a screen position captured
  // when it opens, so any pan/zoom detaches it from its button. Tell open menus
  // to dismiss on viewport change; re-opening re-anchors to the new size/position.
  const onMove = useCallback(() => {
    window.dispatchEvent(new Event(NODE_CARD_DISMISS_EVENT))
  }, [])

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

      // Structural workbook→sheet edges live and die with their sheet node
      // and are never removable on their own (docs/excel-node-model.md §5).
      const nodesById = buildNodesById(nodes)
      const selectedEdges = edges.filter(
        (edge) => edge.selected && !isStructuralEdge(edge, nodesById),
      )
      if (selectedEdges.length === 0) return
      event.preventDefault()
      onEdgesChange(selectedEdges.map((edge) => ({ id: edge.id, type: 'remove' })))
    }

    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [edges, nodes, onEdgesChange])

  // Presentation only (never persisted): structural edges render dashed/muted
  // and are not deletable via React Flow's built-in delete handling.
  const displayEdges = useMemo(() => {
    const nodesById = buildNodesById(nodes)
    return edges.map((edge) =>
      isStructuralEdge(edge, nodesById)
        ? {
            ...edge,
            deletable: false,
            style: { strokeDasharray: '6 4', opacity: 0.55, ...edge.style },
          }
        : edge,
    )
  }, [edges, nodes])

  // Hubs never take part in user-drawn connections (structural edges are
  // created only by the sheet picker) — refuse the drag before onConnect fires.
  const isValidConnection = useCallback<IsValidConnection>(
    (connection) => {
      const nodesById = buildNodesById(nodes)
      const isHub = (id: string | null | undefined) =>
        id != null && nodesById.get(id)?.type === 'excel_workbook'
      return !isHub(connection.source) && !isHub(connection.target)
    },
    [nodes],
  )

  return (
    <ReactFlow
      nodes={nodes}
      edges={displayEdges}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      onConnect={onConnect}
      isValidConnection={isValidConnection}
      onInit={(instance) => { rfInstance.current = instance }}
      onMove={onMove}
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
