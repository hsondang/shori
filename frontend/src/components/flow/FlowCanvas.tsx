import { useCallback, useMemo, useRef } from 'react'
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  type ReactFlowInstance,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { usePipelineStore } from '../../store/pipelineStore'
import CsvSourceNode from './nodes/CsvSourceNode'
import DatabaseSourceNode from './nodes/DatabaseSourceNode'
import TransformNode from './nodes/TransformNode'
import ExportNode from './nodes/ExportNode'
import type { NodeType } from '../../types/pipeline'

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
  const addNode = usePipelineStore((s) => s.addNode)
  const setSelectedNodeId = usePipelineStore((s) => s.setSelectedNodeId)
  const rfInstance = useRef<ReactFlowInstance | null>(null)

  const onDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    e.dataTransfer.dropEffect = 'move'
  }, [])

  const onDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault()
      const type = e.dataTransfer.getData('application/shori-node-type') as NodeType
      if (!type || !rfInstance.current) return

      const position = rfInstance.current.screenToFlowPosition({
        x: e.clientX,
        y: e.clientY,
      })
      addNode(type, position)
    },
    [addNode]
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
