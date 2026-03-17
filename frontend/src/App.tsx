import { ReactFlowProvider } from '@xyflow/react'
import FlowCanvas from './components/flow/FlowCanvas'
import NodeErrorDialog from './components/flow/NodeErrorDialog'
import Toolbar from './components/toolbar/Toolbar'
import NodeConfigPanel from './components/panels/NodeConfigPanel'
import DataPreviewPanel from './components/panels/DataPreviewPanel'

export default function App() {
  return (
    <ReactFlowProvider>
      <div className="h-full flex flex-col">
        <Toolbar />
        <div className="flex-1 flex overflow-hidden">
          <div className="flex-1">
            <FlowCanvas />
          </div>
          <NodeConfigPanel />
        </div>
        <DataPreviewPanel />
        <NodeErrorDialog />
      </div>
    </ReactFlowProvider>
  )
}
