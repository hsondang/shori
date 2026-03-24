import { ReactFlowProvider } from '@xyflow/react'
import { Route, Routes } from 'react-router-dom'
import ProjectSidebar from './components/projects/ProjectSidebar'
import ProjectHome from './components/projects/ProjectHome'
import PipelineEditorPage from './components/projects/PipelineEditorPage'

export default function App() {
  return (
    <div className="h-full flex bg-stone-100">
      <ProjectSidebar />
      <main className="min-w-0 flex-1">
        <Routes>
          <Route path="/" element={<ProjectHome />} />
          <Route
            path="/projects/:projectId"
            element={(
              <ReactFlowProvider>
                <PipelineEditorPage />
              </ReactFlowProvider>
            )}
          />
        </Routes>
      </main>
    </div>
  )
}
