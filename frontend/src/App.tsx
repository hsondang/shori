import { useEffect, useRef, useState } from 'react'
import { ReactFlowProvider } from '@xyflow/react'
import { Route, Routes, useLocation } from 'react-router-dom'
import ProjectSidebar from './components/projects/ProjectSidebar'
import ProjectHome from './components/projects/ProjectHome'
import PipelineEditorPage from './components/projects/PipelineEditorPage'

export default function App() {
  const location = useLocation()
  const [isProjectBrowserOpen, setIsProjectBrowserOpen] = useState(() => location.pathname === '/')
  const previousPathnameRef = useRef(location.pathname)
  const isProjectRoute = location.pathname.startsWith('/projects/')

  useEffect(() => {
    const previousPathname = previousPathnameRef.current
    const wasProjectRoute = previousPathname.startsWith('/projects/')

    if (location.pathname === '/') {
      setIsProjectBrowserOpen(true)
    } else if (isProjectRoute && !wasProjectRoute) {
      setIsProjectBrowserOpen(false)
    }

    previousPathnameRef.current = location.pathname
  }, [isProjectRoute, location.pathname])

  return (
    <div className="relative h-full overflow-hidden bg-stone-100">
      <button
        type="button"
        onClick={() => setIsProjectBrowserOpen((open) => !open)}
        aria-expanded={isProjectBrowserOpen}
        aria-controls="project-browser"
        className="absolute left-4 top-4 z-[60] inline-flex items-center gap-2 rounded-full border border-stone-300 bg-white/95 px-3 py-2 text-sm font-medium text-stone-700 shadow-sm backdrop-blur transition hover:border-stone-400 hover:bg-white"
      >
        <span className="text-base leading-none">{isProjectBrowserOpen ? '←' : '☰'}</span>
        <span>Projects</span>
      </button>

      {isProjectBrowserOpen && isProjectRoute && (
        <button
          type="button"
          aria-label="Close project browser"
          onClick={() => setIsProjectBrowserOpen(false)}
          className="absolute inset-0 z-30 bg-stone-950/15 backdrop-blur-[1px]"
        />
      )}

      <ProjectSidebar
        open={isProjectBrowserOpen}
        onClose={() => setIsProjectBrowserOpen(false)}
      />
      <main className="h-full min-w-0">
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
