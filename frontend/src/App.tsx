import { useEffect, useRef, useState } from 'react'
import { ReactFlowProvider } from '@xyflow/react'
import { Route, Routes, useLocation } from 'react-router-dom'
import ProjectSidebar from './components/projects/ProjectSidebar'
import ProjectHome from './components/projects/ProjectHome'
import PipelineEditorPage from './components/projects/PipelineEditorPage'
import Toolbar from './components/toolbar/Toolbar'
import {
  PROJECT_BROWSER_SIDEBAR_WIDTH_PX,
  PROJECT_BROWSER_TRIGGER_SLOT_PX,
} from './components/projects/projectLayout'

interface ProjectBrowserToggleButtonProps {
  open: boolean
  onToggle: () => void
}

function ProjectBrowserToggleButton({ open, onToggle }: ProjectBrowserToggleButtonProps) {
  return (
    <button
      type="button"
      onClick={onToggle}
      aria-expanded={open}
      aria-controls="project-browser"
      aria-label={open ? 'Close project browser' : 'Open project browser'}
      className="inline-flex h-10 w-10 items-center justify-center rounded-lg border border-gray-200 bg-white text-stone-700 transition hover:bg-white"
    >
      <span className="flex flex-col gap-1">
        <span className="block h-[1.5px] w-4 rounded-full bg-current" />
        <span className="block h-[1.5px] w-4 rounded-full bg-current" />
        <span className="block h-[1.5px] w-4 rounded-full bg-current" />
      </span>
    </button>
  )
}

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
      {!isProjectRoute && (
        <div className="fixed left-3 top-3 z-[70]">
          <ProjectBrowserToggleButton
            open={isProjectBrowserOpen}
            onToggle={() => setIsProjectBrowserOpen((open) => !open)}
          />
        </div>
      )}

      {!isProjectRoute && (
        <ProjectSidebar
          open={isProjectBrowserOpen}
          onClose={() => setIsProjectBrowserOpen(false)}
          variant="overlay"
        />
      )}

      {isProjectRoute ? (
        <div className="flex h-full min-w-0 flex-col">
          <div
            data-testid="project-editor-header"
            className="relative z-20 flex shrink-0 items-stretch border-b border-gray-200 bg-white"
          >
            <div
              data-testid="project-browser-trigger-slot"
              className="flex shrink-0 items-center justify-center bg-white"
              style={{ width: `${PROJECT_BROWSER_TRIGGER_SLOT_PX}px` }}
            >
              <ProjectBrowserToggleButton
                open={isProjectBrowserOpen}
                onToggle={() => setIsProjectBrowserOpen((open) => !open)}
              />
            </div>
            <div className="min-w-0 flex-1">
              <Toolbar />
            </div>
          </div>

          <div data-testid="project-editor-body" className="relative flex min-h-0 min-w-0 flex-1">
            {isProjectBrowserOpen && (
              <button
                type="button"
                data-testid="project-browser-mobile-backdrop"
                aria-label="Close project browser"
                onClick={() => setIsProjectBrowserOpen(false)}
                className="absolute inset-0 z-30 bg-stone-950/15 backdrop-blur-[1px] md:hidden"
              />
            )}

            <div
              data-testid="project-sidebar-rail"
              className={`absolute inset-y-0 left-0 z-40 max-w-[calc(100vw-1rem)] overflow-hidden transition-[width,transform] duration-200 md:relative md:inset-auto md:z-auto md:h-full md:max-w-none md:shrink-0 md:translate-x-0 ${
                isProjectBrowserOpen
                  ? 'translate-x-0'
                  : '-translate-x-[calc(100%+1rem)] pointer-events-none md:pointer-events-auto'
              }`}
              style={{
                width: isProjectBrowserOpen ? `${PROJECT_BROWSER_SIDEBAR_WIDTH_PX}px` : '0px',
                maxWidth: 'calc(100vw - 1rem)',
              }}
            >
              <ProjectSidebar
                open={isProjectBrowserOpen}
                onClose={() => setIsProjectBrowserOpen(false)}
                variant="docked"
              />
            </div>

            <main className="min-h-0 min-w-0 flex-1">
              <Routes>
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
        </div>
      ) : (
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
      )}
    </div>
  )
}
