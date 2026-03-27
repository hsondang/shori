import { useEffect, useRef, useState } from 'react'
import { Link, useLocation, useNavigate } from 'react-router-dom'
import { deletePipeline, listPipelines, savePipeline, setPipelineStar } from '../../api/client'
import { createBlankPipelineDefinition } from '../../lib/pipelineDefinitions'
import { usePipelineStore } from '../../store/pipelineStore'
import type { ProjectSummary } from '../../types/pipeline'
import { PROJECT_BROWSER_TRIGGER_SLOT_PX } from './projectLayout'

function formatUpdatedAt(value: string): string {
  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) return 'Recently updated'
  return parsed.toLocaleString()
}

interface ProjectSidebarProps {
  open: boolean
  onClose: () => void
  variant?: 'docked' | 'overlay'
}

export default function ProjectSidebar({
  open,
  onClose,
  variant = 'overlay',
}: ProjectSidebarProps) {
  const projectListRevision = usePipelineStore((s) => s.projectListRevision)
  const confirmDiscardChanges = usePipelineStore((s) => s.confirmDiscardChanges)
  const markProjectCatalogChanged = usePipelineStore((s) => s.markProjectCatalogChanged)
  const newPipeline = usePipelineStore((s) => s.newPipeline)
  const [projects, setProjects] = useState<ProjectSummary[]>([])
  const [loading, setLoading] = useState(true)
  const [creating, setCreating] = useState(false)
  const [menuProjectId, setMenuProjectId] = useState<string | null>(null)
  const location = useLocation()
  const navigate = useNavigate()
  const rootRef = useRef<HTMLElement>(null)

  useEffect(() => {
    let cancelled = false

    const loadProjects = async () => {
      setLoading(true)
      try {
        const items = await listPipelines()
        if (!cancelled) {
          setProjects(items)
        }
      } finally {
        if (!cancelled) {
          setLoading(false)
        }
      }
    }

    void loadProjects()

    return () => {
      cancelled = true
    }
  }, [projectListRevision])

  useEffect(() => {
    const handlePointerDown = (event: MouseEvent) => {
      if (!rootRef.current?.contains(event.target as Node)) {
        setMenuProjectId(null)
      }
    }

    document.addEventListener('mousedown', handlePointerDown)
    return () => document.removeEventListener('mousedown', handlePointerDown)
  }, [])

  const activeProjectId = location.pathname.startsWith('/projects/')
    ? location.pathname.split('/')[2] ?? null
    : null

  const handleOpenProject = (project: ProjectSummary) => {
    if (activeProjectId === project.id) return
    if (!confirmDiscardChanges(project.name)) return
    setMenuProjectId(null)
    onClose()
    navigate(`/projects/${project.id}`)
  }

  const handleCreateProject = async () => {
    if (!confirmDiscardChanges('Untitled Pipeline')) return

    setCreating(true)
    try {
      const pipeline = createBlankPipelineDefinition()
      await savePipeline(pipeline)
      markProjectCatalogChanged()
      onClose()
      navigate(`/projects/${pipeline.id}`)
    } finally {
      setCreating(false)
    }
  }

  const handleToggleStar = async (project: ProjectSummary) => {
    await setPipelineStar(project.id, !project.starred)
    setMenuProjectId(null)
    markProjectCatalogChanged()
  }

  const handleDeleteProject = async (project: ProjectSummary) => {
    const confirmed = window.confirm(`Delete "${project.name}"? This cannot be undone.`)
    if (!confirmed) return

    await deletePipeline(project.id)
    setMenuProjectId(null)

    if (activeProjectId === project.id) {
      newPipeline()
      onClose()
      navigate('/')
    }

    markProjectCatalogChanged()
  }

  const sidebarClassName = variant === 'docked'
    ? 'relative flex h-full w-full min-w-0 flex-col border-r border-stone-200 bg-[#f6f1e8] shadow-[0_24px_80px_rgba(51,39,20,0.18)] md:shadow-none'
    : `absolute inset-y-0 left-0 z-50 flex h-full w-80 max-w-[calc(100vw-1rem)] flex-col border-r border-stone-200 bg-[#f6f1e8] shadow-[0_24px_80px_rgba(51,39,20,0.18)] transition-transform duration-200 ${
        open ? 'translate-x-0' : '-translate-x-[calc(100%+1rem)] pointer-events-none'
      }`

  return (
    <aside
      id="project-browser"
      ref={rootRef}
      aria-hidden={!open}
      data-variant={variant}
      className={sidebarClassName}
    >
      <div className="border-b border-stone-200 px-5 pb-5 pt-5">
        <Link to="/" className="block">
          <div style={{ paddingLeft: `${PROJECT_BROWSER_TRIGGER_SLOT_PX}px` }}>
            <div className="text-sm font-semibold uppercase tracking-[0.28em] text-stone-500">Shori</div>
          </div>
          <h1 className="mt-3 font-serif text-2xl text-stone-900">Projects</h1>
          <p className="mt-2 max-w-[16rem] text-sm text-stone-600">
            Centralized local catalog for every pipeline project.
          </p>
        </Link>
        <button
          type="button"
          onClick={() => { void handleCreateProject() }}
          disabled={creating}
          className="mt-4 w-full rounded-lg bg-stone-900 px-3 py-2 text-sm font-medium text-stone-50 transition hover:bg-stone-700 disabled:cursor-not-allowed disabled:opacity-60"
        >
          {creating ? 'Creating...' : 'New Project'}
        </button>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto px-3 py-4">
        {loading ? (
          <p className="px-2 text-sm text-stone-500">Loading projects...</p>
        ) : projects.length === 0 ? (
          <div className="rounded-xl border border-dashed border-stone-300 bg-white/70 px-4 py-5 text-sm text-stone-500">
            No projects yet. Create one to start building a pipeline.
          </div>
        ) : (
          <div className="space-y-2">
            {projects.map((project) => {
              const isActive = activeProjectId === project.id
              return (
                <div
                  key={project.id}
                  className={`relative flex items-start gap-2 rounded-xl border px-2 py-2 transition ${
                    isActive
                      ? 'border-stone-900 bg-stone-900 text-stone-50 shadow-sm'
                      : 'border-transparent bg-white/80 text-stone-800 hover:border-stone-300 hover:bg-white'
                  }`}
                >
                  <button
                    type="button"
                    aria-label={`Open project ${project.name}`}
                    onClick={() => handleOpenProject(project)}
                    className="min-w-0 flex-1 rounded-lg px-1 py-1 text-left"
                  >
                    <div className="flex items-center gap-2">
                      <div className="truncate text-sm font-semibold">{project.name}</div>
                      {project.starred && (
                        <span className={`text-xs ${isActive ? 'text-amber-300' : 'text-amber-500'}`}>★</span>
                      )}
                    </div>
                    <div className={`mt-1 text-xs ${isActive ? 'text-stone-300' : 'text-stone-500'}`}>
                      Updated {formatUpdatedAt(project.updated_at)}
                    </div>
                  </button>

                  <button
                    type="button"
                    aria-label={`More options for ${project.name}`}
                    aria-expanded={menuProjectId === project.id}
                    onClick={() => {
                      setMenuProjectId((current) => current === project.id ? null : project.id)
                    }}
                    className={`shrink-0 rounded-lg px-2 py-1 text-lg leading-none transition ${
                      isActive ? 'text-stone-200 hover:bg-stone-800' : 'text-stone-500 hover:bg-stone-100'
                    }`}
                  >
                    ⋯
                  </button>

                  {menuProjectId === project.id && (
                    <div className="absolute right-2 top-12 z-10 min-w-36 rounded-xl border border-stone-200 bg-white p-1.5 text-sm text-stone-700 shadow-lg">
                      <button
                        type="button"
                        onClick={() => { void handleToggleStar(project) }}
                        className="block w-full rounded-lg px-3 py-2 text-left transition hover:bg-stone-100"
                      >
                        {project.starred ? 'Unstar' : 'Star'}
                      </button>
                      <button
                        type="button"
                        onClick={() => { void handleDeleteProject(project) }}
                        className="block w-full rounded-lg px-3 py-2 text-left text-red-600 transition hover:bg-red-50"
                      >
                        Delete
                      </button>
                    </div>
                  )}
                </div>
              )
            })}
          </div>
        )}
      </div>
    </aside>
  )
}
