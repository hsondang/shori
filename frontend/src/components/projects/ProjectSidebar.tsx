import { useEffect, useState } from 'react'
import { Link, useLocation, useNavigate } from 'react-router-dom'
import { listPipelines, savePipeline } from '../../api/client'
import { createBlankPipelineDefinition } from '../../lib/pipelineDefinitions'
import { usePipelineStore } from '../../store/pipelineStore'
import type { ProjectSummary } from '../../types/pipeline'

function formatUpdatedAt(value: string): string {
  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) return 'Recently updated'
  return parsed.toLocaleString()
}

export default function ProjectSidebar() {
  const projectListRevision = usePipelineStore((s) => s.projectListRevision)
  const confirmDiscardChanges = usePipelineStore((s) => s.confirmDiscardChanges)
  const markProjectCatalogChanged = usePipelineStore((s) => s.markProjectCatalogChanged)
  const [projects, setProjects] = useState<ProjectSummary[]>([])
  const [loading, setLoading] = useState(true)
  const [creating, setCreating] = useState(false)
  const location = useLocation()
  const navigate = useNavigate()

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

  const activeProjectId = location.pathname.startsWith('/projects/')
    ? location.pathname.split('/')[2] ?? null
    : null

  const handleOpenProject = (project: ProjectSummary) => {
    if (activeProjectId === project.id) return
    if (!confirmDiscardChanges(project.name)) return
    navigate(`/projects/${project.id}`)
  }

  const handleCreateProject = async () => {
    if (!confirmDiscardChanges('Untitled Pipeline')) return

    setCreating(true)
    try {
      const pipeline = createBlankPipelineDefinition()
      await savePipeline(pipeline)
      markProjectCatalogChanged()
      navigate(`/projects/${pipeline.id}`)
    } finally {
      setCreating(false)
    }
  }

  return (
    <aside className="flex h-full w-80 shrink-0 flex-col border-r border-stone-200 bg-[#f6f1e8]">
      <div className="border-b border-stone-200 px-5 py-5">
        <Link to="/" className="block">
          <div className="text-[11px] font-semibold uppercase tracking-[0.28em] text-stone-500">Shori</div>
          <h1 className="mt-2 font-serif text-2xl text-stone-900">Projects</h1>
          <p className="mt-2 text-sm text-stone-600">
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
                <button
                  key={project.id}
                  type="button"
                  onClick={() => handleOpenProject(project)}
                  className={`w-full rounded-xl border px-3 py-3 text-left transition ${
                    isActive
                      ? 'border-stone-900 bg-stone-900 text-stone-50 shadow-sm'
                      : 'border-transparent bg-white/80 text-stone-800 hover:border-stone-300 hover:bg-white'
                  }`}
                >
                  <div className="truncate text-sm font-semibold">{project.name}</div>
                  <div className={`mt-1 text-xs ${isActive ? 'text-stone-300' : 'text-stone-500'}`}>
                    Updated {formatUpdatedAt(project.updated_at)}
                  </div>
                </button>
              )
            })}
          </div>
        )}
      </div>
    </aside>
  )
}
