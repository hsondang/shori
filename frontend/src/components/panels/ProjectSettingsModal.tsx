import { useEffect, useState } from 'react'
import { usePipelineStore } from '../../store/pipelineStore'
import { compactProjectStorage, getProjectStorage } from '../../api/client'
import type { ProjectSettings } from '../../types/pipeline'

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  const units = ['KB', 'MB', 'GB', 'TB']
  let value = bytes / 1024
  let unit = 0
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024
    unit++
  }
  return `${value.toFixed(1)} ${units[unit]}`
}

const numberFields: Array<{
  key: keyof ProjectSettings
  label: string
  help: string
  min: number
  max: number
}> = [
  { key: 'max_concurrent_nodes', label: 'Max concurrent nodes', help: 'How many nodes run in parallel during a pipeline run.', min: 1, max: 32 },
  { key: 'max_connections_per_database', label: 'Max connections per database', help: 'Concurrent sessions opened against a single source database.', min: 1, max: 16 },
  { key: 'preview_chunk_rows', label: 'Preview page size', help: 'Rows fetched per scroll in live preview.', min: 10, max: 2000 },
  { key: 'preview_max_buffer_rows', label: 'Preview buffer cap', help: 'Max rows held in memory before preview stops paging.', min: 200, max: 1000000 },
  { key: 'preview_session_ttl_seconds', label: 'Preview idle timeout (s)', help: 'Idle live-preview sessions close after this many seconds.', min: 30, max: 86400 },
]

export default function ProjectSettingsModal({ onClose }: { onClose: () => void }) {
  const projectSettings = usePipelineStore((s) => s.projectSettings)
  const updateProjectSettings = usePipelineStore((s) => s.updateProjectSettings)
  const pipelineId = usePipelineStore((s) => s.pipelineId)
  const hasUnsavedChanges = usePipelineStore((s) => s.hasUnsavedChanges)

  const [storage, setStorage] = useState<number | null>(null)
  const [compacting, setCompacting] = useState(false)
  const [storageError, setStorageError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    getProjectStorage(pipelineId)
      .then((info) => { if (!cancelled) setStorage(info.file_size_bytes) })
      .catch(() => { if (!cancelled) setStorage(null) })
    return () => { cancelled = true }
  }, [pipelineId])

  const handleCompact = async () => {
    setCompacting(true)
    setStorageError(null)
    try {
      const info = await compactProjectStorage(pipelineId)
      setStorage(info.file_size_bytes)
    } catch (err) {
      setStorageError(err instanceof Error ? err.message : 'Compaction failed')
    } finally {
      setCompacting(false)
    }
  }

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
      role="dialog"
      aria-modal="true"
      onClick={onClose}
    >
      <div
        className="max-h-[85vh] w-[520px] overflow-auto rounded-lg bg-white shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between border-b border-gray-200 px-5 py-3">
          <h2 className="text-sm font-semibold text-gray-800">Project settings</h2>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600">✕</button>
        </div>

        <div className="space-y-5 px-5 py-4">
          <section>
            <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-gray-500">Execution &amp; preview</h3>
            <div className="space-y-3">
              {numberFields.map((field) => (
                <label key={field.key} className="block">
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-gray-700">{field.label}</span>
                    <input
                      type="number"
                      min={field.min}
                      max={field.max}
                      value={projectSettings[field.key] as number}
                      onChange={(e) => {
                        const value = Number(e.target.value)
                        if (Number.isFinite(value)) updateProjectSettings({ [field.key]: value } as Partial<ProjectSettings>)
                      }}
                      className="w-28 rounded border border-gray-300 px-2 py-1 text-right text-sm"
                    />
                  </div>
                  <p className="mt-0.5 text-[11px] text-gray-400">{field.help}</p>
                </label>
              ))}
              <label className="block">
                <div className="flex items-center justify-between">
                  <span className="text-sm text-gray-700">DuckDB memory limit</span>
                  <input
                    type="text"
                    value={projectSettings.duckdb_memory_limit}
                    onChange={(e) => updateProjectSettings({ duckdb_memory_limit: e.target.value })}
                    className="w-28 rounded border border-gray-300 px-2 py-1 text-right text-sm"
                  />
                </div>
                <p className="mt-0.5 text-[11px] text-gray-400">e.g. 2GB, 512MB. Shared across concurrent loads.</p>
              </label>
            </div>
            {hasUnsavedChanges && (
              <p className="mt-3 text-[11px] text-amber-600">Settings apply on the next run; save the project to persist them.</p>
            )}
          </section>

          <section className="border-t border-gray-100 pt-4">
            <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-gray-500">Storage</h3>
            <div className="flex items-center justify-between">
              <div className="text-sm text-gray-700">
                Project database size:{' '}
                <span className="font-mono">{storage != null ? formatBytes(storage) : '—'}</span>
              </div>
              <button
                onClick={handleCompact}
                disabled={compacting}
                className="rounded border border-gray-300 px-3 py-1 text-sm hover:bg-gray-50 disabled:opacity-50"
              >
                {compacting ? 'Compacting…' : 'Compact'}
              </button>
            </div>
            <p className="mt-1 text-[11px] text-gray-400">
              Compaction rewrites the file to reclaim space from dropped/replaced tables. DuckDB files never shrink on their own.
            </p>
            {storageError && <p className="mt-1 text-[11px] text-red-600">{storageError}</p>}
          </section>
        </div>

        <div className="flex justify-end border-t border-gray-200 px-5 py-3">
          <button onClick={onClose} className="rounded bg-stone-900 px-4 py-1.5 text-sm text-white hover:bg-stone-800">
            Done
          </button>
        </div>
      </div>
    </div>
  )
}
