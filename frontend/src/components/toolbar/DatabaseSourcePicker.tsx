import { useEffect, useRef, useState, type DragEvent } from 'react'
import { useNavigate } from 'react-router-dom'
import { usePipelineStore } from '../../store/pipelineStore'
import { useSettingsStore } from '../../store/settingsStore'
import SavedConnectionModal from '../connections/SavedConnectionModal'
import {
  defaultConnectionConfig,
  getConnectionSummary,
  makeSavedConnectionDraft,
  savedConnectionToInput,
} from '../../lib/databaseConnections'
import {
  DATABASE_CONNECTION_MIME,
  DATABASE_CONNECTION_SCOPE_MIME,
  NODE_TYPE_MIME,
} from '../../lib/dragData'
import type {
  ConnectionScope,
  DbType,
  SavedDatabaseConnection,
  SavedDatabaseConnectionInput,
} from '../../types/pipeline'

export default function DatabaseSourcePicker() {
  const navigate = useNavigate()
  const databaseConnections = usePipelineStore((s) => s.databaseConnections)
  const addDatabaseConnection = usePipelineStore((s) => s.addDatabaseConnection)
  const updateDatabaseConnection = usePipelineStore((s) => s.updateDatabaseConnection)
  const deleteDatabaseConnection = usePipelineStore((s) => s.deleteDatabaseConnection)
  const globalDatabaseConnections = useSettingsStore((s) => s.globalDatabaseConnections)
  const globalConnectionsLoading = useSettingsStore((s) => s.globalConnectionsLoading)
  const loadGlobalDatabaseConnections = useSettingsStore((s) => s.loadGlobalDatabaseConnections)

  const [open, setOpen] = useState(false)
  const [modalOpen, setModalOpen] = useState(false)
  const [editingId, setEditingId] = useState<string | null>(null)
  const [draft, setDraft] = useState<SavedDatabaseConnectionInput>(makeSavedConnectionDraft())
  const [error, setError] = useState<string | null>(null)
  const rootRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!open || modalOpen) return

    const handleClickOutside = (event: MouseEvent) => {
      if (!rootRef.current?.contains(event.target as Node)) {
        setOpen(false)
        resetForm()
      }
    }

    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [modalOpen, open])

  useEffect(() => {
    if (!open) return
    void loadGlobalDatabaseConnections().catch(() => {})
  }, [loadGlobalDatabaseConnections, open])

  const resetForm = () => {
    setEditingId(null)
    setDraft(makeSavedConnectionDraft())
    setError(null)
    setModalOpen(false)
  }

  const startAdd = () => {
    setEditingId(null)
    setDraft(makeSavedConnectionDraft())
    setError(null)
    setModalOpen(true)
  }

  const startEdit = (connection: SavedDatabaseConnection) => {
    setEditingId(connection.id)
    setDraft(savedConnectionToInput(connection))
    setError(null)
    setModalOpen(true)
  }

  const updateDraft = (changes: Partial<SavedDatabaseConnectionInput>) => {
    setDraft((current) => ({ ...current, ...changes } as SavedDatabaseConnectionInput))
  }

  const handleDbTypeChange = (dbType: DbType) => {
    setDraft({
      name: draft.name,
      db_type: dbType,
      ...defaultConnectionConfig(dbType),
    } as SavedDatabaseConnectionInput)
  }

  const handleSave = () => {
    const name = draft.name.trim()
    if (!name) {
      setError('Connection name is required')
      return
    }

    const duplicate = databaseConnections.some((connection) =>
      connection.id !== editingId && connection.name.trim().toLowerCase() === name.toLowerCase()
    )
    if (duplicate) {
      setError('Connection name must be unique within this project')
      return
    }

    const payload = { ...draft, name } as SavedDatabaseConnectionInput
    if (editingId) {
      updateDatabaseConnection(editingId, payload)
    } else {
      addDatabaseConnection(payload)
    }
    resetForm()
  }

  const handleDelete = (id: string) => {
    deleteDatabaseConnection(id)
    if (editingId === id) resetForm()
  }

  const handleDragStart = (event: DragEvent, connectionId: string, scope: ConnectionScope) => {
    event.dataTransfer.setData(NODE_TYPE_MIME, 'db_source')
    event.dataTransfer.setData(DATABASE_CONNECTION_MIME, connectionId)
    event.dataTransfer.setData(DATABASE_CONNECTION_SCOPE_MIME, scope)
    event.dataTransfer.effectAllowed = 'move'
  }

  return (
    <div ref={rootRef} className="relative">
      <button
        type="button"
        onClick={() => {
          if (open) {
            setOpen(false)
            resetForm()
            return
          }
          resetForm()
          setOpen(true)
        }}
        className="border rounded px-2 py-1 text-xs font-medium select-none bg-indigo-100 text-indigo-700 border-indigo-300 hover:bg-indigo-200"
      >
        Database Source
      </button>

      {open && (
        <div
          data-testid="database-source-dropdown"
          className="absolute top-full left-0 mt-2 z-50 w-[28rem] rounded-lg border border-gray-200 bg-white p-3 shadow-lg"
        >
          <div className="mb-3 flex items-center justify-between">
            <h3 className="text-sm font-semibold text-gray-800">Database Connections</h3>
            <button
              type="button"
              onClick={() => {
                setOpen(false)
                resetForm()
              }}
              className="text-xs text-gray-500 hover:text-gray-700"
            >
              Close
            </button>
          </div>

          <div className="space-y-4">
            <section>
              <div className="mb-2 flex items-center justify-between">
                <div>
                  <div className="text-[11px] font-semibold uppercase tracking-[0.24em] text-stone-400">Global</div>
                  <p className="mt-1 text-xs text-stone-500">Available in every project. Managed from Platform Settings.</p>
                </div>
                <button
                  type="button"
                  onClick={() => {
                    setOpen(false)
                    resetForm()
                    navigate('/settings/platform')
                  }}
                  className="rounded border border-stone-300 px-2 py-1 text-xs text-stone-600 hover:bg-stone-50"
                >
                  Manage
                </button>
              </div>

              <div className="space-y-2">
                {globalConnectionsLoading ? (
                  <div className="rounded border border-dashed border-gray-300 px-3 py-4 text-sm text-gray-500">
                    Loading global connections...
                  </div>
                ) : globalDatabaseConnections.length === 0 ? (
                  <div className="rounded border border-dashed border-gray-300 px-3 py-4 text-sm text-gray-500">
                    No global database connections yet.
                  </div>
                ) : (
                  globalDatabaseConnections.map((connection) => (
                    <div
                      key={connection.id}
                      draggable
                      onDragStart={(event) => handleDragStart(event, connection.id, 'global')}
                      className="flex items-center justify-between gap-3 rounded border border-gray-200 px-3 py-2 cursor-grab active:cursor-grabbing"
                    >
                      <div className="min-w-0">
                        <div className="flex items-center gap-2">
                          <div className="truncate text-sm font-medium text-gray-800">{connection.name}</div>
                          <span className="rounded bg-stone-100 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-stone-500">
                            Global
                          </span>
                        </div>
                        <div className="truncate text-xs text-gray-500">
                          {getConnectionSummary(connection.db_type, connection)}
                        </div>
                      </div>
                    </div>
                  ))
                )}
              </div>
            </section>

            <section>
              <div className="mb-2">
                <div className="text-[11px] font-semibold uppercase tracking-[0.24em] text-stone-400">Local</div>
                <p className="mt-1 text-xs text-stone-500">Saved only inside this project.</p>
              </div>

              <div className="space-y-2">
                {databaseConnections.length === 0 ? (
                  <div className="rounded border border-dashed border-gray-300 px-3 py-4 text-sm text-gray-500">
                    No local database connections yet.
                  </div>
                ) : (
                  databaseConnections.map((connection) => (
                    <div
                      key={connection.id}
                      draggable
                      onDragStart={(event) => handleDragStart(event, connection.id, 'local')}
                      className="flex items-center justify-between gap-3 rounded border border-gray-200 px-3 py-2 cursor-grab active:cursor-grabbing"
                    >
                      <div className="min-w-0">
                        <div className="flex items-center gap-2">
                          <div className="truncate text-sm font-medium text-gray-800">{connection.name}</div>
                          <span className="rounded bg-indigo-100 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-indigo-600">
                            Local
                          </span>
                        </div>
                        <div className="truncate text-xs text-gray-500">
                          {getConnectionSummary(connection.db_type, connection)}
                        </div>
                      </div>
                      <div className="flex shrink-0 items-center gap-2">
                        <button
                          type="button"
                          onClick={(event) => {
                            event.stopPropagation()
                            startEdit(connection)
                          }}
                          className="text-xs text-blue-600 hover:text-blue-800"
                        >
                          Edit
                        </button>
                        <button
                          type="button"
                          onClick={(event) => {
                            event.stopPropagation()
                            handleDelete(connection.id)
                          }}
                          className="text-xs text-red-500 hover:text-red-700"
                        >
                          Delete
                        </button>
                      </div>
                    </div>
                  ))
                )}
              </div>

              <div className="border-t border-gray-200 pt-3 mt-3">
                <button
                  type="button"
                  onClick={startAdd}
                  className="w-full rounded-lg border border-dashed border-indigo-300 px-3 py-2 text-sm font-medium text-indigo-700 transition hover:border-indigo-400 hover:bg-indigo-50"
                >
                  Add Local Connection
                </button>
              </div>
            </section>
          </div>
        </div>
      )}

      <SavedConnectionModal
        open={modalOpen}
        editing={Boolean(editingId)}
        scopeLabel="Local Database Source"
        title="Add Local Connection"
        description="Save a reusable local database connection for this project, then drag it onto the canvas when you need it."
        draft={draft}
        error={error}
        onDraftChange={updateDraft}
        onDbTypeChange={handleDbTypeChange}
        onClose={resetForm}
        onSave={handleSave}
        testId="database-source-modal"
      />
    </div>
  )
}
