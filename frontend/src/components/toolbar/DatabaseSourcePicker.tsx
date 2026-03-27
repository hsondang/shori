import { useEffect, useRef, useState, type DragEvent } from 'react'
import { usePipelineStore } from '../../store/pipelineStore'
import ConnectionForm from '../panels/ConnectionForm'
import { defaultConnectionConfig, getConnectionSummary, makeSavedConnectionDraft } from '../../lib/databaseConnections'
import { DATABASE_CONNECTION_MIME, NODE_TYPE_MIME } from '../../lib/dragData'
import type {
  DatabaseConnectionConfig,
  DbType,
  SavedDatabaseConnection,
  SavedDatabaseConnectionInput,
} from '../../types/pipeline'

function getDraftConnectionConfig(draft: SavedDatabaseConnectionInput): DatabaseConnectionConfig {
  if (draft.db_type === 'oracle') {
    return {
      host: draft.host,
      port: draft.port,
      service_name: draft.service_name,
      user: draft.user,
      password: draft.password,
    }
  }

  return {
    host: draft.host,
    port: draft.port,
    database: draft.database,
    user: draft.user,
    password: draft.password,
  }
}

export default function DatabaseSourcePicker() {
  const databaseConnections = usePipelineStore((s) => s.databaseConnections)
  const addDatabaseConnection = usePipelineStore((s) => s.addDatabaseConnection)
  const updateDatabaseConnection = usePipelineStore((s) => s.updateDatabaseConnection)
  const deleteDatabaseConnection = usePipelineStore((s) => s.deleteDatabaseConnection)
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
    setDraft(
      connection.db_type === 'oracle'
        ? {
            name: connection.name,
            db_type: 'oracle',
            host: connection.host,
            port: connection.port,
            service_name: connection.service_name,
            user: connection.user,
            password: connection.password,
          }
        : {
            name: connection.name,
            db_type: 'postgres',
            host: connection.host,
            port: connection.port,
            database: connection.database,
            user: connection.user,
            password: connection.password,
          }
    )
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
      setError('Connection name must be unique within this pipeline')
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

  const handleDragStart = (event: DragEvent, connectionId: string) => {
    event.dataTransfer.setData(NODE_TYPE_MIME, 'db_source')
    event.dataTransfer.setData(DATABASE_CONNECTION_MIME, connectionId)
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
          className="absolute top-full left-0 mt-2 w-96 rounded-lg border border-gray-200 bg-white p-3 shadow-lg z-50"
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

          <div className="mb-3 space-y-2">
            {databaseConnections.length === 0 ? (
              <div className="rounded border border-dashed border-gray-300 px-3 py-4 text-sm text-gray-500">
                No saved database connections yet.
              </div>
            ) : (
              databaseConnections.map((connection) => (
                <div
                  key={connection.id}
                  draggable
                  onDragStart={(event) => handleDragStart(event, connection.id)}
                  className="flex items-center justify-between gap-3 rounded border border-gray-200 px-3 py-2 cursor-grab active:cursor-grabbing"
                >
                  <div className="min-w-0">
                    <div className="truncate text-sm font-medium text-gray-800">{connection.name}</div>
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

          <div className="border-t border-gray-200 pt-3">
            <button
              type="button"
              onClick={startAdd}
              className="w-full rounded-lg border border-dashed border-indigo-300 px-3 py-2 text-sm font-medium text-indigo-700 transition hover:border-indigo-400 hover:bg-indigo-50"
            >
              Add
            </button>
          </div>
        </div>
      )}

      {modalOpen && (
        <div className="fixed inset-0 z-[60] flex items-center justify-center bg-stone-950/30 px-4" data-testid="database-source-modal">
          <button
            type="button"
            aria-label="Discard connection changes"
            onClick={resetForm}
            className="absolute inset-0 cursor-default"
          />
          <div
            role="dialog"
            aria-modal="true"
            aria-labelledby="database-connection-modal-title"
            className="relative z-10 w-full max-w-lg rounded-3xl border border-stone-200 bg-white p-6 shadow-[0_30px_80px_rgba(15,23,42,0.24)]"
          >
            <div className="mb-5">
              <div className="text-[11px] font-semibold uppercase tracking-[0.24em] text-stone-400">Database Source</div>
              <h3 id="database-connection-modal-title" className="mt-2 text-xl font-semibold text-stone-900">
                {editingId ? 'Edit connection' : 'Add a connection'}
              </h3>
              <p className="mt-2 text-sm text-stone-500">
                Save reusable database connections here, then drag them onto the canvas when you need them.
              </p>
            </div>

            <div className="space-y-3">
              <div>
                <label htmlFor="database-connection-name" className="mb-1 block text-xs text-gray-500">
                  Connection Name
                </label>
                <input
                  id="database-connection-name"
                  type="text"
                  value={draft.name}
                  onChange={(event) => updateDraft({ name: event.target.value })}
                  className="w-full rounded border border-gray-300 px-2 py-1 text-sm"
                />
              </div>

              <div>
                <label htmlFor="database-connection-db-type" className="mb-1 block text-xs text-gray-500">
                  Database Type
                </label>
                <select
                  id="database-connection-db-type"
                  value={draft.db_type}
                  onChange={(event) => handleDbTypeChange(event.target.value as DbType)}
                  className="w-full rounded border border-gray-300 px-2 py-1 text-sm"
                >
                  <option value="postgres">PostgreSQL</option>
                  <option value="oracle">Oracle</option>
                </select>
              </div>

              <ConnectionForm
                config={getDraftConnectionConfig(draft)}
                onChange={(config) => updateDraft(config)}
                dbType={draft.db_type}
              />

              {error && <div className="text-xs text-red-600">{error}</div>}

              <div className="flex justify-end gap-2 pt-2">
                <button
                  type="button"
                  onClick={resetForm}
                  className="rounded border border-gray-300 px-3 py-1 text-sm text-gray-600 hover:bg-gray-50"
                >
                  Discard
                </button>
                <button
                  type="button"
                  onClick={handleSave}
                  className="rounded bg-indigo-500 px-3 py-1 text-sm text-white hover:bg-indigo-600"
                >
                  Save
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
