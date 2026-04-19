import { useEffect, useState } from 'react'
import axios from 'axios'
import SavedConnectionModal from '../connections/SavedConnectionModal'
import {
  defaultConnectionConfig,
  getConnectionSummary,
  makeSavedConnectionDraft,
  savedConnectionToInput,
} from '../../lib/databaseConnections'
import { useSettingsStore } from '../../store/settingsStore'
import type {
  DbType,
  SavedDatabaseConnection,
  SavedDatabaseConnectionInput,
} from '../../types/pipeline'

function getErrorMessage(error: unknown, fallback: string): string {
  if (axios.isAxiosError(error) && typeof error.response?.data?.detail === 'string') {
    return error.response.data.detail
  }
  return error instanceof Error ? error.message : fallback
}

export default function PlatformSettingsPage() {
  const globalDatabaseConnections = useSettingsStore((s) => s.globalDatabaseConnections)
  const globalConnectionsLoading = useSettingsStore((s) => s.globalConnectionsLoading)
  const loadGlobalDatabaseConnections = useSettingsStore((s) => s.loadGlobalDatabaseConnections)
  const createGlobalDatabaseConnection = useSettingsStore((s) => s.createGlobalDatabaseConnection)
  const updateGlobalDatabaseConnection = useSettingsStore((s) => s.updateGlobalDatabaseConnection)
  const deleteGlobalDatabaseConnection = useSettingsStore((s) => s.deleteGlobalDatabaseConnection)

  const [modalOpen, setModalOpen] = useState(false)
  const [editingId, setEditingId] = useState<string | null>(null)
  const [draft, setDraft] = useState<SavedDatabaseConnectionInput>(makeSavedConnectionDraft())
  const [error, setError] = useState<string | null>(null)
  const [pageError, setPageError] = useState<string | null>(null)

  useEffect(() => {
    void loadGlobalDatabaseConnections().catch((loadError) => {
      setPageError(getErrorMessage(loadError, 'Unable to load global database connections'))
    })
  }, [loadGlobalDatabaseConnections])

  const closeModal = () => {
    setModalOpen(false)
    setEditingId(null)
    setDraft(makeSavedConnectionDraft())
    setError(null)
  }

  const openAddModal = () => {
    setEditingId(null)
    setDraft(makeSavedConnectionDraft())
    setError(null)
    setModalOpen(true)
  }

  const openEditModal = (connection: SavedDatabaseConnection) => {
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

  const handleSave = async () => {
    const name = draft.name.trim()
    if (!name) {
      setError('Connection name is required')
      return
    }

    const duplicate = globalDatabaseConnections.some((connection) =>
      connection.id !== editingId && connection.name.trim().toLowerCase() === name.toLowerCase()
    )
    if (duplicate) {
      setError('Connection name must be unique across global connections')
      return
    }

    try {
      const payload = { ...draft, name } as SavedDatabaseConnectionInput
      if (editingId) {
        await updateGlobalDatabaseConnection(editingId, payload)
      } else {
        await createGlobalDatabaseConnection(payload)
      }
      closeModal()
      setPageError(null)
    } catch (saveError) {
      setError(getErrorMessage(saveError, 'Unable to save global database connection'))
    }
  }

  const handleDelete = async (connection: SavedDatabaseConnection) => {
    try {
      await deleteGlobalDatabaseConnection(connection.id)
      setPageError(null)
    } catch (deleteError) {
      setPageError(getErrorMessage(deleteError, `Unable to delete "${connection.name}"`))
    }
  }

  return (
    <main className="h-full overflow-y-auto bg-stone-100 px-4 py-6 md:px-8">
      <div className="mx-auto max-w-5xl">
        <div className="rounded-[2rem] border border-stone-200 bg-white px-6 py-6 shadow-sm">
          <div className="flex flex-col gap-4 border-b border-stone-200 pb-6 md:flex-row md:items-end md:justify-between">
            <div>
              <div className="text-[11px] font-semibold uppercase tracking-[0.24em] text-stone-400">Platform Settings</div>
              <h1 className="mt-2 font-serif text-3xl text-stone-900">Global Database Connections</h1>
              <p className="mt-2 max-w-2xl text-sm text-stone-600">
                Configure reusable database sources for the entire application. Every project can see and use these global connections.
              </p>
            </div>
            <button
              type="button"
              onClick={openAddModal}
              className="rounded-lg bg-stone-900 px-4 py-2 text-sm font-medium text-white hover:bg-stone-700"
            >
              Add Global Connection
            </button>
          </div>

          {pageError && (
            <div className="mt-4 rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
              {pageError}
            </div>
          )}

          <div className="mt-6 space-y-3">
            {globalConnectionsLoading ? (
              <div className="rounded-2xl border border-dashed border-stone-300 bg-stone-50 px-4 py-6 text-sm text-stone-500">
                Loading global database connections...
              </div>
            ) : globalDatabaseConnections.length === 0 ? (
              <div className="rounded-2xl border border-dashed border-stone-300 bg-stone-50 px-4 py-6 text-sm text-stone-500">
                No global database connections yet.
              </div>
            ) : (
              globalDatabaseConnections.map((connection) => (
                <div
                  key={connection.id}
                  className="flex flex-col gap-4 rounded-2xl border border-stone-200 bg-stone-50/60 px-4 py-4 md:flex-row md:items-center md:justify-between"
                >
                  <div className="min-w-0">
                    <div className="flex items-center gap-2">
                      <h2 className="truncate text-base font-semibold text-stone-900">{connection.name}</h2>
                      <span className="rounded bg-stone-200 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-stone-600">
                        Global
                      </span>
                    </div>
                    <div className="mt-1 text-sm text-stone-600">{getConnectionSummary(connection.db_type, connection)}</div>
                  </div>

                  <div className="flex shrink-0 items-center gap-2">
                    <button
                      type="button"
                      onClick={() => openEditModal(connection)}
                      className="rounded border border-stone-300 px-3 py-1.5 text-sm text-stone-700 hover:bg-white"
                    >
                      Edit
                    </button>
                    <button
                      type="button"
                      onClick={() => { void handleDelete(connection) }}
                      className="rounded border border-red-200 px-3 py-1.5 text-sm text-red-600 hover:bg-red-50"
                    >
                      Delete
                    </button>
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
      </div>

      <SavedConnectionModal
        open={modalOpen}
        editing={Boolean(editingId)}
        scopeLabel="Platform Setting"
        title="Add Global Connection"
        description="Save a reusable global database connection for the entire application. Projects will be able to use it immediately."
        draft={draft}
        error={error}
        onDraftChange={updateDraft}
        onDbTypeChange={handleDbTypeChange}
        onClose={closeModal}
        onSave={() => { void handleSave() }}
        testId="global-connection-modal"
      />
    </main>
  )
}
