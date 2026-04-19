import ConnectionForm from '../panels/ConnectionForm'
import {
  getDraftConnectionConfig,
} from '../../lib/databaseConnections'
import type { DbType, SavedDatabaseConnectionInput } from '../../types/pipeline'

interface SavedConnectionModalProps {
  open: boolean
  editing: boolean
  scopeLabel: string
  title: string
  description: string
  draft: SavedDatabaseConnectionInput
  error: string | null
  onDraftChange: (changes: Partial<SavedDatabaseConnectionInput>) => void
  onDbTypeChange: (dbType: DbType) => void
  onClose: () => void
  onSave: () => void
  testId?: string
}

export default function SavedConnectionModal({
  open,
  editing,
  scopeLabel,
  title,
  description,
  draft,
  error,
  onDraftChange,
  onDbTypeChange,
  onClose,
  onSave,
  testId,
}: SavedConnectionModalProps) {
  if (!open) return null

  return (
    <div className="fixed inset-0 z-[60] flex items-center justify-center bg-stone-950/30 px-4" data-testid={testId}>
      <button
        type="button"
        aria-label="Discard connection changes"
        onClick={onClose}
        className="absolute inset-0 cursor-default"
      />
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="saved-connection-modal-title"
        className="relative z-10 w-full max-w-lg rounded-3xl border border-stone-200 bg-white p-6 shadow-[0_30px_80px_rgba(15,23,42,0.24)]"
      >
        <div className="mb-5">
          <div className="text-[11px] font-semibold uppercase tracking-[0.24em] text-stone-400">{scopeLabel}</div>
          <h3 id="saved-connection-modal-title" className="mt-2 text-xl font-semibold text-stone-900">
            {editing ? title.replace('Add', 'Edit') : title}
          </h3>
          <p className="mt-2 text-sm text-stone-500">{description}</p>
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
              onChange={(event) => onDraftChange({ name: event.target.value })}
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
              onChange={(event) => onDbTypeChange(event.target.value as DbType)}
              className="w-full rounded border border-gray-300 px-2 py-1 text-sm"
            >
              <option value="postgres">PostgreSQL</option>
              <option value="oracle">Oracle</option>
            </select>
          </div>

          <ConnectionForm
            config={getDraftConnectionConfig(draft)}
            onChange={(config) => onDraftChange(config)}
            dbType={draft.db_type}
          />

          {error && <div className="text-xs text-red-600">{error}</div>}

          <div className="flex justify-end gap-2 pt-2">
            <button
              type="button"
              onClick={onClose}
              className="rounded border border-gray-300 px-3 py-1 text-sm text-gray-600 hover:bg-gray-50"
            >
              Discard
            </button>
            <button
              type="button"
              onClick={onSave}
              className="rounded bg-stone-900 px-3 py-1 text-sm text-white hover:bg-stone-700"
            >
              Save
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}
