import { useEffect, useRef, type ChangeEvent } from 'react'
import { uploadCsv } from '../../api/client'
import { defaultConnectionConfig } from '../../lib/databaseConnections'
import { usePipelineStore } from '../../store/pipelineStore'
import type {
  CsvPreprocessingConfig,
  CsvSourceConfig,
  DatabaseConnectionConfig,
  DbType,
  ExportConfig,
} from '../../types/pipeline'
import ConnectionForm from './ConnectionForm'
import SqlEditor from './SqlEditor'

function getNodeTypeTitle(type: string): string {
  switch (type) {
    case 'csv_source':
      return 'CSV Source'
    case 'db_source':
      return 'Database Source'
    case 'transform':
      return 'Transform'
    case 'export':
      return 'Export'
    default:
      return 'Node'
  }
}

export default function NodeEditorModal() {
  const nodeEditorMode = usePipelineStore((s) => s.nodeEditorMode)
  const draft = usePipelineStore((s) => s.nodeEditorDraft)
  const editingNodeId = usePipelineStore((s) => s.editingNodeId)
  const nodes = usePipelineStore((s) => s.nodes)
  const edges = usePipelineStore((s) => s.edges)
  const updateNodeEditorDraft = usePipelineStore((s) => s.updateNodeEditorDraft)
  const closeNodeEditor = usePipelineStore((s) => s.closeNodeEditor)
  const commitNodeEditor = usePipelineStore((s) => s.commitNodeEditor)
  const fileInputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    if (nodeEditorMode === 'closed') return

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        closeNodeEditor()
      }
    }

    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [closeNodeEditor, nodeEditorMode])

  if (nodeEditorMode === 'closed' || !draft) return null

  const isCreateMode = nodeEditorMode === 'create'
  const title = getNodeTypeTitle(draft.type)
  const csvConfig = draft.type === 'csv_source' ? (draft.config as unknown as CsvSourceConfig) : null
  const dbConfig = draft.type === 'db_source' ? (draft.config as Record<string, unknown>) : null
  const transformConfig = draft.type === 'transform' ? (draft.config as Record<string, unknown>) : null
  const exportConfig = draft.type === 'export' ? (draft.config as unknown as ExportConfig) : null
  const csvPreprocessing: CsvPreprocessingConfig = csvConfig?.preprocessing ?? {
    enabled: false,
    runtime: 'python',
    script: '',
  }
  const dbType = ((dbConfig?.db_type as string | undefined) ?? 'postgres') as DbType
  const dbConnection = (dbConfig?.connection as DatabaseConnectionConfig | undefined) ?? defaultConnectionConfig(dbType)
  const targetNodeId = editingNodeId ?? draft.id
  const upstreamTableNames = edges
    .filter((edge) => edge.target === targetNodeId)
    .map((edge) => nodes.find((node) => node.id === edge.source))
    .filter((node): node is (typeof nodes)[number] => Boolean(node))
    .map((node) => ((node.data as Record<string, unknown>).tableName as string | undefined) ?? '')
    .filter(Boolean)

  const updateConfig = (config: Record<string, unknown>) => {
    updateNodeEditorDraft({ config })
  }

  const handleCsvUpload = async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (!file || !csvConfig) return

    const result = await uploadCsv(file)
    updateConfig({
      ...csvConfig,
      file_path: result.file_path,
      original_filename: result.filename,
      preprocessing: csvConfig.preprocessing ?? csvPreprocessing,
    })
  }

  return (
    <div className="fixed inset-0 z-[70] flex items-center justify-center bg-stone-950/30 px-4" data-testid="node-editor-modal">
      <button
        type="button"
        aria-label="Discard node changes"
        onClick={closeNodeEditor}
        className="absolute inset-0 cursor-default"
      />
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="node-editor-modal-title"
        className="relative z-10 flex max-h-[calc(100vh-3rem)] w-full max-w-3xl flex-col overflow-hidden rounded-3xl border border-stone-200 bg-white shadow-[0_30px_80px_rgba(15,23,42,0.24)]"
      >
        <div className="border-b border-stone-200 px-6 py-5">
          <div className="text-[11px] font-semibold uppercase tracking-[0.24em] text-stone-400">{title}</div>
          <h3 id="node-editor-modal-title" className="mt-2 text-xl font-semibold text-stone-900">
            {isCreateMode ? `Create ${title}` : `Edit ${title}`}
          </h3>
          <p className="mt-2 text-sm text-stone-500">
            {isCreateMode
              ? 'Review and adjust the node configuration before it is added to the canvas.'
              : 'Update this node configuration before saving the changes.'}
          </p>
        </div>

        <div className="min-h-0 space-y-6 overflow-y-auto px-6 py-5">
          <div className="grid gap-4 md:grid-cols-2">
            <div>
              <label htmlFor="node-editor-label" className="mb-1 block text-xs text-gray-500">
                Label
              </label>
              <input
                id="node-editor-label"
                type="text"
                value={draft.label}
                onChange={(event) => updateNodeEditorDraft({ label: event.target.value })}
                className="w-full rounded border border-gray-300 px-2 py-1 text-sm"
              />
            </div>

            <div>
              <label htmlFor="node-editor-table-name" className="mb-1 block text-xs text-gray-500">
                Table Name
              </label>
              <input
                id="node-editor-table-name"
                type="text"
                value={draft.tableName}
                onChange={(event) => updateNodeEditorDraft({ tableName: event.target.value })}
                className="w-full rounded border border-gray-300 px-2 py-1 text-sm font-mono"
              />
            </div>
          </div>

          {draft.type === 'csv_source' && csvConfig && (
            <div className="space-y-4">
              <div>
                <label className="mb-2 block text-xs text-gray-500">CSV File</label>
                <input
                  ref={fileInputRef}
                  type="file"
                  accept=".csv"
                  onChange={handleCsvUpload}
                  className="hidden"
                />
                <button
                  type="button"
                  onClick={() => fileInputRef.current?.click()}
                  className="w-full rounded-lg border-2 border-dashed border-gray-300 p-4 text-sm text-gray-500 transition hover:border-blue-400 hover:text-blue-500"
                >
                  {csvConfig.original_filename || 'Click to upload CSV'}
                </button>
              </div>

              <div>
                <div className="mb-2 flex items-center justify-between">
                  <label className="block text-xs text-gray-500">Preprocessing</label>
                  <button
                    type="button"
                    role="switch"
                    aria-checked={csvPreprocessing.enabled}
                    aria-label="Enable preprocessing"
                    onClick={() => updateConfig({
                      ...csvConfig,
                      preprocessing: {
                        ...csvPreprocessing,
                        enabled: !csvPreprocessing.enabled,
                      },
                    })}
                    className={`relative inline-flex h-6 w-11 shrink-0 items-center rounded-full px-0.5 transition ${
                      csvPreprocessing.enabled ? 'bg-blue-500' : 'bg-gray-300'
                    }`}
                  >
                    <span
                      className={`h-5 w-5 rounded-full bg-white shadow-sm transition ${
                        csvPreprocessing.enabled ? 'translate-x-5' : 'translate-x-0'
                      }`}
                    />
                  </button>
                </div>

                {csvPreprocessing.enabled && (
                  <div className="space-y-3 rounded-lg border border-gray-200 bg-gray-50 p-3">
                    <div>
                      <label htmlFor="node-editor-csv-runtime" className="mb-1 block text-xs text-gray-500">
                        Runtime
                      </label>
                      <select
                        id="node-editor-csv-runtime"
                        value={csvPreprocessing.runtime}
                        onChange={(event) => updateConfig({
                          ...csvConfig,
                          preprocessing: {
                            ...csvPreprocessing,
                            runtime: event.target.value as CsvPreprocessingConfig['runtime'],
                          },
                        })}
                        className="w-full rounded border border-gray-300 px-2 py-1 text-sm"
                      >
                        <option value="python">Python</option>
                        <option value="bash">Bash</option>
                      </select>
                    </div>
                    <div>
                      <label htmlFor="node-editor-csv-script" className="mb-1 block text-xs text-gray-500">
                        Script
                      </label>
                      <textarea
                        id="node-editor-csv-script"
                        value={csvPreprocessing.script}
                        onChange={(event) => updateConfig({
                          ...csvConfig,
                          preprocessing: {
                            ...csvPreprocessing,
                            script: event.target.value,
                          },
                        })}
                        rows={8}
                        spellCheck={false}
                        className="w-full rounded border border-gray-300 bg-white px-2 py-1.5 text-sm font-mono"
                      />
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}

          {draft.type === 'db_source' && dbConfig && (
            <div className="space-y-4">
              <div>
                <label htmlFor="node-editor-db-type" className="mb-1 block text-xs text-gray-500">
                  Database Type
                </label>
                <select
                  id="node-editor-db-type"
                  value={dbType}
                  onChange={(event) => updateConfig({
                    ...dbConfig,
                    db_type: event.target.value as DbType,
                    connection: defaultConnectionConfig(event.target.value as DbType),
                  })}
                  className="w-full rounded border border-gray-300 px-2 py-1 text-sm"
                >
                  <option value="postgres">PostgreSQL</option>
                  <option value="oracle">Oracle</option>
                </select>
              </div>

              <ConnectionForm
                config={dbConnection}
                onChange={(connection) => updateConfig({
                  ...dbConfig,
                  db_type: dbType,
                  connection,
                })}
                dbType={dbType}
              />

              <div>
                <label className="mb-2 block text-xs font-semibold uppercase tracking-wide text-gray-500">
                  SQL Query
                </label>
                <SqlEditor
                  value={(dbConfig.query as string | undefined) ?? ''}
                  onChange={(query) => updateConfig({ ...dbConfig, query })}
                  upstreamTables={[]}
                />
              </div>
            </div>
          )}

          {draft.type === 'transform' && transformConfig && (
            <div>
              <label className="mb-2 block text-xs font-semibold uppercase tracking-wide text-gray-500">
                SQL Query
              </label>
              <SqlEditor
                value={(transformConfig.sql as string | undefined) ?? ''}
                onChange={(sql) => updateConfig({ ...transformConfig, sql })}
                upstreamTables={upstreamTableNames}
              />
            </div>
          )}

          {draft.type === 'export' && exportConfig && (
            <div>
              <label htmlFor="node-editor-export-format" className="mb-1 block text-xs text-gray-500">
                Export Format
              </label>
              <select
                id="node-editor-export-format"
                value={exportConfig.format ?? 'csv'}
                onChange={(event) => updateConfig({ ...exportConfig, format: event.target.value })}
                className="w-full rounded border border-gray-300 px-2 py-1 text-sm"
              >
                <option value="csv">CSV</option>
              </select>
            </div>
          )}
        </div>

        <div className="flex justify-end gap-2 border-t border-stone-200 px-6 py-4">
          <button
            type="button"
            onClick={closeNodeEditor}
            className="rounded border border-gray-300 px-3 py-1 text-sm text-gray-600 hover:bg-gray-50"
          >
            Discard
          </button>
          <button
            type="button"
            onClick={commitNodeEditor}
            className="rounded bg-stone-900 px-3 py-1 text-sm text-white hover:bg-stone-700"
          >
            {isCreateMode ? 'Create' : 'Save'}
          </button>
        </div>
      </div>
    </div>
  )
}
