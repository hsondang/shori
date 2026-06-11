import { useEffect, useRef, type ChangeEvent } from 'react'
import { materializeExcelSheet, uploadCsv } from '../../api/client'
import { createExcelUploadHandler } from '../../lib/excelUpload'
import {
  defaultConnectionConfig,
  defaultOracleFetchConfig,
  findSavedConnectionById,
  getConnectionSummary,
  getDatabaseSourceConnectionScope,
  getDatabaseSourceConnectionSourceId,
  switchDatabaseSourceConfigDbType,
} from '../../lib/databaseConnections'
import { usePipelineStore } from '../../store/pipelineStore'
import { useSettingsStore } from '../../store/settingsStore'
import type {
  CsvPreprocessingConfig,
  CsvSourceConfig,
  DatabaseSourceConfig,
  DbType,
  ExcelSheetPreview,
  ExcelSourceConfig,
  ExportConfig,
  OracleConnectionConfig,
  OracleFetchConfig,
  PostgresConnectionConfig,
} from '../../types/pipeline'
import ConnectionForm from './ConnectionForm'
import SqlEditor from './SqlEditor'

function getNodeTypeTitle(type: string): string {
  switch (type) {
    case 'csv_source':
      return 'CSV Source'
    case 'excel_source':
      return 'Excel Source'
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

function getDatabaseSourceConfig(config: Record<string, unknown>): DatabaseSourceConfig {
  if (config.connection_mode === 'global') {
    if (config.db_type === 'oracle') {
      const fetchConfig = config.fetch_config as Partial<OracleFetchConfig> | undefined
      return {
        connection_mode: 'global',
        connection_source_id: (config.connection_source_id as string | undefined) ?? '',
        db_type: 'oracle',
        query: (config.query as string | undefined) ?? '',
        fetch_config: {
          mode: fetchConfig?.mode === 'fetchmany' ? 'fetchmany' : 'fetchall',
          arraysize: Number.isInteger(fetchConfig?.arraysize) ? Number(fetchConfig?.arraysize) : 100,
          prefetchrows: Number.isInteger(fetchConfig?.prefetchrows) ? Number(fetchConfig?.prefetchrows) : 2,
        },
      }
    }

    return {
      connection_mode: 'global',
      connection_source_id: (config.connection_source_id as string | undefined) ?? '',
      db_type: 'postgres',
      query: (config.query as string | undefined) ?? '',
    }
  }

  if (config.db_type === 'oracle') {
    const fetchConfig = config.fetch_config as Partial<OracleFetchConfig> | undefined
    return {
      connection_mode: 'local',
      db_type: 'oracle',
      connection: (config.connection as OracleConnectionConfig | undefined) ?? defaultConnectionConfig('oracle') as OracleConnectionConfig,
      query: (config.query as string | undefined) ?? '',
      fetch_config: {
        mode: fetchConfig?.mode === 'fetchmany' ? 'fetchmany' : 'fetchall',
        arraysize: Number.isInteger(fetchConfig?.arraysize) ? Number(fetchConfig?.arraysize) : 100,
        prefetchrows: Number.isInteger(fetchConfig?.prefetchrows) ? Number(fetchConfig?.prefetchrows) : 2,
      },
    }
  }

  return {
    connection_mode: 'local',
    db_type: 'postgres',
    connection: (config.connection as PostgresConnectionConfig | undefined) ?? defaultConnectionConfig('postgres') as PostgresConnectionConfig,
    query: (config.query as string | undefined) ?? '',
  }
}

function getOracleFetchConfigError(fetchConfig: OracleFetchConfig): string | null {
  if (!Number.isInteger(fetchConfig.arraysize) || fetchConfig.arraysize < 1) {
    return 'Arraysize must be an integer greater than or equal to 1.'
  }
  if (!Number.isInteger(fetchConfig.prefetchrows) || fetchConfig.prefetchrows < 0) {
    return 'Prefetchrows must be an integer greater than or equal to 0.'
  }
  return null
}

function ExcelSheetPreviewGrid({ sheet }: { sheet: ExcelSheetPreview | null }) {
  if (!sheet) {
    return (
      <div className="rounded-lg border border-gray-200 bg-gray-50 px-3 py-3 text-sm text-gray-400">
        Select a sheet to preview its first cells.
      </div>
    )
  }

  return (
    <div className="overflow-auto rounded-lg border border-gray-200 bg-white">
      <table className="min-w-full border-collapse text-xs">
        <tbody>
          {sheet.rows.length === 0 ? (
            <tr>
              <td className="px-3 py-3 text-gray-400">This sheet is empty.</td>
            </tr>
          ) : sheet.rows.map((row, rowIndex) => (
            <tr key={rowIndex}>
              <th className="sticky left-0 border-b border-r border-gray-100 bg-gray-50 px-2 py-1 text-right font-normal text-gray-400">
                {rowIndex + 1}
              </th>
              {row.map((cell, cellIndex) => (
                <td key={`${rowIndex}-${cellIndex}`} className="max-w-36 truncate border-b border-r border-gray-100 px-2 py-1 text-gray-700">
                  {cell || <span className="text-gray-300">""</span>}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
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
  const globalDatabaseConnections = useSettingsStore((s) => s.globalDatabaseConnections)
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
  const excelConfig = draft.type === 'excel_source' ? (draft.config as unknown as ExcelSourceConfig) : null
  const dbConfig = draft.type === 'db_source' ? getDatabaseSourceConfig(draft.config as Record<string, unknown>) : null
  const transformConfig = draft.type === 'transform' ? (draft.config as Record<string, unknown>) : null
  const exportConfig = draft.type === 'export' ? (draft.config as unknown as ExportConfig) : null
  const csvPreprocessing: CsvPreprocessingConfig = (csvConfig?.preprocessing ?? excelConfig?.preprocessing) ?? {
    enabled: false,
    runtime: 'python',
    script: '',
  }
  const selectedExcelSheetPreview = excelConfig?.sheets?.find((sheet) => sheet.name === excelConfig.selected_sheet) ?? null
  const dbType = dbConfig?.db_type ?? 'postgres'
  const dbConnectionScope = dbConfig
    ? getDatabaseSourceConnectionScope(dbConfig as unknown as Record<string, unknown>)
    : 'local'
  const globalConnection = dbConfig
    ? findSavedConnectionById(
        globalDatabaseConnections,
        getDatabaseSourceConnectionSourceId(dbConfig as unknown as Record<string, unknown>),
      )
    : null
  const localDbConnection = (
    dbConfig && dbConnectionScope !== 'global' && 'connection' in dbConfig
      ? dbConfig.connection
      : defaultConnectionConfig(dbType)
  )
  const dbConnection = dbConnectionScope === 'global'
    ? globalConnection
    : localDbConnection
  const oracleFetchConfig = dbType === 'oracle'
    ? (dbConfig && 'fetch_config' in dbConfig ? dbConfig.fetch_config : undefined) ?? defaultOracleFetchConfig()
    : null
  const oracleFetchError = oracleFetchConfig ? getOracleFetchConfigError(oracleFetchConfig) : null
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

  const handleExcelUpload = createExcelUploadHandler({
    excelConfig,
    csvPreprocessing,
    applyConfig: (config) => updateConfig({ ...config }),
  })

  const handleExcelSheetSelect = async (sheetName: string) => {
    if (!excelConfig?.file_path) return
    if (!sheetName) {
      updateConfig({
        ...excelConfig,
        selected_sheet: '',
        materialized_csv_path: '',
        materialized_csv_filename: '',
        preprocessing: excelConfig.preprocessing ?? csvPreprocessing,
      })
      return
    }

    updateConfig({
      ...excelConfig,
      selected_sheet: sheetName,
      materialized_csv_path: '',
      materialized_csv_filename: '',
      preprocessing: excelConfig.preprocessing ?? csvPreprocessing,
    })
    const result = await materializeExcelSheet(excelConfig.file_path, sheetName)
    updateConfig({
      ...excelConfig,
      selected_sheet: sheetName,
      materialized_csv_path: result.file_path,
      materialized_csv_filename: result.filename,
      preprocessing: excelConfig.preprocessing ?? csvPreprocessing,
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

          {draft.type === 'excel_source' && excelConfig && (
            <div className="space-y-4">
              <div>
                <label className="mb-2 block text-xs text-gray-500">Excel Workbook</label>
                <input
                  ref={fileInputRef}
                  type="file"
                  accept=".xls,.xlsx,.xlsm,.xlsb,.ods"
                  onChange={handleExcelUpload}
                  className="hidden"
                />
                <button
                  type="button"
                  onClick={() => fileInputRef.current?.click()}
                  className="w-full rounded-lg border-2 border-dashed border-gray-300 p-4 text-sm text-gray-500 transition hover:border-emerald-400 hover:text-emerald-600"
                >
                  {excelConfig.original_filename || 'Click to upload Excel workbook'}
                </button>
              </div>

              {excelConfig.sheet_names.length > 0 && (
                <div>
                  <label htmlFor="node-editor-excel-sheet" className="mb-1 block text-xs text-gray-500">
                    Sheet
                  </label>
                  <select
                    id="node-editor-excel-sheet"
                    value={excelConfig.selected_sheet}
                    onChange={(event) => { void handleExcelSheetSelect(event.target.value) }}
                    className="w-full rounded border border-gray-300 px-2 py-1 text-sm"
                  >
                    <option value="">Select a sheet</option>
                    {excelConfig.sheet_names.map((sheetName) => (
                      <option key={sheetName} value={sheetName}>{sheetName}</option>
                    ))}
                  </select>
                  <div className="mt-3">
                    <ExcelSheetPreviewGrid sheet={selectedExcelSheetPreview} />
                  </div>
                </div>
              )}

              <div>
                <div className="mb-2 flex items-center justify-between">
                  <label className="block text-xs text-gray-500">Preprocessing</label>
                  <button
                    type="button"
                    role="switch"
                    aria-checked={csvPreprocessing.enabled}
                    aria-label="Enable preprocessing"
                    onClick={() => updateConfig({
                      ...excelConfig,
                      preprocessing: {
                        ...csvPreprocessing,
                        enabled: !csvPreprocessing.enabled,
                      },
                    })}
                    className={`relative inline-flex h-6 w-11 shrink-0 items-center rounded-full px-0.5 transition ${
                      csvPreprocessing.enabled ? 'bg-emerald-500' : 'bg-gray-300'
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
                      <label htmlFor="node-editor-excel-runtime" className="mb-1 block text-xs text-gray-500">
                        Runtime
                      </label>
                      <select
                        id="node-editor-excel-runtime"
                        value={csvPreprocessing.runtime}
                        onChange={(event) => updateConfig({
                          ...excelConfig,
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
                      <label htmlFor="node-editor-excel-script" className="mb-1 block text-xs text-gray-500">
                        Script
                      </label>
                      <textarea
                        id="node-editor-excel-script"
                        value={csvPreprocessing.script}
                        onChange={(event) => updateConfig({
                          ...excelConfig,
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
                {dbConnectionScope === 'global' ? (
                  <div className="rounded border border-gray-300 bg-gray-50 px-3 py-2 text-sm text-gray-600">
                    {dbType === 'oracle' ? 'Oracle' : 'PostgreSQL'}
                  </div>
                ) : (
                  <select
                    id="node-editor-db-type"
                    value={dbType}
                    onChange={(event) => updateConfig(
                      switchDatabaseSourceConfigDbType(
                        event.target.value as DbType,
                        dbConfig as unknown as Partial<DatabaseSourceConfig>,
                      ) as unknown as Record<string, unknown>
                    )}
                    className="w-full rounded border border-gray-300 px-2 py-1 text-sm"
                  >
                    <option value="postgres">PostgreSQL</option>
                    <option value="oracle">Oracle</option>
                  </select>
                )}
              </div>

              {dbConnectionScope === 'global' ? (
                <div className="rounded-xl border border-stone-200 bg-stone-50 p-4">
                  <div className="text-xs font-semibold uppercase tracking-wide text-stone-500">Global Source</div>
                  <p className="mt-2 text-sm text-stone-500">
                    {globalConnection
                      ? 'This node is linked to a global database connection. Edit it from Platform Settings.'
                      : 'This node references a global database connection that no longer exists.'}
                  </p>
                  <div className="mt-4 space-y-3">
                    <div>
                      <div className="mb-1 block text-xs text-gray-500">Connection Name</div>
                      <div className="rounded border border-gray-300 bg-white px-3 py-2 text-sm text-gray-700">
                        {globalConnection?.name ?? 'Missing global source'}
                      </div>
                    </div>
                    {dbConnection && (
                      <div>
                        <div className="mb-1 block text-xs text-gray-500">Connection</div>
                        <div className="rounded border border-gray-300 bg-white px-3 py-2 text-sm text-gray-700">
                          {getConnectionSummary(dbType, dbConnection)}
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              ) : (
                <ConnectionForm
                  config={localDbConnection}
                  onChange={(connection) => updateConfig({
                    ...dbConfig,
                    connection,
                  })}
                  dbType={dbType}
                />
              )}

              {dbType === 'oracle' && oracleFetchConfig && (
                <div className="rounded-xl border border-stone-200 bg-stone-50 p-4">
                  <div className="text-xs font-semibold uppercase tracking-wide text-stone-500">
                    Advanced Configuration
                  </div>
                  <p className="mt-2 text-sm text-stone-500">
                    Arraysize and prefetchrows are Oracle cursor settings and apply to both `fetchall()` and `fetchmany()`.
                  </p>

                  <div className="mt-4 space-y-3">
                    <div>
                      <label htmlFor="node-editor-fetch-mode" className="mb-1 block text-xs text-gray-500">
                        Fetch Mode
                      </label>
                      <select
                        id="node-editor-fetch-mode"
                        value={oracleFetchConfig.mode}
                        onChange={(event) => updateConfig({
                          ...dbConfig,
                          fetch_config: {
                            ...oracleFetchConfig,
                            mode: event.target.value as OracleFetchConfig['mode'],
                          },
                        })}
                        className="w-full rounded border border-gray-300 px-2 py-1 text-sm"
                      >
                        <option value="fetchall">fetchall()</option>
                        <option value="fetchmany">fetchmany()</option>
                      </select>
                    </div>

                    <div className="grid gap-3 md:grid-cols-2">
                      <div>
                        <label htmlFor="node-editor-arraysize" className="mb-1 block text-xs text-gray-500">
                          Arraysize
                        </label>
                        <input
                          id="node-editor-arraysize"
                          type="number"
                          min={1}
                          step={1}
                          value={oracleFetchConfig.arraysize}
                          onChange={(event) => updateConfig({
                            ...dbConfig,
                            fetch_config: {
                              ...oracleFetchConfig,
                              arraysize: event.target.value === '' ? 0 : Number.parseInt(event.target.value, 10),
                            },
                          })}
                          className="w-full rounded border border-gray-300 px-2 py-1 text-sm"
                        />
                      </div>

                      <div>
                        <label htmlFor="node-editor-prefetchrows" className="mb-1 block text-xs text-gray-500">
                          Prefetchrows
                        </label>
                        <input
                          id="node-editor-prefetchrows"
                          type="number"
                          min={0}
                          step={1}
                          value={oracleFetchConfig.prefetchrows}
                          onChange={(event) => updateConfig({
                            ...dbConfig,
                            fetch_config: {
                              ...oracleFetchConfig,
                              prefetchrows: event.target.value === '' ? -1 : Number.parseInt(event.target.value, 10),
                            },
                          })}
                          className="w-full rounded border border-gray-300 px-2 py-1 text-sm"
                        />
                      </div>
                    </div>

                    {oracleFetchError && (
                      <p className="text-sm text-red-600">{oracleFetchError}</p>
                    )}
                  </div>
                </div>
              )}

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
            onClick={() => {
              if (oracleFetchError) return
              commitNodeEditor()
            }}
            disabled={Boolean(oracleFetchError)}
            className="rounded bg-stone-900 px-3 py-1 text-sm text-white hover:bg-stone-700"
          >
            {isCreateMode ? 'Create' : 'Save'}
          </button>
        </div>
      </div>
    </div>
  )
}
