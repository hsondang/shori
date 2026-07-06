import { useEffect, useMemo, useState } from 'react'
import { Button, Modal } from '@shori/design-system'
import { usePipelineStore, type WorkbookSheetSelection } from '../../store/pipelineStore'
import { dedupeTableName, slugifyTableName, validateTableName } from '../../lib/tableNames'
import type { ExcelWorkbookConfig, NodeLoadMode } from '../../types/pipeline'

interface SheetRowState {
  checked: boolean
  tableName: string
  cellRange: string
  header: boolean
  allVarchar: boolean
}

type BatchLoadChoice = NodeLoadMode | 'none'

/**
 * The workbook hub's sheet picker (docs/excel-node-model.md §4): a checkbox
 * row per sheet, table names prefilled from the slugified sheet name and
 * deduped against the project + other checked rows. Confirm creates one
 * sheet node + structural edge per selection, optionally batch-loading them.
 * Reopening later is the add-more-sheets flow; already-imported sheets stay
 * checkable (two extractions of one sheet is a legitimate Excel layout).
 */
export default function SheetPickerModal() {
  const hubId = usePipelineStore((s) => s.sheetPickerHubId)
  const closeSheetPicker = usePipelineStore((s) => s.closeSheetPicker)
  const addWorkbookSheets = usePipelineStore((s) => s.addWorkbookSheets)
  const nodes = usePipelineStore((s) => s.nodes)
  const edges = usePipelineStore((s) => s.edges)

  const hub = hubId ? nodes.find((node) => node.id === hubId) : undefined
  const hubConfig = hub
    ? ((hub.data as Record<string, unknown>).config as ExcelWorkbookConfig)
    : null
  const sheetNames = useMemo(() => hubConfig?.sheet_names ?? [], [hubConfig])

  // How many sheet nodes already extract each sheet of this hub.
  const importedCounts = useMemo(() => {
    const counts = new Map<string, number>()
    if (!hubId) return counts
    const childIds = new Set(edges.filter((e) => e.source === hubId).map((e) => e.target))
    nodes.forEach((node) => {
      if (!childIds.has(node.id)) return
      const sheet = ((node.data as Record<string, unknown>).config as Record<string, unknown>)?.selected_sheet
      if (typeof sheet === 'string' && sheet) {
        counts.set(sheet, (counts.get(sheet) ?? 0) + 1)
      }
    })
    return counts
  }, [edges, hubId, nodes])

  const projectTableNames = useMemo(
    () => new Set(
      nodes
        .map((node) => (node.data as Record<string, unknown>).tableName as string)
        .filter(Boolean),
    ),
    [nodes],
  )

  const [rows, setRows] = useState<Record<string, SheetRowState>>({})
  const [batchLoad, setBatchLoad] = useState<BatchLoadChoice>('none')
  const [submitting, setSubmitting] = useState(false)

  // Re-seed whenever the picker opens (or the workbook's sheets change).
  useEffect(() => {
    if (!hubId) return
    setRows(Object.fromEntries(sheetNames.map((sheet) => [sheet, {
      checked: false,
      tableName: '',
      cellRange: '',
      header: true,
      allVarchar: false,
    }])))
    setBatchLoad('none')
    setSubmitting(false)
  }, [hubId, sheetNames])

  if (!hubId || !hub || !hubConfig) return null

  const checkedSheets = sheetNames.filter((sheet) => rows[sheet]?.checked)

  const toggleSheet = (sheet: string) => {
    setRows((prev) => {
      const row = prev[sheet]
      if (!row) return prev
      if (row.checked) {
        return { ...prev, [sheet]: { ...row, checked: false } }
      }
      // Prefill on check: slugified sheet name, deduped against the project
      // and every other checked row.
      const taken = new Set(projectTableNames)
      sheetNames.forEach((other) => {
        const otherRow = prev[other]
        if (other !== sheet && otherRow?.checked && otherRow.tableName) taken.add(otherRow.tableName)
      })
      const tableName = row.tableName || dedupeTableName(slugifyTableName(sheet), taken)
      return { ...prev, [sheet]: { ...row, checked: true, tableName } }
    })
  }

  const updateRow = (sheet: string, patch: Partial<SheetRowState>) => {
    setRows((prev) => ({ ...prev, [sheet]: { ...prev[sheet], ...patch } }))
  }

  const rowError = (sheet: string): string | null => {
    const row = rows[sheet]
    if (!row?.checked) return null
    const taken = new Set(projectTableNames)
    checkedSheets.forEach((other) => {
      if (other !== sheet && rows[other]?.tableName) taken.add(rows[other].tableName.trim())
    })
    return validateTableName(row.tableName, taken)
  }

  const hasErrors = checkedSheets.some((sheet) => rowError(sheet) !== null)
  const canConfirm = checkedSheets.length > 0 && !hasErrors && !submitting

  const handleConfirm = async () => {
    if (!canConfirm) return
    setSubmitting(true)
    const selections: WorkbookSheetSelection[] = checkedSheets.map((sheet) => ({
      sheet,
      tableName: rows[sheet].tableName.trim(),
      cellRange: rows[sheet].cellRange.trim() || undefined,
      header: rows[sheet].header,
      allVarchar: rows[sheet].allVarchar,
    }))
    closeSheetPicker()
    await addWorkbookSheets(hubId, selections, {
      batchLoadMode: batchLoad === 'none' ? null : batchLoad,
    })
  }

  return (
    <Modal
      open
      onClose={closeSheetPicker}
      size="lg"
      title="Import sheets"
      description={`${hubConfig.original_filename} — each selected sheet becomes its own table on the canvas.`}
      footer={
        <>
          <div className="mr-auto flex items-center gap-2 text-xs text-gray-500">
            <label htmlFor="sheet-picker-batch-load">After creating</label>
            <select
              id="sheet-picker-batch-load"
              value={batchLoad}
              onChange={(event) => setBatchLoad(event.target.value as BatchLoadChoice)}
              className="rounded border border-gray-300 px-2 py-1 text-xs"
            >
              <option value="none">Do nothing</option>
              <option value="in_memory">Load to memory</option>
              <option value="materialized">Materialize</option>
            </select>
          </div>
          <Button variant="secondary" onClick={closeSheetPicker}>Cancel</Button>
          <Button variant="primary" disabled={!canConfirm} onClick={() => { void handleConfirm() }}>
            {checkedSheets.length > 0
              ? `Add ${checkedSheets.length} ${checkedSheets.length === 1 ? 'sheet' : 'sheets'}`
              : 'Add sheets'}
          </Button>
        </>
      }
    >
      <div className="max-h-[50vh] space-y-2 overflow-y-auto pr-1" data-testid="sheet-picker-rows">
        {sheetNames.map((sheet) => {
          const row = rows[sheet]
          if (!row) return null
          const imported = importedCounts.get(sheet) ?? 0
          const error = rowError(sheet)
          return (
            <div key={sheet} className="rounded-lg border border-gray-200 bg-white p-3">
              <label className="flex items-center gap-2 text-sm text-gray-800">
                <input
                  type="checkbox"
                  checked={row.checked}
                  onChange={() => toggleSheet(sheet)}
                  aria-label={`Import sheet ${sheet}`}
                />
                <span className="font-medium">{sheet}</span>
                {imported > 0 && (
                  <span className="rounded bg-gray-100 px-1.5 py-0.5 text-[11px] text-gray-500">
                    imported ({imported})
                  </span>
                )}
              </label>

              {row.checked && (
                <div className="mt-3 grid gap-3 pl-6 md:grid-cols-2">
                  <div>
                    <label htmlFor={`sheet-table-${sheet}`} className="mb-1 block text-xs text-gray-500">
                      Table name
                    </label>
                    <input
                      id={`sheet-table-${sheet}`}
                      type="text"
                      value={row.tableName}
                      onChange={(event) => updateRow(sheet, { tableName: event.target.value })}
                      className={`w-full rounded border px-2 py-1 font-mono text-sm ${
                        error ? 'border-red-400' : 'border-gray-300'
                      }`}
                    />
                    {error && <p className="mt-1 text-xs text-red-600">{error}</p>}
                  </div>
                  <div>
                    <label htmlFor={`sheet-range-${sheet}`} className="mb-1 block text-xs text-gray-500">
                      Range (optional)
                    </label>
                    <input
                      id={`sheet-range-${sheet}`}
                      type="text"
                      value={row.cellRange}
                      onChange={(event) => updateRow(sheet, { cellRange: event.target.value })}
                      placeholder="e.g. A1:F500"
                      className="w-full rounded border border-gray-300 px-2 py-1 font-mono text-sm"
                    />
                  </div>
                  <label className="flex items-center gap-2 text-sm text-gray-600">
                    <input
                      type="checkbox"
                      checked={row.header}
                      onChange={(event) => updateRow(sheet, { header: event.target.checked })}
                    />
                    First row is the header
                  </label>
                  <label className="flex items-center gap-2 text-sm text-gray-600">
                    <input
                      type="checkbox"
                      checked={row.allVarchar}
                      onChange={(event) => updateRow(sheet, { allVarchar: event.target.checked })}
                    />
                    Read every column as text (all_varchar)
                  </label>
                </div>
              )}
            </div>
          )
        })}
        {sheetNames.length === 0 && (
          <p className="text-sm text-gray-500">Upload a workbook to see its sheets.</p>
        )}
      </div>
    </Modal>
  )
}
