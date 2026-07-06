import type { ChangeEvent } from 'react'
import { uploadExcel } from '../api/client'
import type { ExcelSourceConfig, ExcelWorkbookConfig } from '../types/pipeline'

/**
 * Builds the file-input change handler shared by the Excel source editors.
 *
 * Uploading a workbook just records its path and sheet names (read straight
 * from the file) and auto-selects the first sheet. The actual load happens
 * later via DuckDB read_xlsx; there is no preview/materialize roundtrip.
 */
export function createExcelUploadHandler({
  excelConfig,
  applyConfig,
}: {
  excelConfig: ExcelSourceConfig | null
  applyConfig: (config: ExcelSourceConfig) => void
}) {
  return async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (!file || !excelConfig) return

    try {
      const result = await uploadExcel(file)
      applyConfig({
        ...excelConfig,
        file_path: result.file_path,
        original_filename: result.filename,
        sheet_names: result.sheet_names,
        selected_sheet: result.sheet_names[0] ?? '',
        header: excelConfig.header ?? true,
      })
    } finally {
      event.target.value = ''
    }
  }
}

/**
 * Upload handler for the workbook hub: records only the file identity and its
 * sheet names. No sheet is auto-selected — extraction settings belong to the
 * sheet nodes the picker creates (docs/excel-node-model.md §4).
 */
export function createWorkbookUploadHandler({
  applyConfig,
}: {
  applyConfig: (config: ExcelWorkbookConfig) => void
}) {
  return async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (!file) return

    try {
      const result = await uploadExcel(file)
      applyConfig({
        file_path: result.file_path,
        original_filename: result.filename,
        sheet_names: result.sheet_names,
        sheet_dimensions: result.sheet_dimensions,
      })
    } finally {
      event.target.value = ''
    }
  }
}
