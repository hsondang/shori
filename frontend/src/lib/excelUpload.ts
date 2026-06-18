import type { ChangeEvent } from 'react'
import { uploadExcel } from '../api/client'
import type { ExcelSourceConfig } from '../types/pipeline'

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
