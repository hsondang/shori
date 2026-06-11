import type { ChangeEvent } from 'react'
import { materializeExcelSheet, uploadExcel } from '../api/client'
import type { CsvPreprocessingConfig, ExcelSourceConfig } from '../types/pipeline'

/**
 * Builds the file-input change handler shared by the Excel source editors.
 *
 * Uploading a workbook auto-selects and materializes its first sheet so the
 * node is immediately usable. The only thing call sites differ on is how the
 * resulting config is persisted, which they supply via `applyConfig`.
 */
export function createExcelUploadHandler({
  excelConfig,
  csvPreprocessing,
  applyConfig,
}: {
  excelConfig: ExcelSourceConfig | null
  csvPreprocessing: CsvPreprocessingConfig
  applyConfig: (config: ExcelSourceConfig) => void
}) {
  return async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (!file || !excelConfig) return

    try {
      const result = await uploadExcel(file)
      const selectedSheet = result.sheet_names[0] ?? ''
      const nextConfig: ExcelSourceConfig = {
        ...excelConfig,
        file_path: result.file_path,
        original_filename: result.filename,
        sheet_names: result.sheet_names,
        sheets: result.sheets,
        selected_sheet: selectedSheet,
        materialized_csv_path: '',
        materialized_csv_filename: '',
        preprocessing: excelConfig.preprocessing ?? csvPreprocessing,
      }

      applyConfig(nextConfig)

      if (selectedSheet) {
        const materialized = await materializeExcelSheet(result.file_path, selectedSheet)
        applyConfig({
          ...nextConfig,
          materialized_csv_path: materialized.file_path,
          materialized_csv_filename: materialized.filename,
        })
      }
    } finally {
      event.target.value = ''
    }
  }
}
