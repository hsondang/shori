import type { CsvPreprocessingConfig, CsvSourceConfig } from '../types/pipeline'

export function getCsvPreprocessFingerprint(
  config: (Partial<Pick<CsvSourceConfig, 'file_path' | 'original_filename'>> & { preprocessing?: CsvPreprocessingConfig }) | null | undefined
): string | null {
  if (!config?.file_path) return null

  const preprocessing = config.preprocessing
  if (!preprocessing?.enabled) return null

  return JSON.stringify({
    file_path: config.file_path,
    runtime: preprocessing.runtime,
    script: preprocessing.script,
  })
}
