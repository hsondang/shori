import type { CsvSourceConfig } from '../types/pipeline'

export function getCsvPreprocessFingerprint(config: CsvSourceConfig | null | undefined): string | null {
  if (!config?.file_path) return null

  const preprocessing = config.preprocessing
  if (!preprocessing?.enabled) return null

  return JSON.stringify({
    file_path: config.file_path,
    runtime: preprocessing.runtime,
    script: preprocessing.script,
  })
}
