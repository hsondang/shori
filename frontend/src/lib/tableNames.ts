/**
 * Table names for sheet nodes created by the workbook picker
 * (docs/excel-node-model.md §4): prefilled from the slugified sheet name,
 * deduped with a numeric suffix, validated inline. Validation mirrors the
 * backend's validate_user_table_name (duckdb_manager.py) plus project-wide
 * uniqueness — the backend validators remain authoritative on save.
 */

const RESERVED_PREFIX = '_shori_'
const STAGING_SUFFIX = '__staging'

export function slugifyTableName(sheetName: string): string {
  const slug = sheetName
    .normalize('NFKD')
    .replace(/[̀-ͯ]/g, '') // strip combining accents
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
  if (!slug) return 'sheet'
  return /^[0-9]/.test(slug) ? `t_${slug}` : slug
}

export function dedupeTableName(base: string, taken: ReadonlySet<string>): string {
  if (!taken.has(base)) return base
  for (let i = 2; ; i++) {
    const candidate = `${base}_${i}`
    if (!taken.has(candidate)) return candidate
  }
}

export function validateTableName(
  name: string,
  taken: ReadonlySet<string>,
): string | null {
  const trimmed = name.trim()
  if (!trimmed) return 'Table name must not be empty'
  if (trimmed.startsWith(RESERVED_PREFIX)) {
    return `Table names starting with '${RESERVED_PREFIX}' are reserved`
  }
  if (trimmed.endsWith(STAGING_SUFFIX)) {
    return `Table names ending with '${STAGING_SUFFIX}' are reserved`
  }
  if (taken.has(trimmed)) {
    return `'${trimmed}' is already used in this project`
  }
  return null
}
