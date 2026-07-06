import { describe, expect, it } from 'vitest'
import { dedupeTableName, slugifyTableName, validateTableName } from './tableNames'

describe('slugifyTableName', () => {
  it('lowercases and snake_cases plain sheet names', () => {
    expect(slugifyTableName('DS Active')).toBe('ds_active')
    expect(slugifyTableName('KM Leads')).toBe('km_leads')
  })

  it('collapses symbol runs and trims edge underscores', () => {
    expect(slugifyTableName('  Raw -- July!! ')).toBe('raw_july')
    expect(slugifyTableName('a///b')).toBe('a_b')
  })

  it('prefixes names that start with a digit', () => {
    expect(slugifyTableName('2026 Budget')).toBe('t_2026_budget')
  })

  it('strips accents and falls back for all-symbol names', () => {
    expect(slugifyTableName('Café Décor')).toBe('cafe_decor')
    expect(slugifyTableName('!!!')).toBe('sheet')
    expect(slugifyTableName('データ')).toBe('sheet')
  })
})

describe('dedupeTableName', () => {
  it('returns the base when free', () => {
    expect(dedupeTableName('orders', new Set())).toBe('orders')
  })

  it('walks numeric suffixes past collisions', () => {
    const taken = new Set(['orders', 'orders_2', 'orders_3'])
    expect(dedupeTableName('orders', taken)).toBe('orders_4')
  })
})

describe('validateTableName', () => {
  it('accepts a free, ordinary name', () => {
    expect(validateTableName('orders', new Set(['other']))).toBeNull()
  })

  it('rejects empty, reserved, and duplicate names', () => {
    expect(validateTableName('  ', new Set())).toMatch(/empty/)
    expect(validateTableName('_shori_meta', new Set())).toMatch(/reserved/)
    expect(validateTableName('orders__staging', new Set())).toMatch(/reserved/)
    expect(validateTableName('orders', new Set(['orders']))).toMatch(/already used/)
  })
})
