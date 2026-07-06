import { describe, expect, it } from 'vitest'
import { computeWorkbookRollup } from './workbookRollup'
import type { NodeExecutionResult } from '../types/pipeline'

const edges = [
  { source: 'hub', target: 's1' },
  { source: 'hub', target: 's2' },
  { source: 'hub', target: 's3' },
  { source: 's1', target: 'tx' }, // data edge — not a child of the hub
]

function result(id: string, status: NodeExecutionResult['status']): NodeExecutionResult {
  return { node_id: id, status }
}

describe('computeWorkbookRollup', () => {
  it('is neutral with no results yet', () => {
    const rollup = computeWorkbookRollup({ hubId: 'hub', edges, nodeResults: {} })
    expect(rollup.childIds).toEqual(['s1', 's2', 's3'])
    expect(rollup.kind).toBe('neutral')
    expect(rollup.label).toBeNull()
  })

  it('reports loaded count while all-successful', () => {
    const rollup = computeWorkbookRollup({
      hubId: 'hub',
      edges,
      nodeResults: { s1: result('s1', 'success'), s2: result('s2', 'success') },
    })
    expect(rollup.kind).toBe('neutral')
    expect(rollup.label).toBe('2/3 loaded')
  })

  it('is mixed when some children failed', () => {
    const rollup = computeWorkbookRollup({
      hubId: 'hub',
      edges,
      nodeResults: {
        s1: result('s1', 'success'),
        s2: result('s2', 'success'),
        s3: result('s3', 'error'),
      },
    })
    expect(rollup.kind).toBe('mixed')
    expect(rollup.label).toBe('2/3 loaded, 1 failed')
  })

  it('is error only when every child failed', () => {
    const rollup = computeWorkbookRollup({
      hubId: 'hub',
      edges,
      nodeResults: {
        s1: result('s1', 'error'),
        s2: result('s2', 'error'),
        s3: result('s3', 'error'),
      },
    })
    expect(rollup.kind).toBe('error')
    expect(rollup.label).toBe('0/3 loaded, 3 failed')
  })

  it('ignores non-child results and hubs without children', () => {
    const rollup = computeWorkbookRollup({
      hubId: 'hub',
      edges: [],
      nodeResults: { tx: result('tx', 'error') },
    })
    expect(rollup.total).toBe(0)
    expect(rollup.kind).toBe('neutral')
    expect(rollup.label).toBeNull()
  })
})
