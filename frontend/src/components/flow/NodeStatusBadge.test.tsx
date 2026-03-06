import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import NodeStatusBadge from './NodeStatusBadge'
import type { NodeExecutionResult } from '../../types/pipeline'

function badge(overrides: Partial<NodeExecutionResult> = {}) {
  return render(
    <NodeStatusBadge result={{ node_id: 'n1', status: 'idle', ...overrides }} />
  )
}

describe('NodeStatusBadge', () => {
  it('shows idle status text', () => {
    badge({ status: 'idle' })
    expect(screen.getByText(/idle/)).toBeInTheDocument()
  })

  it('shows running status text', () => {
    badge({ status: 'running' })
    expect(screen.getByText(/running/)).toBeInTheDocument()
  })

  it('shows success status text', () => {
    badge({ status: 'success', row_count: 5, column_count: 3 })
    expect(screen.getByText(/success/)).toBeInTheDocument()
  })

  it('shows error status text', () => {
    badge({ status: 'error', error: 'Something broke' })
    expect(screen.getByText(/error/)).toBeInTheDocument()
    expect(screen.getByText('Something broke')).toBeInTheDocument()
  })

  it('shows row count when status is success', () => {
    badge({ status: 'success', row_count: 42, column_count: 3 })
    expect(screen.getByText(/42/)).toBeInTheDocument()
  })

  it('formats large row counts with locale separators', () => {
    badge({ status: 'success', row_count: 1000, column_count: 2 })
    const text = screen.getByText(/1[,.]?000/)
    expect(text).toBeInTheDocument()
  })

  it('shows rounded execution time when present', () => {
    badge({ status: 'success', execution_time_ms: 123.456, row_count: 1, column_count: 1 })
    expect(screen.getByText(/123ms/)).toBeInTheDocument()
  })

  it('does not show row count when null', () => {
    const { container } = badge({ status: 'idle' })
    expect(container.textContent).not.toMatch(/rows/)
  })
})
