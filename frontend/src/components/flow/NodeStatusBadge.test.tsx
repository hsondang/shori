import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, expect, vi } from 'vitest'
import NodeStatusBadge from './NodeStatusBadge'
import type { NodeExecutionResult } from '../../types/pipeline'
import { usePipelineStore } from '../../store/pipelineStore'

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
    expect(screen.getByText(/Running/)).toBeInTheDocument()
  })

  it('shows connecting status text without a timer', () => {
    badge({ status: 'connecting', started_at: '2026-04-08T10:00:00Z' })
    expect(screen.getByText('Connecting')).toBeInTheDocument()
    expect(screen.queryByText(/00:/)).not.toBeInTheDocument()
  })

  it('shows a running timer when started_at is present', () => {
    usePipelineStore.setState({ executionClockNow: Date.parse('2026-04-08T10:01:05Z') })
    badge({ status: 'running', started_at: '2026-04-08T10:00:00Z' })
    expect(screen.getByText(/Running · 01:05/)).toBeInTheDocument()
  })

  it('shows success status text', () => {
    badge({ status: 'success', row_count: 5, column_count: 3 })
    expect(screen.getByText(/success/)).toBeInTheDocument()
  })

  it('shows cancelled status text', () => {
    badge({ status: 'cancelled' })
    expect(screen.getByText(/Cancelled/)).toBeInTheDocument()
  })

  it('shows error status text', () => {
    badge({ status: 'error', error: 'Something broke' })
    expect(screen.getByText(/error/)).toBeInTheDocument()
    expect(screen.queryByText('Something broke')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /view error/i })).not.toBeInTheDocument()
  })

  it('shows a view error action when a handler is provided', async () => {
    const user = userEvent.setup()
    const onViewError = vi.fn()
    render(
      <NodeStatusBadge
        result={{ node_id: 'n1', status: 'error', error: 'Something broke' }}
        onViewError={onViewError}
      />
    )

    await user.click(screen.getByRole('button', { name: /view error/i }))
    expect(onViewError).toHaveBeenCalledTimes(1)
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
