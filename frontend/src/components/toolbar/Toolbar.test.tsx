import { render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { act } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import Toolbar from './Toolbar'
import { usePipelineStore } from '../../store/pipelineStore'

const mockListPipelines = vi.fn()
const mockExecutePipeline = vi.fn()
const mockPreviewData = vi.fn()
const mockPreviewCsvSource = vi.fn()
const mockPreviewPreprocessedCsvSource = vi.fn()
const mockSavePipeline = vi.fn()
const mockLoadPipeline = vi.fn()
const mockTestDbConnection = vi.fn()
const mockDeletePreprocessedCsvArtifact = vi.fn((..._args: any[]) => Promise.resolve({ deleted: true }))

vi.mock('../../api/client', () => ({
  listPipelines: (...args: unknown[]) => mockListPipelines(...args),
  executePipeline: (...args: unknown[]) => mockExecutePipeline(...args),
  previewData: (...args: unknown[]) => mockPreviewData(...args),
  previewCsvSource: (...args: unknown[]) => mockPreviewCsvSource(...args),
  previewPreprocessedCsvSource: (...args: unknown[]) => mockPreviewPreprocessedCsvSource(...args),
  savePipeline: (...args: unknown[]) => mockSavePipeline(...args),
  loadPipeline: (...args: unknown[]) => mockLoadPipeline(...args),
  testDbConnection: (...args: unknown[]) => mockTestDbConnection(...args),
  deletePreprocessedCsvArtifact: (...args: unknown[]) => mockDeletePreprocessedCsvArtifact(...args),
}))

describe('Toolbar', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    act(() => usePipelineStore.getState().newPipeline())
  })

  it('opens the database source dropdown and shows the empty state', async () => {
    const user = userEvent.setup()
    render(<Toolbar />)

    await user.click(screen.getByRole('button', { name: 'Database Source' }))
    const dropdown = screen.getByTestId('database-source-dropdown')

    expect(screen.getByText('Database Connections')).toBeInTheDocument()
    expect(screen.getByText('No saved database connections yet.')).toBeInTheDocument()
    expect(within(dropdown as HTMLElement).getByRole('button', { name: 'Add' })).toBeInTheDocument()
    expect(screen.queryByLabelText(/connection name/i)).not.toBeInTheDocument()
  })

  it('adds a saved connection and renders it as a draggable preset', async () => {
    const user = userEvent.setup()
    render(<Toolbar />)

    await user.click(screen.getByRole('button', { name: 'Database Source' }))
    const dropdown = screen.getByTestId('database-source-dropdown')
    await user.click(within(dropdown).getByRole('button', { name: 'Add' }))
    const modal = screen.getByTestId('database-source-modal')
    await user.type(within(modal).getByLabelText(/connection name/i), 'Analytics Postgres')
    await user.type(screen.getByPlaceholderText('Host'), 'localhost')
    await user.type(screen.getByPlaceholderText('Database'), 'analytics')
    await user.click(within(modal).getByRole('button', { name: 'Save' }))

    const preset = screen.getByText('Analytics Postgres').closest('div[draggable="true"]')
    expect(preset).not.toBeNull()
    expect(usePipelineStore.getState().databaseConnections).toEqual([
      expect.objectContaining({ name: 'Analytics Postgres', database: 'analytics' }),
    ])
  })

  it('edits and deletes saved connections from the dropdown', async () => {
    const user = userEvent.setup()
    act(() => {
      usePipelineStore.getState().addDatabaseConnection({
        name: 'Warehouse',
        db_type: 'oracle',
        host: 'orahost',
        port: 1521,
        service_name: 'ORCL',
        user: 'user',
        password: 'secret',
      })
    })

    render(<Toolbar />)

    await user.click(screen.getByRole('button', { name: 'Database Source' }))
    await user.click(screen.getByRole('button', { name: 'Edit' }))
    const modal = screen.getByTestId('database-source-modal')
    await user.clear(within(modal).getByLabelText(/connection name/i))
    await user.type(within(modal).getByLabelText(/connection name/i), 'Warehouse Prod')
    await user.click(within(modal).getByRole('button', { name: 'Save' }))

    expect(usePipelineStore.getState().databaseConnections[0]).toEqual(
      expect.objectContaining({ name: 'Warehouse Prod' })
    )

    await user.click(screen.getByRole('button', { name: 'Delete' }))
    expect(usePipelineStore.getState().databaseConnections).toEqual([])
  })

  it('discards modal changes without mutating saved connections', async () => {
    const user = userEvent.setup()
    render(<Toolbar />)

    await user.click(screen.getByRole('button', { name: 'Database Source' }))
    await user.click(screen.getByRole('button', { name: 'Add' }))
    const modal = screen.getByTestId('database-source-modal')
    await user.type(within(modal).getByLabelText(/connection name/i), 'Transient Connection')
    await user.click(within(modal).getByRole('button', { name: 'Discard' }))

    expect(screen.queryByTestId('database-source-modal')).not.toBeInTheDocument()
    expect(usePipelineStore.getState().databaseConnections).toEqual([])
  })
})
