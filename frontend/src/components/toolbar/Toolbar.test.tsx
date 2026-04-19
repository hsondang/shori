import { fireEvent, render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { act } from 'react'
import { MemoryRouter } from 'react-router-dom'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import Toolbar from './Toolbar'
import { usePipelineStore } from '../../store/pipelineStore'
import { useSettingsStore } from '../../store/settingsStore'

const mockListPipelines = vi.fn()
const mockExecutePipeline = vi.fn()
const mockPreviewData = vi.fn()
const mockPreviewCsvSource = vi.fn()
const mockPreviewPreprocessedCsvSource = vi.fn()
const mockSavePipeline = vi.fn()
const mockLoadPipeline = vi.fn()
const mockTestDbConnection = vi.fn()
const mockListGlobalDatabaseConnections = vi.fn()
const mockDeletePreprocessedCsvArtifact = vi.fn((..._args: any[]) => Promise.resolve({ deleted: true }))

vi.mock('../../api/client', () => ({
  listPipelines: (...args: unknown[]) => mockListPipelines(...args),
  executePipeline: (...args: unknown[]) => mockExecutePipeline(...args),
  previewData: (...args: unknown[]) => mockPreviewData(...args),
  previewCsvSource: (...args: unknown[]) => mockPreviewCsvSource(...args),
  previewPreprocessedCsvSource: (...args: unknown[]) => mockPreviewPreprocessedCsvSource(...args),
  savePipeline: (...args: unknown[]) => mockSavePipeline(...args),
  loadPipeline: (...args: unknown[]) => mockLoadPipeline(...args),
  listGlobalDatabaseConnections: (...args: unknown[]) => mockListGlobalDatabaseConnections(...args),
  testDbConnection: (...args: unknown[]) => mockTestDbConnection(...args),
  deletePreprocessedCsvArtifact: (...args: unknown[]) => mockDeletePreprocessedCsvArtifact(...args),
}))

function renderToolbar() {
  return render(
    <MemoryRouter>
      <Toolbar />
    </MemoryRouter>
  )
}

describe('Toolbar', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockListGlobalDatabaseConnections.mockResolvedValue([])
    act(() => {
      usePipelineStore.getState().newPipeline()
      useSettingsStore.setState({
        globalDatabaseConnections: [],
        globalConnectionsLoaded: false,
        globalConnectionsLoading: false,
      })
    })
  })

  it('opens the database source dropdown and shows the empty state', async () => {
    const user = userEvent.setup()
    renderToolbar()

    await user.click(screen.getByRole('button', { name: 'Database Source' }))
    const dropdown = screen.getByTestId('database-source-dropdown')

    expect(screen.getByText('Database Connections')).toBeInTheDocument()
    expect(screen.getByText('No global database connections yet.')).toBeInTheDocument()
    expect(screen.getByText('No local database connections yet.')).toBeInTheDocument()
    expect(within(dropdown as HTMLElement).getByRole('button', { name: 'Add Local Connection' })).toBeInTheDocument()
    expect(screen.queryByLabelText(/connection name/i)).not.toBeInTheDocument()
  })

  it('adds a saved connection and renders it as a draggable preset', async () => {
    const user = userEvent.setup()
    renderToolbar()

    await user.click(screen.getByRole('button', { name: 'Database Source' }))
    const dropdown = screen.getByTestId('database-source-dropdown')
    await user.click(within(dropdown).getByRole('button', { name: 'Add Local Connection' }))
    const modal = screen.getByTestId('database-source-modal')
    fireEvent.change(within(modal).getByLabelText(/connection name/i), { target: { value: 'Analytics Postgres' } })
    fireEvent.change(screen.getByPlaceholderText('Host'), { target: { value: 'localhost' } })
    fireEvent.change(screen.getByPlaceholderText('Database'), { target: { value: 'analytics' } })
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

    renderToolbar()

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
    renderToolbar()

    await user.click(screen.getByRole('button', { name: 'Database Source' }))
    await user.click(screen.getByRole('button', { name: 'Add Local Connection' }))
    const modal = screen.getByTestId('database-source-modal')
    await user.type(within(modal).getByLabelText(/connection name/i), 'Transient Connection')
    await user.click(within(modal).getByRole('button', { name: 'Discard' }))

    expect(screen.queryByTestId('database-source-modal')).not.toBeInTheDocument()
    expect(usePipelineStore.getState().databaseConnections).toEqual([])
  })

  it('shows the active pipeline timer while a tracked run is live', () => {
    act(() => {
      usePipelineStore.setState({
        activePipelineExecutionId: 'exec-1',
        activeExecutions: {
          'exec-1': {
            execution_id: 'exec-1',
            kind: 'pipeline',
            status: 'running',
            started_at: '2026-04-08T10:00:00Z',
            node_results: {},
          },
        },
        executionClockNow: Date.parse('2026-04-08T10:01:05Z'),
      })
    })

    renderToolbar()

    expect(screen.getByText('Running pipeline · 01:05')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Running...' })).toBeDisabled()
  })

  it('shows global and local connection groups in the dropdown', async () => {
    const user = userEvent.setup()
    act(() => {
      useSettingsStore.setState({
        globalDatabaseConnections: [
          {
            id: 'global-1',
            name: 'Shared Warehouse',
            db_type: 'postgres',
            host: 'db.internal',
            port: 5432,
            database: 'warehouse',
            user: 'readonly',
            password: 'secret',
          },
        ],
        globalConnectionsLoaded: true,
        globalConnectionsLoading: false,
      })
      usePipelineStore.getState().addDatabaseConnection({
        name: 'Project Replica',
        db_type: 'postgres',
        host: 'localhost',
        port: 5432,
        database: 'analytics',
        user: 'user',
        password: 'secret',
      })
    })

    renderToolbar()

    await user.click(screen.getByRole('button', { name: 'Database Source' }))

    expect(screen.getAllByText('Global').length).toBeGreaterThan(0)
    expect(screen.getAllByText('Local').length).toBeGreaterThan(0)
    expect(screen.getByText('Shared Warehouse')).toBeInTheDocument()
    expect(screen.getByText('Project Replica')).toBeInTheDocument()
  })
})
