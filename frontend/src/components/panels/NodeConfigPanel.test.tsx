import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { act } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import NodeConfigPanel from './NodeConfigPanel'
import { usePipelineStore } from '../../store/pipelineStore'
import type { DataPreview, NodeExecutionResult } from '../../types/pipeline'

const mockUploadCsv = vi.fn()
const mockExecuteNode = vi.fn()
const mockPreviewData = vi.fn()

vi.mock('@monaco-editor/react', () => ({
  default: ({ value, onChange }: { value: string; onChange?: (value: string) => void }) => (
    <textarea aria-label="sql-editor" value={value} onChange={(event) => onChange?.(event.target.value)} />
  ),
}))

vi.mock('../../api/client', () => ({
  uploadCsv: (...args: unknown[]) => mockUploadCsv(...args),
  executePipeline: vi.fn(),
  executeNode: (...args: unknown[]) => mockExecuteNode(...args),
  previewData: (...args: unknown[]) => mockPreviewData(...args),
  savePipeline: vi.fn(),
  loadPipeline: vi.fn(),
  listPipelines: vi.fn(),
}))

function makePreview(overrides: Partial<DataPreview> = {}): DataPreview {
  return {
    columns: ['id', 'name'],
    column_types: ['INTEGER', 'VARCHAR'],
    rows: [[1, 'Alice']],
    total_rows: 1,
    offset: 0,
    limit: 100,
    ...overrides,
  }
}

describe('NodeConfigPanel', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    act(() => {
      usePipelineStore.getState().newPipeline()
      usePipelineStore.setState({
        nodes: [
          {
            id: 'db-node',
            type: 'db_source',
            position: { x: 0, y: 0 },
            data: {
              label: 'Analytics Postgres',
              tableName: 'db_table',
              config: {
                db_type: 'postgres',
                connection: {
                  host: 'localhost',
                  port: 5432,
                  database: 'analytics',
                  user: 'user',
                  password: 'secret',
                },
                query: 'SELECT 1',
              },
            },
          },
        ],
        selectedNodeId: 'db-node',
      })
    })
  })

  it('shows SQL editing only for database nodes', () => {
    render(<NodeConfigPanel />)

    expect(screen.getByText('Analytics Postgres')).toBeInTheDocument()
    expect(screen.getByText('SQL Query')).toBeInTheDocument()
    expect(screen.getByLabelText('sql-editor')).toHaveValue('SELECT 1')

    expect(screen.queryByText('Label')).not.toBeInTheDocument()
    expect(screen.queryByText('Table Name')).not.toBeInTheDocument()
    expect(screen.queryByText('Database Type')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /test connection/i })).not.toBeInTheDocument()
  })

  it('stages csv metadata edits and discards them without updating the store', async () => {
    const user = userEvent.setup()

    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'csv-node',
            type: 'csv_source',
            position: { x: 0, y: 0 },
            data: {
              label: 'Orders CSV',
              tableName: 'orders_table',
              config: { file_path: '/tmp/orders.csv', original_filename: 'orders.csv' },
            },
          },
        ],
        selectedNodeId: 'csv-node',
      })
    })

    render(<NodeConfigPanel />)

    expect(screen.getByRole('button', { name: 'Edit' })).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: 'Edit' }))

    const saveButton = screen.getByRole('button', { name: 'Save' })
    expect(saveButton).toBeDisabled()

    await user.clear(screen.getByLabelText('Label'))
    await user.type(screen.getByLabelText('Label'), 'Renamed CSV')
    expect(saveButton).toBeEnabled()

    await user.click(screen.getByRole('button', { name: 'Discard' }))

    expect(screen.queryByRole('button', { name: 'Save' })).not.toBeInTheDocument()
    expect((usePipelineStore.getState().nodes[0].data as Record<string, unknown>).label).toBe('Orders CSV')
    expect(screen.getByText('Orders CSV')).toBeInTheDocument()
  })

  it('saves csv metadata changes only after the user confirms them', async () => {
    const user = userEvent.setup()

    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'csv-node',
            type: 'csv_source',
            position: { x: 0, y: 0 },
            data: {
              label: 'Orders CSV',
              tableName: 'orders_table',
              config: { file_path: '/tmp/orders.csv', original_filename: 'orders.csv' },
            },
          },
        ],
        selectedNodeId: 'csv-node',
      })
    })

    render(<NodeConfigPanel />)

    await user.click(screen.getByRole('button', { name: 'Edit' }))
    await user.clear(screen.getByLabelText('Label'))
    await user.type(screen.getByLabelText('Label'), 'Orders Cleaned')
    await user.clear(screen.getByLabelText('Table Name'))
    await user.type(screen.getByLabelText('Table Name'), 'orders_cleaned')
    await user.click(screen.getByRole('button', { name: 'Save' }))

    const updated = usePipelineStore.getState().nodes[0].data as Record<string, unknown>
    expect(updated.label).toBe('Orders Cleaned')
    expect(updated.tableName).toBe('orders_cleaned')
    expect(screen.getByText('Orders Cleaned')).toBeInTheDocument()
    expect(screen.getByText('orders_cleaned')).toBeInTheDocument()
  })

  it('disables csv execution until a file has been uploaded', () => {
    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'csv-node',
            type: 'csv_source',
            position: { x: 0, y: 0 },
            data: {
              label: 'Orders CSV',
              tableName: 'orders_table',
              config: { file_path: '', original_filename: '' },
            },
          },
        ],
        selectedNodeId: 'csv-node',
      })
    })

    render(<NodeConfigPanel />)

    expect(screen.getByRole('button', { name: 'Run and Preview' })).toBeDisabled()
  })

  it('executes the csv node and loads preview data on success', async () => {
    const user = userEvent.setup()
    const result: NodeExecutionResult = {
      node_id: 'csv-node',
      status: 'success',
      row_count: 1,
      column_count: 2,
      columns: ['id', 'name'],
      execution_time_ms: 5,
    }
    mockExecuteNode.mockResolvedValueOnce(result)
    mockPreviewData.mockResolvedValueOnce(makePreview())

    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'csv-node',
            type: 'csv_source',
            position: { x: 0, y: 0 },
            data: {
              label: 'Orders CSV',
              tableName: 'orders_table',
              config: { file_path: '/tmp/orders.csv', original_filename: 'orders.csv' },
            },
          },
        ],
        selectedNodeId: 'csv-node',
      })
    })

    render(<NodeConfigPanel />)

    await user.click(screen.getByRole('button', { name: 'Run and Preview' }))

    await waitFor(() => {
      expect(mockExecuteNode).toHaveBeenCalledWith(expect.objectContaining({
        id: 'csv-node',
        table_name: 'orders_table',
      }))
      expect(mockPreviewData).toHaveBeenCalledWith('orders_table', 0)
    })

    expect(usePipelineStore.getState().nodeResults['csv-node']).toEqual(result)
    expect(usePipelineStore.getState().previewData).toEqual(makePreview())
  })
})
