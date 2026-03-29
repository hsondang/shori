import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { act } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import NodeConfigPanel from './NodeConfigPanel'
import { usePipelineStore } from '../../store/pipelineStore'
import type { NodeExecutionResult, TablePreviewData } from '../../types/pipeline'

const mockUploadCsv = vi.fn()
const mockExecuteNode = vi.fn()
const mockPreviewData = vi.fn()
const mockPreviewCsvSource = vi.fn()
const mockPreviewPreprocessedCsvSource = vi.fn()
const mockDeleteTable = vi.fn((..._args: any[]) => Promise.resolve({ deleted: true }))
const mockDeletePreprocessedCsvArtifact = vi.fn((..._args: any[]) => Promise.resolve({ deleted: true }))

vi.mock('@monaco-editor/react', () => ({
  default: ({ value, onChange }: { value: string; onChange?: (value: string) => void }) => (
    <textarea aria-label="sql-editor" value={value} onChange={(event) => onChange?.(event.target.value)} />
  ),
}))

vi.mock('../../api/client', () => ({
  uploadCsv: (...args: any[]) => mockUploadCsv(...args),
  executePipeline: vi.fn(),
  executeNode: (...args: any[]) => mockExecuteNode(...args),
  previewData: (...args: any[]) => mockPreviewData(...args),
  previewCsvSource: (...args: any[]) => mockPreviewCsvSource(...args),
  previewPreprocessedCsvSource: (...args: any[]) => mockPreviewPreprocessedCsvSource(...args),
  deleteTable: (...args: any[]) => mockDeleteTable(...args),
  deletePreprocessedCsvArtifact: (...args: any[]) => mockDeletePreprocessedCsvArtifact(...args),
  savePipeline: vi.fn(),
  loadPipeline: vi.fn(),
  listPipelines: vi.fn(),
}))

function makePreview(overrides: Partial<TablePreviewData> = {}): TablePreviewData {
  return {
    kind: 'table',
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

    expect(screen.getByTestId('node-config-panel')).toHaveAttribute('data-layout-state', 'collapsed')
    expect(screen.getByTestId('node-config-panel')).toHaveStyle({ width: '320px' })
    expect(screen.getByText('Analytics Postgres')).toBeInTheDocument()
    expect(screen.getByText('SQL Query')).toBeInTheDocument()
    expect(screen.getByLabelText('sql-editor')).toHaveValue('SELECT 1')
    expect(screen.getByRole('button', { name: 'Execute' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Edit mode' })).toBeInTheDocument()

    expect(screen.queryByText('Label')).not.toBeInTheDocument()
    expect(screen.queryByText('Table Name')).not.toBeInTheDocument()
    expect(screen.queryByText('Database Type')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /test connection/i })).not.toBeInTheDocument()
  })

  it('expands the database editor in edit mode and resets it when the selection changes', async () => {
    const user = userEvent.setup()
    render(<NodeConfigPanel />)

    await user.click(screen.getByRole('button', { name: 'Edit mode' }))
    expect(screen.getByTestId('node-config-panel')).toHaveAttribute('data-layout-state', 'expanded')
    expect(screen.getByTestId('node-config-panel')).toHaveStyle({
      width: '36vw',
      minWidth: '28rem',
      maxWidth: '44rem',
    })

    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'transform-node',
            type: 'transform',
            position: { x: 0, y: 0 },
            data: {
              label: 'Transform',
              tableName: 'transform_table',
              config: { sql: 'select * from db_table' },
            },
          },
        ],
        selectedNodeId: 'transform-node',
      })
    })

    expect(screen.getByTestId('node-config-panel')).toHaveAttribute('data-layout-state', 'collapsed')
  })

  it('executes the database node and loads preview data on success', async () => {
    const user = userEvent.setup()
    const result: NodeExecutionResult = {
      node_id: 'db-node',
      status: 'success',
      row_count: 1,
      column_count: 2,
      columns: ['id', 'name'],
      execution_time_ms: 5,
    }
    mockExecuteNode.mockResolvedValueOnce(result)
    mockPreviewData.mockResolvedValueOnce(makePreview())

    render(<NodeConfigPanel />)

    await user.click(screen.getByRole('button', { name: 'Execute' }))

    await waitFor(() => {
      expect(mockExecuteNode).toHaveBeenCalledWith(expect.objectContaining({
        id: 'db-node',
        table_name: 'db_table',
      }))
      expect(mockPreviewData).toHaveBeenCalledWith('db_table', 0)
    })

    expect(usePipelineStore.getState().nodeResults['db-node']).toEqual(result)
    expect(usePipelineStore.getState().previewData).toEqual(makePreview())
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

    expect(screen.getByRole('button', { name: 'Preview data' })).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Load data' })).toBeDisabled()
  })

  it('hides the preprocessing section until the toggle is enabled', async () => {
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

    expect(screen.getByRole('switch', { name: 'Enable preprocessing' })).toHaveAttribute('aria-checked', 'false')
    expect(screen.queryByLabelText('Script')).not.toBeInTheDocument()
    expect(screen.queryByText(/SHORI_INPUT_CSV/i)).not.toBeInTheDocument()

    await user.click(screen.getByRole('switch', { name: 'Enable preprocessing' }))

    expect(screen.getByRole('switch', { name: 'Enable preprocessing' })).toHaveAttribute('aria-checked', 'true')
    expect(screen.getByLabelText('Script')).toBeInTheDocument()
    expect(screen.getByText(/SHORI_INPUT_CSV/i)).toBeInTheDocument()
  })

  it('preserves preprocessing values when toggled off and on again', async () => {
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

    const toggle = screen.getByRole('switch', { name: 'Enable preprocessing' })
    await user.click(toggle)
    await user.selectOptions(screen.getByRole('combobox'), 'bash')
    await user.type(screen.getByLabelText('Script'), 'tail -n +3 "$1"')

    expect(screen.getByRole('combobox')).toHaveValue('bash')
    expect(screen.getByLabelText('Script')).toHaveValue('tail -n +3 "$1"')

    await user.click(screen.getByRole('switch', { name: 'Enable preprocessing' }))

    expect(screen.queryByRole('combobox')).not.toBeInTheDocument()
    expect(screen.queryByLabelText('Script')).not.toBeInTheDocument()

    await user.click(screen.getByRole('switch', { name: 'Enable preprocessing' }))

    expect(screen.getByRole('combobox')).toHaveValue('bash')
    expect(screen.getByLabelText('Script')).toHaveValue('tail -n +3 "$1"')
  })

  it('previews the raw csv without executing the node', async () => {
    const user = userEvent.setup()
    mockPreviewCsvSource.mockResolvedValueOnce({
      kind: 'csv_text',
      csv_stage: 'raw',
      rows: [['id', 'name'], ['1', 'Alice']],
      limit: 100,
      truncated: false,
      artifact_ready: false,
    })

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

    await user.click(screen.getByRole('button', { name: 'Preview data' }))

    await waitFor(() => {
      expect(mockPreviewCsvSource).toHaveBeenCalledWith('/tmp/orders.csv')
    })
    expect(mockExecuteNode).not.toHaveBeenCalled()
  })

  it('runs Preprocess and enables Load data after review', async () => {
    const user = userEvent.setup()
    mockPreviewPreprocessedCsvSource.mockResolvedValueOnce({
      kind: 'csv_text',
      csv_stage: 'preprocessed',
      rows: [['id', 'name'], ['1', 'Alice']],
      limit: 100,
      truncated: false,
      artifact_ready: true,
    })

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
              config: {
                file_path: '/tmp/orders.csv',
                original_filename: 'orders.csv',
                preprocessing: { enabled: true, runtime: 'python', script: 'print(1)' },
              },
            },
          },
        ],
        selectedNodeId: 'csv-node',
      })
    })

    render(<NodeConfigPanel />)

    expect(screen.getByRole('button', { name: 'Load data' })).toBeDisabled()

    await user.click(screen.getByRole('button', { name: 'Preprocess' }))

    await waitFor(() => {
      expect(mockPreviewPreprocessedCsvSource).toHaveBeenCalledWith(
        'csv-node',
        '/tmp/orders.csv',
        { enabled: true, runtime: 'python', script: 'print(1)' },
      )
    })

    expect(screen.getByRole('button', { name: 'Load data' })).toBeEnabled()
    expect(screen.getByText(/Reviewed preprocess output is ready to load/i)).toBeInTheDocument()
  })

  it('executes the csv node and loads table preview data on success', async () => {
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

    await user.click(screen.getByRole('button', { name: 'Load data' }))

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

  it('disables Load data when preprocessing is enabled without a script', async () => {
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
              config: {
                file_path: '/tmp/orders.csv',
                original_filename: 'orders.csv',
                preprocessing: { enabled: false, runtime: 'python', script: '' },
              },
            },
          },
        ],
        selectedNodeId: 'csv-node',
      })
    })

    render(<NodeConfigPanel />)

    await user.click(screen.getByRole('switch', { name: 'Enable preprocessing' }))

    expect(screen.getByRole('button', { name: 'Load data' })).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Preprocess' })).toBeDisabled()
    expect(screen.getByLabelText('Script')).toBeInTheDocument()
    expect(screen.getByText(/Add a preprocessing script/i)).toBeInTheDocument()
  })

  it('clears reviewed preprocess readiness when the script changes', async () => {
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
              config: {
                file_path: '/tmp/orders.csv',
                original_filename: 'orders.csv',
                preprocessing: { enabled: true, runtime: 'python', script: 'print(1)' },
              },
            },
          },
        ],
        selectedNodeId: 'csv-node',
        csvPreprocessArtifacts: {
          'csv-node': JSON.stringify({
            file_path: '/tmp/orders.csv',
            runtime: 'python',
            script: 'print(1)',
          }),
        },
      })
    })

    render(<NodeConfigPanel />)

    expect(screen.getByRole('button', { name: 'Load data' })).toBeEnabled()

    await user.clear(screen.getByLabelText('Script'))
    await user.type(screen.getByLabelText('Script'), 'print(2)')

    expect(screen.getByRole('button', { name: 'Load data' })).toBeDisabled()
    expect(screen.getByText(/Run Preprocess and review the output before loading data/i)).toBeInTheDocument()
  })

  it('shows transform Run and Preview and invokes the shared store action', async () => {
    const user = userEvent.setup()
    const runTransformPreview = vi.fn()

    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'src-node',
            type: 'csv_source',
            position: { x: 0, y: 0 },
            data: {
              label: 'Orders CSV',
              tableName: 'orders_table',
              config: { file_path: '/tmp/orders.csv', original_filename: 'orders.csv' },
            },
          },
          {
            id: 'tx-node',
            type: 'transform',
            position: { x: 200, y: 0 },
            data: {
              label: 'Orders Transform',
              tableName: 'orders_final',
              config: { sql: 'SELECT * FROM orders_table' },
            },
          },
        ],
        edges: [{ id: 'edge-1', source: 'src-node', target: 'tx-node' }],
        selectedNodeId: 'tx-node',
        runTransformPreview,
      })
    })

    render(<NodeConfigPanel />)

    expect(screen.getByRole('button', { name: 'Edit mode' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Run and Preview' })).toBeEnabled()
    expect(screen.getByText(/Missing upstream tables will prompt before running dependencies/i)).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'Run and Preview' }))

    expect(runTransformPreview).toHaveBeenCalledWith('tx-node')
  })

  it('expands the transform editor in edit mode and resets it when the selection changes', async () => {
    const user = userEvent.setup()

    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'tx-node',
            type: 'transform',
            position: { x: 200, y: 0 },
            data: {
              label: 'Orders Transform',
              tableName: 'orders_final',
              config: { sql: 'SELECT * FROM orders_table' },
            },
          },
        ],
        selectedNodeId: 'tx-node',
      })
    })

    render(<NodeConfigPanel />)

    await user.click(screen.getByRole('button', { name: 'Edit mode' }))
    expect(screen.getByTestId('node-config-panel')).toHaveAttribute('data-layout-state', 'expanded')
    expect(screen.getByTestId('node-config-panel')).toHaveStyle({
      width: '36vw',
      minWidth: '28rem',
      maxWidth: '44rem',
    })

    act(() => {
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

    expect(screen.getByTestId('node-config-panel')).toHaveAttribute('data-layout-state', 'collapsed')
  })

  it('disables transform Run and Preview when SQL is blank', () => {
    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'tx-node',
            type: 'transform',
            position: { x: 200, y: 0 },
            data: {
              label: 'Orders Transform',
              tableName: 'orders_final',
              config: { sql: '   ' },
            },
          },
        ],
        selectedNodeId: 'tx-node',
      })
    })

    render(<NodeConfigPanel />)

    expect(screen.getByRole('button', { name: 'Run and Preview' })).toBeDisabled()
  })

  it('shows transform Run and Preview as disabled while running', () => {
    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'tx-node',
            type: 'transform',
            position: { x: 200, y: 0 },
            data: {
              label: 'Orders Transform',
              tableName: 'orders_final',
              config: { sql: 'SELECT 1' },
            },
          },
        ],
        selectedNodeId: 'tx-node',
        nodeResults: {
          'tx-node': { node_id: 'tx-node', status: 'running' },
        },
      })
    })

    render(<NodeConfigPanel />)

    expect(screen.getByRole('button', { name: 'Running...' })).toBeDisabled()
  })
})
