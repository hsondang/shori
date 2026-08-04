import { fireEvent, render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { act } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import NodeConfigPanel from './NodeConfigPanel'
import NodeEditorModal from './NodeEditorModal'
import { usePipelineStore } from '../../store/pipelineStore'
import { useSettingsStore } from '../../store/settingsStore'

const mockUploadCsv = vi.fn()
const mockUploadExcel = vi.fn()
const mockMaterializeExcelSheet = vi.fn()
const mockExecuteNode = vi.fn()
const mockPreviewData = vi.fn()
const mockPreviewCsvSource = vi.fn()
const mockPreviewPreprocessedCsvSource = vi.fn()
const mockAbortExecutionRun = vi.fn()
const mockDeleteTable = vi.fn((..._args: any[]) => Promise.resolve({ deleted: true }))
const mockDeletePreprocessedCsvArtifact = vi.fn((..._args: any[]) => Promise.resolve({ deleted: true }))
const mockTestDbConnection = vi.fn()

vi.mock('@monaco-editor/react', () => ({
  default: ({
    value,
    onChange,
    height,
  }: {
    value: string
    onChange?: (value: string) => void
    height?: string
  }) => (
    <div data-testid="sql-editor-shell" data-height={height}>
      <textarea aria-label="sql-editor" value={value} onChange={(event) => onChange?.(event.target.value)} />
    </div>
  ),
}))

vi.mock('../../api/client', () => ({
  uploadCsv: (...args: any[]) => mockUploadCsv(...args),
  uploadExcel: (...args: any[]) => mockUploadExcel(...args),
  materializeExcelSheet: (...args: any[]) => mockMaterializeExcelSheet(...args),
  executePipeline: vi.fn(),
  executeNode: (...args: any[]) => mockExecuteNode(...args),
  previewData: (...args: any[]) => mockPreviewData(...args),
  previewCsvSource: (...args: any[]) => mockPreviewCsvSource(...args),
  previewPreprocessedCsvSource: (...args: any[]) => mockPreviewPreprocessedCsvSource(...args),
  abortExecutionRun: (...args: any[]) => mockAbortExecutionRun(...args),
  deleteTable: (...args: any[]) => mockDeleteTable(...args),
  deletePreprocessedCsvArtifact: (...args: any[]) => mockDeletePreprocessedCsvArtifact(...args),
  savePipeline: vi.fn(),
  loadPipeline: vi.fn(),
  listPipelines: vi.fn(),
  testDbConnection: (...args: any[]) => mockTestDbConnection(...args),
}))

function renderPanel() {
  return render(
    <>
      <NodeConfigPanel />
      <NodeEditorModal />
    </>
  )
}

describe('NodeConfigPanel', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.spyOn(window, 'confirm').mockReturnValue(true)
    Object.defineProperty(window, 'innerWidth', {
      configurable: true,
      value: 1600,
      writable: true,
    })
    act(() => {
      usePipelineStore.getState().newPipeline()
    })
  })

  it.each([
    {
      type: 'csv_source',
      label: 'Orders CSV',
      config: { file_path: '/tmp/orders.csv', original_filename: 'orders.csv' },
    },
    {
      type: 'excel_source',
      label: 'Orders Workbook',
      config: {
        file_path: '/tmp/orders.xlsx',
        original_filename: 'orders.xlsx',
        sheet_names: ['Orders'],
        sheets: [],
        selected_sheet: 'Orders',
        materialized_csv_path: '/tmp/orders_Orders.csv',
        materialized_csv_filename: 'orders_Orders.csv',
      },
    },
    {
      type: 'db_source',
      label: 'Analytics DB',
      config: {
        db_type: 'postgres',
        connection: { host: 'localhost', port: 5432, database: 'analytics', user: 'user', password: 'secret' },
        query: 'SELECT 1',
      },
    },
    {
      type: 'transform',
      label: 'Transform Orders',
      config: { sql: 'select * from orders_table' },
    },
    {
      type: 'export',
      label: 'Export Orders',
      config: { format: 'csv' },
    },
  ])('shows the actions menu for $type nodes', async ({ type, label, config }) => {
    const user = userEvent.setup()

    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'node-1',
            type,
            position: { x: 0, y: 0 },
            data: {
              label,
              autoLabel: label,
              labelMode: 'auto',
              tableName: 'table_1',
              config,
            },
          },
        ],
        selectedNodeId: 'node-1',
      })
    })

    renderPanel()

    await user.click(screen.getByRole('button', { name: `More options for ${label}` }))

    const menu = screen.getByTestId('node-config-actions-menu')
    expect(within(menu).getByRole('button', { name: 'Edit' })).toBeInTheDocument()
    expect(within(menu).getByRole('button', { name: 'Delete' })).toBeInTheDocument()
  })

  it('renders a CSV node saved with the legacy preprocessing shape without crashing', () => {
    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'legacy-csv',
            type: 'csv_source',
            position: { x: 0, y: 0 },
            data: {
              label: 'Legacy CSV',
              autoLabel: 'Legacy CSV',
              labelMode: 'auto',
              tableName: 'prod',
              config: {
                file_path: '/tmp/prod.csv',
                original_filename: 'prod.csv',
                // Pre-refactor shape: no script_path, has runtime/script.
                preprocessing: { enabled: true, runtime: 'python', script: 'print(1)' },
              },
            },
          },
        ],
        selectedNodeId: 'legacy-csv',
      })
    })

    renderPanel()

    expect(screen.getByText('prod.csv')).toBeInTheDocument()
    expect(screen.getByText('Load to memory')).toBeInTheDocument()
  })

  it('uploads an excel workbook and selects a sheet for native read_xlsx', async () => {
    const user = userEvent.setup()
    mockUploadExcel.mockResolvedValue({
      file_path: '/tmp/orders.xlsx',
      filename: 'orders.xlsx',
      sheet_names: ['Orders', 'Summary'],
    })

    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'excel-node',
            type: 'excel_source',
            position: { x: 0, y: 0 },
            data: {
              label: 'Excel Source',
              autoLabel: 'Excel Source',
              labelMode: 'auto',
              tableName: 'excel_table',
              config: {
                file_path: '',
                original_filename: '',
                sheet_names: [],
                selected_sheet: '',
                load_mode: 'in_memory',
                header: true,
              },
            },
          },
        ],
        selectedNodeId: 'excel-node',
      })
    })

    const { container } = renderPanel()
    const fileInput = container.querySelector('input[type="file"]') as HTMLInputElement
    await user.upload(fileInput, new File(['workbook'], 'orders.xlsx', {
      type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    }))

    expect(await screen.findByText('orders.xlsx')).toBeInTheDocument()
    expect(mockMaterializeExcelSheet).not.toHaveBeenCalled()
    expect(screen.getByLabelText('Sheet')).toHaveValue('Orders')

    await user.selectOptions(screen.getByLabelText('Sheet'), 'Summary')

    const config = (usePipelineStore.getState().nodes[0].data as Record<string, unknown>).config as Record<string, unknown>
    expect(config).toMatchObject({
      file_path: '/tmp/orders.xlsx',
      selected_sheet: 'Summary',
      sheet_names: ['Orders', 'Summary'],
    })
  })

  it('opens the shared modal from the actions menu and saves edits with invalidation', async () => {
    const user = userEvent.setup()

    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'transform-node',
            type: 'transform',
            position: { x: 0, y: 0 },
            data: {
              label: 'Transform Orders',
              autoLabel: 'Transform',
              labelMode: 'custom',
              tableName: 'orders_final',
              config: { sql: 'select * from orders_table' },
            },
          },
        ],
        selectedNodeId: 'transform-node',
        previewTabsByNodeId: {
          'transform-node': {
            nodeId: 'transform-node',
            tableNameAtLoad: 'orders_final',
            data: null,
            loading: false,
            error: null,
            isStale: false,
          },
        },
        previewTabOrder: ['transform-node'],
        activePreviewTarget: { kind: 'tab', nodeId: 'transform-node' },
      })
    })

    renderPanel()

    await user.click(screen.getByRole('button', { name: 'More options for Transform Orders' }))
    await user.click(screen.getByRole('button', { name: 'Edit' }))

    const modal = screen.getByTestId('node-editor-modal')
    expect(within(modal).getByDisplayValue('Transform Orders')).toBeInTheDocument()
    expect(within(modal).getByDisplayValue('orders_final')).toBeInTheDocument()
    expect(within(modal).getByLabelText('sql-editor')).toHaveValue('select * from orders_table')

    await user.clear(within(modal).getByLabelText('Label'))
    await user.type(within(modal).getByLabelText('Label'), 'Transform Curated')
    await user.clear(within(modal).getByLabelText('Table Name'))
    await user.type(within(modal).getByLabelText('Table Name'), 'orders_curated')
    await user.clear(within(modal).getByLabelText('sql-editor'))
    await user.type(within(modal).getByLabelText('sql-editor'), 'select id from orders_table')
    await user.click(within(modal).getByRole('button', { name: 'Save' }))

    const updated = usePipelineStore.getState().nodes[0].data as Record<string, unknown>
    expect(updated.label).toBe('Transform Curated')
    expect(updated.tableName).toBe('orders_curated')
    expect((updated.config as Record<string, unknown>).sql).toBe('select id from orders_table')
    // The table is no longer dropped client-side on edit; it's marked stale and
    // reconciled server-side on save.
    expect(mockDeleteTable).not.toHaveBeenCalled()
    expect(usePipelineStore.getState().previewTabsByNodeId['transform-node']?.isStale).toBe(true)
  })

  it('saves database query edits with spaces from the shared modal', async () => {
    const user = userEvent.setup()

    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'db-node',
            type: 'db_source',
            position: { x: 0, y: 0 },
            data: {
              label: 'Analytics DB',
              autoLabel: 'Analytics DB',
              labelMode: 'auto',
              tableName: 'analytics_table',
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

    renderPanel()

    await user.click(screen.getByRole('button', { name: 'More options for Analytics DB' }))
    await user.click(screen.getByRole('button', { name: 'Edit' }))

    const modal = screen.getByTestId('node-editor-modal')
    await user.clear(within(modal).getByLabelText('sql-editor'))
    await user.type(within(modal).getByLabelText('sql-editor'), 'SELECT id FROM analytics_table')
    await user.click(within(modal).getByRole('button', { name: 'Save' }))

    const updated = usePipelineStore.getState().nodes[0].data as Record<string, unknown>
    expect((updated.config as Record<string, unknown>).query).toBe('SELECT id FROM analytics_table')
  })

  it('shows oracle advanced configuration in the shared modal and saves fetch config', async () => {
    const user = userEvent.setup()

    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'oracle-node',
            type: 'db_source',
            position: { x: 0, y: 0 },
            data: {
              label: 'Warehouse Oracle',
              autoLabel: 'Warehouse Oracle',
              labelMode: 'auto',
              tableName: 'warehouse_table',
              config: {
                db_type: 'oracle',
                connection: {
                  host: 'orahost',
                  port: 1521,
                  service_name: 'ORCL',
                  user: 'user',
                  password: 'secret',
                },
                query: 'SELECT 1 FROM dual',
              },
            },
          },
        ],
        selectedNodeId: 'oracle-node',
      })
    })

    renderPanel()

    await user.click(screen.getByRole('button', { name: 'More options for Warehouse Oracle' }))
    await user.click(screen.getByRole('button', { name: 'Edit' }))

    const modal = screen.getByTestId('node-editor-modal')
    expect(within(modal).getByText('Advanced Configuration')).toBeInTheDocument()
    expect(within(modal).getByLabelText('Fetch Mode')).toHaveValue('fetchall')
    expect(within(modal).getByLabelText('Arraysize')).toHaveValue(100)
    expect(within(modal).getByLabelText('Prefetchrows')).toHaveValue(2)

    await user.selectOptions(within(modal).getByLabelText('Fetch Mode'), 'fetchmany')
    fireEvent.change(within(modal).getByLabelText('Arraysize'), { target: { value: '1000' } })
    fireEvent.change(within(modal).getByLabelText('Prefetchrows'), { target: { value: '5' } })
    await user.click(within(modal).getByRole('button', { name: 'Save' }))

    const updated = usePipelineStore.getState().nodes[0].data as Record<string, unknown>
    expect((updated.config as Record<string, unknown>).fetch_config).toEqual({
      mode: 'fetchmany',
      arraysize: 1000,
      prefetchrows: 5,
    })
  })

  it('hides oracle advanced configuration for postgres and resets oracle fetch config when switching db type', async () => {
    const user = userEvent.setup()

    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'db-node',
            type: 'db_source',
            position: { x: 0, y: 0 },
            data: {
              label: 'Analytics DB',
              autoLabel: 'Analytics DB',
              labelMode: 'auto',
              tableName: 'analytics_table',
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

    renderPanel()

    await user.click(screen.getByRole('button', { name: 'More options for Analytics DB' }))
    await user.click(screen.getByRole('button', { name: 'Edit' }))

    const modal = screen.getByTestId('node-editor-modal')
    expect(within(modal).queryByText('Advanced Configuration')).not.toBeInTheDocument()

    await user.selectOptions(within(modal).getByLabelText('Database Type'), 'oracle')
    expect(within(modal).getByText('Advanced Configuration')).toBeInTheDocument()
    expect(within(modal).getByLabelText('Arraysize')).toHaveValue(100)
    expect(within(modal).getByLabelText('Prefetchrows')).toHaveValue(2)

    await user.selectOptions(within(modal).getByLabelText('Database Type'), 'postgres')
    expect(within(modal).queryByText('Advanced Configuration')).not.toBeInTheDocument()
    await user.click(within(modal).getByRole('button', { name: 'Save' }))

    const updated = usePipelineStore.getState().nodes[0].data as Record<string, unknown>
    expect((updated.config as Record<string, unknown>).db_type).toBe('postgres')
    expect(updated.config).not.toHaveProperty('fetch_config')
  })

  it('blocks saving invalid oracle fetch settings in the shared modal', async () => {
    const user = userEvent.setup()

    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'oracle-node',
            type: 'db_source',
            position: { x: 0, y: 0 },
            data: {
              label: 'Warehouse Oracle',
              autoLabel: 'Warehouse Oracle',
              labelMode: 'auto',
              tableName: 'warehouse_table',
              config: {
                db_type: 'oracle',
                connection: {
                  host: 'orahost',
                  port: 1521,
                  service_name: 'ORCL',
                  user: 'user',
                  password: 'secret',
                },
                query: 'SELECT 1 FROM dual',
              },
            },
          },
        ],
        selectedNodeId: 'oracle-node',
      })
    })

    renderPanel()

    await user.click(screen.getByRole('button', { name: 'More options for Warehouse Oracle' }))
    await user.click(screen.getByRole('button', { name: 'Edit' }))

    const modal = screen.getByTestId('node-editor-modal')
    fireEvent.change(within(modal).getByLabelText('Arraysize'), { target: { value: '0' } })

    expect(within(modal).getByText('Arraysize must be an integer greater than or equal to 1.')).toBeInTheDocument()
    expect(within(modal).getByRole('button', { name: 'Save' })).toBeDisabled()
  })

  it('keeps inline database query editing and edit mode in the sidebar', async () => {
    const user = userEvent.setup()

    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'db-node',
            type: 'db_source',
            position: { x: 0, y: 0 },
            data: {
              label: 'Analytics DB',
              autoLabel: 'Analytics DB',
              labelMode: 'auto',
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

    renderPanel()

    expect(screen.getByText('SQL Query')).toBeInTheDocument()
    expect(screen.getByLabelText('sql-editor')).toHaveValue('SELECT 1')
    expect(screen.getByRole('button', { name: 'Execute' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Edit mode' })).toBeInTheDocument()
    expect(screen.getByTestId('node-config-panel')).toHaveAttribute('data-layout-state', 'collapsed')
    expect(screen.getByTestId('node-config-panel')).toHaveStyle({ width: '320px' })

    await user.click(screen.getByRole('button', { name: 'Edit mode' }))

    expect(screen.getByTestId('node-config-panel')).toHaveAttribute('data-layout-state', 'expanded')
    expect(screen.getByTestId('node-config-panel')).toHaveStyle({ width: '576px' })

    await user.clear(screen.getByLabelText('sql-editor'))
    await user.type(screen.getByLabelText('sql-editor'), 'SELECT id FROM events')

    const updated = usePipelineStore.getState().nodes[0].data as Record<string, unknown>
    expect((updated.config as Record<string, unknown>).query).toBe('SELECT id FROM events')
  })

  it('hides table name, load mode, and description for a database node in edit mode', async () => {
    const user = userEvent.setup()

    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'db-node',
            type: 'db_source',
            position: { x: 0, y: 0 },
            data: {
              label: 'Analytics DB',
              autoLabel: 'Analytics DB',
              labelMode: 'auto',
              tableName: 'db_table',
              description: 'Nightly analytics pull',
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

    renderPanel()

    expect(screen.getByText('db_table')).toBeInTheDocument()
    expect(screen.getByText('Default load mode')).toBeInTheDocument()
    expect(screen.getByText('Description')).toBeInTheDocument()

    expect(screen.getByText('Connection')).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'Edit mode' }))

    expect(screen.queryByText('db_table')).not.toBeInTheDocument()
    expect(screen.queryByText('Default load mode')).not.toBeInTheDocument()
    expect(screen.queryByText('Description')).not.toBeInTheDocument()
    // Connection and Source collapse too, leaving the SQL editor maximum room.
    expect(screen.queryByText('Connection')).not.toBeInTheDocument()
    expect(screen.getByLabelText('sql-editor')).toHaveValue('SELECT 1')
  })

  it('shows Abort for a busy database node and aborts the tracked execution', async () => {
    const user = userEvent.setup()
    mockAbortExecutionRun.mockResolvedValueOnce({
      execution_id: 'exec-1',
      kind: 'node',
      status: 'cancelled',
      started_at: '2026-04-08T10:00:00Z',
      finished_at: '2026-04-08T10:00:02Z',
      node_results: {
        'db-node': {
          node_id: 'db-node',
          status: 'cancelled',
          error: 'Execution aborted by user.',
          started_at: '2026-04-08T10:00:00Z',
          finished_at: '2026-04-08T10:00:02Z',
        },
      },
    })

    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'db-node',
            type: 'db_source',
            position: { x: 0, y: 0 },
            data: {
              label: 'Analytics DB',
              autoLabel: 'Analytics DB',
              labelMode: 'auto',
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
        nodeResults: {
          'db-node': {
            node_id: 'db-node',
            status: 'connecting',
            started_at: '2026-04-08T10:00:00Z',
          },
        },
        activeExecutions: {
          'exec-1': {
            execution_id: 'exec-1',
            kind: 'node',
            status: 'running',
            started_at: '2026-04-08T10:00:00Z',
            node_results: {
              'db-node': {
                node_id: 'db-node',
                status: 'connecting',
                started_at: '2026-04-08T10:00:00Z',
              },
            },
          },
        },
        activeExecutionIdByNodeId: {
          'db-node': 'exec-1',
        },
        selectedNodeId: 'db-node',
      })
    })

    renderPanel()

    expect(screen.getByRole('button', { name: 'Abort' })).toBeInTheDocument()
    expect(screen.getByText('Connecting')).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'Abort' }))

    expect(mockAbortExecutionRun).toHaveBeenCalledWith('exec-1')
  })

  it('keeps inline transform query editing and run controls in the sidebar', async () => {
    const user = userEvent.setup()

    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'upstream-node',
            type: 'db_source',
            position: { x: 0, y: 0 },
            data: {
              label: 'Upstream',
              autoLabel: 'Upstream',
              labelMode: 'auto',
              tableName: 'orders_table',
              config: {
                db_type: 'postgres',
                connection: {
                  host: 'localhost',
                  port: 5432,
                  database: 'analytics',
                  user: 'user',
                  password: 'secret',
                },
                query: 'SELECT * FROM orders',
              },
            },
          },
          {
            id: 'transform-node',
            type: 'transform',
            position: { x: 0, y: 0 },
            data: {
              label: 'Transform Orders',
              autoLabel: 'Transform',
              labelMode: 'custom',
              tableName: 'orders_final',
              config: { sql: 'select * from orders_table' },
            },
          },
        ],
        edges: [
          {
            id: 'edge-1',
            source: 'upstream-node',
            target: 'transform-node',
          },
        ],
        selectedNodeId: 'transform-node',
      })
    })

    renderPanel()

    expect(screen.getByText('Available Tables')).toBeInTheDocument()
    expect(screen.getByText('orders_table')).toBeInTheDocument()
    expect(screen.getByLabelText('sql-editor')).toHaveValue('select * from orders_table')
    expect(screen.getByRole('button', { name: 'Run and Preview' })).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'Edit mode' }))
    expect(screen.getByTestId('node-config-panel')).toHaveAttribute('data-layout-state', 'expanded')

    await user.clear(screen.getByLabelText('sql-editor'))
    await user.type(screen.getByLabelText('sql-editor'), 'select id from orders_table')

    const updated = usePipelineStore.getState().nodes[1].data as Record<string, unknown>
    expect((updated.config as Record<string, unknown>).sql).toBe('select id from orders_table')
  })

  it('hides table name, load mode, and description for a transform node in edit mode', async () => {
    const user = userEvent.setup()

    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'transform-node',
            type: 'transform',
            position: { x: 0, y: 0 },
            data: {
              label: 'Transform Orders',
              autoLabel: 'Transform',
              labelMode: 'custom',
              tableName: 'orders_final',
              description: 'Curated orders',
              config: { sql: 'select * from orders_table' },
            },
          },
        ],
        selectedNodeId: 'transform-node',
      })
    })

    renderPanel()

    expect(screen.getByText('orders_final')).toBeInTheDocument()
    expect(screen.getByText('Default load mode')).toBeInTheDocument()
    expect(screen.getByText('Description')).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'Edit mode' }))

    expect(screen.queryByText('orders_final')).not.toBeInTheDocument()
    expect(screen.queryByText('Default load mode')).not.toBeInTheDocument()
    expect(screen.queryByText('Description')).not.toBeInTheDocument()
    // The live preview button and the SQL editor stay available while editing.
    expect(screen.getByRole('button', { name: 'Preview (live, no table written)' })).toBeInTheDocument()
    expect(screen.getByLabelText('sql-editor')).toHaveValue('select * from orders_table')
  })

  it('resizes the panel horizontally from the left-edge drag handle', () => {
    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'export-node',
            type: 'export',
            position: { x: 0, y: 0 },
            data: {
              label: 'Export Orders',
              autoLabel: 'Export',
              labelMode: 'custom',
              tableName: 'orders_export',
              config: { format: 'csv' },
            },
          },
        ],
        selectedNodeId: 'export-node',
      })
    })

    renderPanel()

    const panel = screen.getByTestId('node-config-panel')
    const resizeHandle = screen.getByTestId('node-config-panel-resize-handle')

    expect(panel).toHaveStyle({ width: '320px' })

    fireEvent.mouseDown(resizeHandle, { clientX: 900 })
    fireEvent.mouseMove(window, { clientX: 780 })
    fireEvent.mouseUp(window)

    expect(panel).toHaveStyle({ width: '440px' })
  })

  it('clamps resized widths to the configured min and max bounds', async () => {
    const user = userEvent.setup()

    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'db-node',
            type: 'db_source',
            position: { x: 0, y: 0 },
            data: {
              label: 'Analytics DB',
              autoLabel: 'Analytics DB',
              labelMode: 'auto',
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

    renderPanel()

    const panel = screen.getByTestId('node-config-panel')
    const resizeHandle = screen.getByTestId('node-config-panel-resize-handle')

    fireEvent.mouseDown(resizeHandle, { clientX: 900 })
    fireEvent.mouseMove(window, { clientX: 100 })
    fireEvent.mouseUp(window)
    expect(panel).toHaveStyle({ width: '704px' })

    fireEvent.mouseDown(resizeHandle, { clientX: 900 })
    fireEvent.mouseMove(window, { clientX: 1500 })
    fireEvent.mouseUp(window)
    expect(panel).toHaveStyle({ width: '320px' })

    await user.click(screen.getByRole('button', { name: 'Edit mode' }))
    expect(panel).toHaveStyle({ width: '576px' })

    fireEvent.mouseDown(resizeHandle, { clientX: 900 })
    fireEvent.mouseMove(window, { clientX: 1500 })
    fireEvent.mouseUp(window)
    expect(panel).toHaveStyle({ width: '448px' })
  })

  it('remembers independent widths for collapsed and expanded query modes', async () => {
    const user = userEvent.setup()

    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'db-node',
            type: 'db_source',
            position: { x: 0, y: 0 },
            data: {
              label: 'Analytics DB',
              autoLabel: 'Analytics DB',
              labelMode: 'auto',
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

    renderPanel()

    const panel = screen.getByTestId('node-config-panel')
    const resizeHandle = screen.getByTestId('node-config-panel-resize-handle')

    fireEvent.mouseDown(resizeHandle, { clientX: 900 })
    fireEvent.mouseMove(window, { clientX: 800 })
    fireEvent.mouseUp(window)
    expect(panel).toHaveStyle({ width: '420px' })

    await user.click(screen.getByRole('button', { name: 'Edit mode' }))
    expect(panel).toHaveStyle({ width: '576px' })

    fireEvent.mouseDown(resizeHandle, { clientX: 900 })
    fireEvent.mouseMove(window, { clientX: 826 })
    fireEvent.mouseUp(window)
    expect(panel).toHaveStyle({ width: '650px' })

    await user.click(screen.getByRole('button', { name: 'Edit mode' }))
    expect(panel).toHaveStyle({ width: '420px' })

    await user.click(screen.getByRole('button', { name: 'Edit mode' }))
    expect(panel).toHaveStyle({ width: '650px' })
  })

  it('prompts before deleting and only deletes after confirmation', async () => {
    const user = userEvent.setup()
    const confirmSpy = vi.spyOn(window, 'confirm')

    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'export-node',
            type: 'export',
            position: { x: 0, y: 0 },
            data: {
              label: 'Export Orders',
              autoLabel: 'Export',
              labelMode: 'custom',
              tableName: 'orders_export',
              config: { format: 'csv' },
            },
          },
        ],
        selectedNodeId: 'export-node',
      })
    })

    renderPanel()

    confirmSpy.mockReturnValueOnce(false)
    await user.click(screen.getByRole('button', { name: 'More options for Export Orders' }))
    await user.click(screen.getByRole('button', { name: 'Delete' }))
    expect(usePipelineStore.getState().nodes).toHaveLength(1)

    confirmSpy.mockReturnValueOnce(true)
    await user.click(screen.getByRole('button', { name: 'More options for Export Orders' }))
    await user.click(screen.getByRole('button', { name: 'Delete' }))

    expect(confirmSpy).toHaveBeenCalledWith('Delete "Export Orders"? This cannot be undone.')
    expect(usePipelineStore.getState().nodes).toHaveLength(0)
  })

  describe('export to a database', () => {
    const ORACLE_APPROVED = {
      id: 'ora-1', name: 'Oracle Prod', db_type: 'oracle' as const, host: 'h', port: 1521,
      service_name: 'KM', user: 'u', password: 'p', allow_export: true,
    }
    const ORACLE_NOT_APPROVED = { ...ORACLE_APPROVED, id: 'ora-2', name: 'Oracle Dev', allow_export: false }
    const POSTGRES = {
      id: 'pg-1', name: 'Warehouse', db_type: 'postgres' as const, host: 'h', port: 5432,
      database: 'w', user: 'u', password: 'p',
    }

    function setupExportNode(config: Record<string, unknown>, connections = [ORACLE_APPROVED]) {
      act(() => {
        useSettingsStore.setState({ globalDatabaseConnections: connections as never })
        usePipelineStore.setState({
          nodes: [
            {
              id: 'src', type: 'csv_source', position: { x: 0, y: 0 },
              data: { label: 'Orders', autoLabel: 'Orders', labelMode: 'auto', tableName: 'orders', config: {} },
            },
            {
              id: 'export-node', type: 'export', position: { x: 200, y: 0 },
              data: { label: 'Export Orders', autoLabel: 'Export', labelMode: 'auto', tableName: 'export_1', config },
            },
          ],
          edges: [{ id: 'e1', source: 'src', target: 'export-node' }],
          selectedNodeId: 'export-node',
        })
      })
    }

    it('lists only export-approved oracle connections as destinations', () => {
      setupExportNode({ format: 'csv' }, [ORACLE_APPROVED, ORACLE_NOT_APPROVED, POSTGRES] as never)
      renderPanel()

      const select = screen.getByLabelText('Destination') as HTMLSelectElement
      const options = Array.from(select.options).map((option) => option.textContent)
      expect(options).toContain('Oracle Prod')
      expect(options).not.toContain('Oracle Dev')
      expect(options).not.toContain('Warehouse')
    })

    it('switches to the database destination and records the connection', async () => {
      const user = userEvent.setup()
      setupExportNode({ format: 'csv' })
      renderPanel()

      await user.selectOptions(screen.getByLabelText('Destination'), 'db:ora-1')

      const config = usePipelineStore.getState().nodes.find((n) => n.id === 'export-node')!.data.config as Record<string, unknown>
      expect(config.destination).toBe('database')
      expect(config.connection_source_id).toBe('ora-1')
    })

    it('keeps a revoked connection visible instead of silently dropping the target', () => {
      setupExportNode(
        { format: 'csv', destination: 'database', connection_source_id: 'ora-2', target_table: 'S.T' },
        [ORACLE_APPROVED, ORACLE_NOT_APPROVED] as never,
      )
      renderPanel()

      expect(screen.getByText('Oracle Dev (exports not enabled)')).toBeInTheDocument()
    })

    it('requires a well-formed SCHEMA.TABLE before the export button enables', async () => {
      const user = userEvent.setup()
      setupExportNode({ format: 'csv', destination: 'database', connection_source_id: 'ora-1' })
      renderPanel()

      const exportButton = screen.getByRole('button', { name: 'Export' })
      expect(exportButton).toBeDisabled()

      await user.type(screen.getByLabelText(/target table/i), 'orders')
      expect(screen.getByRole('button', { name: 'Export' })).toBeDisabled()
      expect(screen.getByText('Use the form SCHEMA.TABLE_NAME.')).toBeInTheDocument()

      await user.clear(screen.getByLabelText(/target table/i))
      await user.type(screen.getByLabelText(/target table/i), 'SALES.ORDERS')
      expect(screen.getByRole('button', { name: 'Export' })).toBeEnabled()
    })

    it('hides the SQL editor until the query toggle is turned on', async () => {
      const user = userEvent.setup()
      setupExportNode({
        format: 'csv', destination: 'database', connection_source_id: 'ora-1', target_table: 'SALES.ORDERS',
      })
      renderPanel()

      expect(screen.queryByLabelText('sql-editor')).not.toBeInTheDocument()

      await user.click(screen.getByRole('switch', { name: /use a sql query/i }))

      expect(screen.getByLabelText('sql-editor')).toBeInTheDocument()
      // Seeded from the upstream table so there is something runnable to edit.
      const config = usePipelineStore.getState().nodes.find((n) => n.id === 'export-node')!.data.config as Record<string, unknown>
      expect(config.sql).toBe('SELECT * FROM orders')
    })

    it('does not overwrite an existing query when the toggle is turned back on', async () => {
      const user = userEvent.setup()
      setupExportNode({
        format: 'csv', destination: 'database', connection_source_id: 'ora-1',
        target_table: 'SALES.ORDERS', use_sql: true, sql: 'SELECT id FROM orders',
      })
      renderPanel()

      await user.click(screen.getByRole('switch', { name: /use a sql query/i }))
      await user.click(screen.getByRole('switch', { name: /use a sql query/i }))

      const config = usePipelineStore.getState().nodes.find((n) => n.id === 'export-node')!.data.config as Record<string, unknown>
      expect(config.sql).toBe('SELECT id FROM orders')
    })

    it('previews without validating by default, and validates from the dropdown', async () => {
      const user = userEvent.setup()
      const startLivePreview = vi.fn()
      const validateDatabaseExport = vi.fn()
      setupExportNode({
        format: 'csv', destination: 'database', connection_source_id: 'ora-1', target_table: 'SALES.ORDERS',
      })
      act(() => { usePipelineStore.setState({ startLivePreview, validateDatabaseExport }) })
      renderPanel()

      // The plain button never reaches the destination database.
      await user.click(screen.getByRole('button', { name: 'Preview' }))
      expect(startLivePreview).toHaveBeenCalledWith('export-node')
      expect(validateDatabaseExport).not.toHaveBeenCalled()

      await user.click(screen.getByRole('button', { name: 'More preview options' }))
      await user.click(screen.getByRole('menuitem', { name: 'Preview and validate target' }))
      expect(validateDatabaseExport).toHaveBeenCalledWith('export-node')
    })

    it('runs the export and shows validation problems', async () => {
      const user = userEvent.setup()
      const runDatabaseExport = vi.fn()
      setupExportNode({
        format: 'csv', destination: 'database', connection_source_id: 'ora-1', target_table: 'SALES.ORDERS',
      })
      act(() => {
        usePipelineStore.setState({
          runDatabaseExport,
          databaseExportValidationByNodeId: {
            'export-node': {
              status: 'done',
              report: {
                target_table: 'SALES.ORDERS',
                target_exists: true,
                columns: [
                  { source_column: 'ghost', source_type: 'VARCHAR', target_column: null, target_type: null, status: 'missing_in_target', message: null },
                ],
                unmapped_target_columns: [],
                errors: ['Column "ghost" does not exist in the target table'],
                warnings: [],
                ok: false,
              },
            },
          },
        })
      })
      renderPanel()

      expect(screen.getByText('SALES.ORDERS cannot accept this query')).toBeInTheDocument()
      expect(screen.getByText(/Column "ghost" does not exist/)).toBeInTheDocument()

      await user.click(screen.getByRole('button', { name: 'Export' }))
      expect(runDatabaseExport).toHaveBeenCalledWith('export-node')
    })

    it('keeps the compact panel for local and AI workspace destinations', () => {
      setupExportNode({ format: 'csv', destination: 'local', output_path: '/tmp/out.csv' })
      renderPanel()

      expect(screen.getByLabelText('Output path')).toBeInTheDocument()
      expect(screen.queryByLabelText(/target table/i)).not.toBeInTheDocument()
      expect(screen.queryByRole('button', { name: 'Preview' })).not.toBeInTheDocument()
    })
  })
})
