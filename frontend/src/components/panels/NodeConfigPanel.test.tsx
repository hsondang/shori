import { fireEvent, render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { act } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import NodeConfigPanel from './NodeConfigPanel'
import NodeEditorModal from './NodeEditorModal'
import { usePipelineStore } from '../../store/pipelineStore'

const mockUploadCsv = vi.fn()
const mockExecuteNode = vi.fn()
const mockPreviewData = vi.fn()
const mockPreviewCsvSource = vi.fn()
const mockPreviewPreprocessedCsvSource = vi.fn()
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
    expect(mockDeleteTable).toHaveBeenCalledWith('orders_final')
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

  it('disables database execution while connecting and shows the connecting label', () => {
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
        selectedNodeId: 'db-node',
      })
    })

    renderPanel()

    expect(screen.getByRole('button', { name: 'Connecting...' })).toBeDisabled()
    expect(screen.getByText('Connecting')).toBeInTheDocument()
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
})
