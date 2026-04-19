import { fireEvent, render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { act } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import FlowCanvas from './FlowCanvas'
import NodeEditorModal from '../panels/NodeEditorModal'
import {
  DATABASE_CONNECTION_MIME,
  DATABASE_CONNECTION_SCOPE_MIME,
  NODE_TYPE_MIME,
} from '../../lib/dragData'
import { usePipelineStore } from '../../store/pipelineStore'
import { useSettingsStore } from '../../store/settingsStore'

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

vi.mock('@xyflow/react', async () => {
  const React = await import('react')

  return {
    ReactFlow: ({
      onInit,
      onDrop,
      onDragOver,
      onPaneClick,
      onConnect,
      onEdgesChange,
      children,
    }: {
      onInit?: (instance: { screenToFlowPosition: (position: { x: number; y: number }) => { x: number; y: number } }) => void
      onDrop?: (event: React.DragEvent) => void
      onDragOver?: (event: React.DragEvent) => void
      onPaneClick?: () => void
      onConnect?: (connection: { source?: string; target?: string }) => void
      onEdgesChange?: (changes: Array<{ id: string; type: string }>) => void
      children?: React.ReactNode
    }) => {
      React.useEffect(() => {
        onInit?.({
          screenToFlowPosition: ({ x, y }) => ({ x, y }),
        })
      }, [onInit])

      return (
        <div data-testid="flow-canvas" onDrop={onDrop} onDragOver={onDragOver} onClick={onPaneClick}>
          <button
            type="button"
            data-testid="connect-self"
            onClick={() => onConnect?.({ source: 'node-1', target: 'node-1' })}
          >
            Connect self
          </button>
          <button
            type="button"
            data-testid="connect-two-nodes"
            onClick={() => onConnect?.({ source: 'node-1', target: 'node-2' })}
          >
            Connect nodes
          </button>
          <button
            type="button"
            data-testid="remove-edge"
            onClick={() => onEdgesChange?.([{ id: 'edge-1', type: 'remove' }])}
          >
            Remove edge
          </button>
          {children}
        </div>
      )
    },
    Background: () => <div data-testid="flow-background" />,
    Controls: () => <div data-testid="flow-controls" />,
    MiniMap: () => <div data-testid="flow-minimap" />,
    applyEdgeChanges: (
      changes: Array<{ id: string; type: string }>,
      edges: Array<{ id: string }>
    ) => edges.filter((edge) => !changes.some((change) => change.type === 'remove' && change.id === edge.id)),
    Handle: () => null,
    Position: { Left: 'left', Right: 'right' },
  }
})

const mockUploadCsv = vi.fn()
const mockTestDbConnection = vi.fn()

vi.mock('../../api/client', () => ({
  uploadCsv: (...args: any[]) => mockUploadCsv(...args),
  executePipeline: vi.fn(),
  executeNode: vi.fn(),
  previewData: vi.fn(),
  previewCsvSource: vi.fn(),
  previewPreprocessedCsvSource: vi.fn(),
  deleteTable: vi.fn((..._args: any[]) => Promise.resolve({ deleted: true })),
  deletePreprocessedCsvArtifact: vi.fn((..._args: any[]) => Promise.resolve({ deleted: true })),
  savePipeline: vi.fn(),
  loadPipeline: vi.fn(),
  listPipelines: vi.fn(),
  testDbConnection: (...args: any[]) => mockTestDbConnection(...args),
}))

function renderCanvas() {
  return render(
    <>
      <FlowCanvas />
      <NodeEditorModal />
    </>
  )
}

function makeDataTransfer(values: Record<string, string>) {
  return {
    getData: (key: string) => values[key] ?? '',
    setData: vi.fn(),
    effectAllowed: 'move',
    dropEffect: 'move',
  }
}

describe('FlowCanvas', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    act(() => {
      usePipelineStore.getState().newPipeline()
      useSettingsStore.setState({
        globalDatabaseConnections: [],
        globalConnectionsLoaded: true,
        globalConnectionsLoading: false,
      })
    })
  })

  it('opens the create modal on generic drop and waits for create before mutating the canvas', async () => {
    const user = userEvent.setup()

    renderCanvas()

    fireEvent.drop(screen.getByTestId('flow-canvas'), {
      clientX: 120,
      clientY: 180,
      dataTransfer: makeDataTransfer({ [NODE_TYPE_MIME]: 'transform' }),
    })

    expect(usePipelineStore.getState().nodes).toHaveLength(0)

    const modal = screen.getByTestId('node-editor-modal')
    expect(within(modal).getByRole('heading', { name: 'Create Transform' })).toBeInTheDocument()
    expect(within(modal).getByDisplayValue('Transform')).toBeInTheDocument()

    await user.click(within(modal).getByRole('button', { name: 'Discard' }))

    expect(screen.queryByTestId('node-editor-modal')).not.toBeInTheDocument()
    expect(usePipelineStore.getState().nodes).toHaveLength(0)
  })

  it('opens the create modal for dragged database presets and creates the node with edited values', async () => {
    const user = userEvent.setup()
    let connectionId = ''

    act(() => {
      connectionId = usePipelineStore.getState().addDatabaseConnection({
        name: 'Analytics Postgres',
        db_type: 'postgres',
        host: 'localhost',
        port: 5432,
        database: 'analytics',
        user: 'user',
        password: 'secret',
      })
    })

    renderCanvas()

    fireEvent.drop(screen.getByTestId('flow-canvas'), {
      clientX: 64,
      clientY: 96,
      dataTransfer: makeDataTransfer({
        [NODE_TYPE_MIME]: 'db_source',
        [DATABASE_CONNECTION_MIME]: connectionId,
        [DATABASE_CONNECTION_SCOPE_MIME]: 'local',
      }),
    })

    const modal = screen.getByTestId('node-editor-modal')
    expect(within(modal).getByDisplayValue('Analytics Postgres')).toBeInTheDocument()
    expect(within(modal).getByPlaceholderText('Host')).toHaveValue('localhost')
    expect(within(modal).getByPlaceholderText('Database')).toHaveValue('analytics')

    fireEvent.change(within(modal).getByLabelText('Label'), { target: { value: 'Warehouse Read Replica' } })
    fireEvent.change(within(modal).getByLabelText('Table Name'), { target: { value: 'warehouse_orders' } })
    fireEvent.change(within(modal).getByLabelText('sql-editor'), { target: { value: 'SELECT * FROM orders' } })
    await user.click(within(modal).getByRole('button', { name: 'Create' }))

    const state = usePipelineStore.getState()
    expect(state.nodes).toHaveLength(1)
    const node = state.nodes[0]
    const data = node.data as Record<string, unknown>
    const config = data.config as Record<string, unknown>

    expect(data.label).toBe('Warehouse Read Replica')
    expect(data.tableName).toBe('warehouse_orders')
    expect((config.connection as Record<string, unknown>).host).toBe('localhost')
    expect(config.query).toBe('SELECT * FROM orders')
  })

  it('opens a read-only linked editor for dragged global database presets', async () => {
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
    })

    renderCanvas()

    fireEvent.drop(screen.getByTestId('flow-canvas'), {
      clientX: 64,
      clientY: 96,
      dataTransfer: makeDataTransfer({
        [NODE_TYPE_MIME]: 'db_source',
        [DATABASE_CONNECTION_MIME]: 'global-1',
        [DATABASE_CONNECTION_SCOPE_MIME]: 'global',
      }),
    })

    const modal = screen.getByTestId('node-editor-modal')
    expect(within(modal).getByDisplayValue('Shared Warehouse')).toBeInTheDocument()
    expect(within(modal).getByText('This node is linked to a global database connection. Edit it from Platform Settings.')).toBeInTheDocument()
    expect(within(modal).queryByPlaceholderText('Host')).not.toBeInTheDocument()

    fireEvent.change(within(modal).getByLabelText('Label'), { target: { value: 'Orders Global' } })
    fireEvent.change(within(modal).getByLabelText('Table Name'), { target: { value: 'orders_global' } })
    fireEvent.change(within(modal).getByLabelText('sql-editor'), { target: { value: 'SELECT * FROM orders' } })
    await user.click(within(modal).getByRole('button', { name: 'Create' }))

    const state = usePipelineStore.getState()
    expect(state.nodes).toHaveLength(1)
    const config = (state.nodes[0].data as Record<string, unknown>).config as Record<string, unknown>
    expect(config.connection_mode).toBe('global')
    expect(config.connection_source_id).toBe('global-1')
    expect(config).not.toHaveProperty('connection')
    expect(config.query).toBe('SELECT * FROM orders')
  })

  it('rejects self-loop connections', async () => {
    const user = userEvent.setup()
    act(() => {
      usePipelineStore.setState({
        nodes: [{
          id: 'node-1',
          type: 'transform',
          position: { x: 0, y: 0 },
          data: {
            label: 'Transform',
            autoLabel: 'Transform',
            labelMode: 'auto',
            tableName: 'tx_table',
            config: { sql: 'SELECT 1' },
          },
        }],
        edges: [],
      })
    })

    renderCanvas()
    await user.click(screen.getByTestId('connect-self'))

    expect(usePipelineStore.getState().edges).toEqual([])
  })

  it('removes the selected edge when Delete is pressed', async () => {
    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'node-1',
            type: 'csv_source',
            position: { x: 0, y: 0 },
            data: { label: 'CSV', autoLabel: 'CSV', labelMode: 'auto', tableName: 'src', config: { file_path: '/tmp/a.csv', original_filename: 'a.csv' } },
          },
          {
            id: 'node-2',
            type: 'transform',
            position: { x: 100, y: 0 },
            data: { label: 'Transform', autoLabel: 'Transform', labelMode: 'auto', tableName: 'tx', config: { sql: 'SELECT * FROM src' } },
          },
        ],
        edges: [{ id: 'edge-1', source: 'node-1', target: 'node-2', selected: true }],
      })
    })

    renderCanvas()
    fireEvent.keyDown(window, { key: 'Delete' })

    expect(usePipelineStore.getState().edges).toEqual([])
  })

  it('removes the selected edge when Backspace is pressed', async () => {
    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'node-1',
            type: 'csv_source',
            position: { x: 0, y: 0 },
            data: { label: 'CSV', autoLabel: 'CSV', labelMode: 'auto', tableName: 'src', config: { file_path: '/tmp/a.csv', original_filename: 'a.csv' } },
          },
          {
            id: 'node-2',
            type: 'transform',
            position: { x: 100, y: 0 },
            data: { label: 'Transform', autoLabel: 'Transform', labelMode: 'auto', tableName: 'tx', config: { sql: 'SELECT * FROM src' } },
          },
        ],
        edges: [{ id: 'edge-1', source: 'node-1', target: 'node-2', selected: true }],
      })
    })

    renderCanvas()
    fireEvent.keyDown(window, { key: 'Backspace' })

    expect(usePipelineStore.getState().edges).toEqual([])
  })
})
