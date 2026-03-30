import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, expect, beforeEach, vi } from 'vitest'
import { act } from 'react'
import DataPreviewPanel from './DataPreviewPanel'
import { usePipelineStore } from '../../store/pipelineStore'
import type { CsvTextPreviewData, TablePreviewData } from '../../types/pipeline'

vi.mock('../../api/client', () => ({
  executePipeline: vi.fn(),
  previewData: vi.fn(),
  previewCsvSource: vi.fn(),
  previewPreprocessedCsvSource: vi.fn(),
  deletePreprocessedCsvArtifact: vi.fn((..._args: any[]) => Promise.resolve({ deleted: true })),
  savePipeline: vi.fn(),
  loadPipeline: vi.fn(),
  listPipelines: vi.fn(),
}))

function makeTablePreview(overrides: Partial<TablePreviewData> = {}): TablePreviewData {
  return {
    kind: 'table',
    columns: ['id', 'name'],
    column_types: ['INTEGER', 'VARCHAR'],
    rows: [[1, 'Alice'], [2, 'Bob'], [3, 'Carol']],
    total_rows: 3,
    offset: 0,
    limit: 100,
    ...overrides,
  }
}

function makeCsvPreview(overrides: Partial<CsvTextPreviewData> = {}): CsvTextPreviewData {
  return {
    kind: 'csv_text',
    csv_stage: 'raw',
    rows: [['id', 'name'], ['1', 'Alice'], ['2', 'Bob']],
    limit: 100,
    truncated: false,
    artifact_ready: false,
    ...overrides,
  }
}

function seedTabPreview(
  preview: TablePreviewData,
  nodeId = 'n1',
  tableName = 'my_table',
  overrides: { label?: string; labelMode?: 'auto' | 'custom'; isStale?: boolean; error?: string | null; loading?: boolean } = {},
) {
  act(() => {
    usePipelineStore.setState({
      nodes: [{
        id: nodeId,
        type: 'transform',
        position: { x: 0, y: 0 },
        data: {
          label: overrides.label ?? 'Transform',
          autoLabel: 'Transform',
          labelMode: overrides.labelMode ?? 'auto',
          tableName,
          config: { sql: 'select * from my_table' },
        },
      }],
      previewTabsByNodeId: {
        [nodeId]: {
          nodeId,
          tableNameAtLoad: tableName,
          data: preview,
          loading: overrides.loading ?? false,
          error: overrides.error ?? null,
          isStale: overrides.isStale ?? false,
        },
      },
      previewTabOrder: [nodeId],
      activePreviewTarget: { kind: 'tab', nodeId },
      transientPreview: {
        nodeId: null,
        data: null,
        loading: false,
        error: null,
      },
    })
  })
}

function seedTransientPreview(
  preview: CsvTextPreviewData | null,
  nodeId = 'csv-node',
  previewError: string | null = null,
  loading = false,
) {
  act(() => {
    usePipelineStore.setState({
      nodes: [{
        id: nodeId,
        type: 'csv_source',
        position: { x: 0, y: 0 },
        data: {
          label: 'CSV Source',
          autoLabel: 'CSV Source',
          labelMode: 'auto',
          tableName: 'orders_table',
          config: { original_filename: 'orders.csv' },
        },
      }],
      activePreviewTarget: { kind: 'transient', nodeId },
      transientPreview: {
        nodeId,
        data: preview,
        loading,
        error: previewError,
      },
    })
  })
}

beforeEach(() => {
  act(() => usePipelineStore.getState().newPipeline())
})

describe('DataPreviewPanel', () => {
  it('shows empty state when there is no active preview and no tabs', () => {
    render(<DataPreviewPanel />)
    expect(screen.getByText(/Preview data/i)).toBeInTheDocument()
  })

  it('renders column headers for table preview tabs', () => {
    seedTabPreview(makeTablePreview())
    render(<DataPreviewPanel />)
    expect(screen.getByText('id')).toBeInTheDocument()
    expect(screen.getByText('name')).toBeInTheDocument()
  })

  it('renders row data for table preview tabs', () => {
    seedTabPreview(makeTablePreview())
    render(<DataPreviewPanel />)
    expect(screen.getByText('Alice')).toBeInTheDocument()
    expect(screen.getByText('Bob')).toBeInTheDocument()
  })

  it('renders null values with NULL text', () => {
    seedTabPreview(makeTablePreview({ rows: [[1, null]] }))
    render(<DataPreviewPanel />)
    expect(screen.getByText('NULL')).toBeInTheDocument()
  })

  it('shows a stale badge for stale tabs and disables pagination', () => {
    seedTabPreview(makeTablePreview({ offset: 0, limit: 100, total_rows: 200 }), 'n1', 'orders_table', { isStale: true })
    render(<DataPreviewPanel />)

    expect(screen.getAllByText('Stale')).not.toHaveLength(0)
    expect(screen.getByRole('button', { name: /next/i })).toBeDisabled()
  })

  it('uses the custom node label as the tab title when label mode is custom', () => {
    seedTabPreview(makeTablePreview(), 'n1', 'orders_table', { label: 'Orders Final', labelMode: 'custom' })
    render(<DataPreviewPanel />)

    expect(screen.getByRole('tab', { name: /Orders Final/i })).toBeInTheDocument()
    expect(screen.queryByRole('tab', { name: /^orders_table$/i })).not.toBeInTheDocument()
  })

  it('uses the loaded table name for stale auto-labeled tabs', () => {
    act(() => {
      usePipelineStore.setState({
        nodes: [{
          id: 'n1',
          type: 'transform',
          position: { x: 0, y: 0 },
          data: {
            label: 'Transform',
            autoLabel: 'Transform',
            labelMode: 'auto',
            tableName: 'orders_new',
            config: { sql: 'select * from orders_table' },
          },
        }],
        previewTabsByNodeId: {
          n1: {
            nodeId: 'n1',
            tableNameAtLoad: 'orders_old',
            data: makeTablePreview(),
            loading: false,
            error: null,
            isStale: true,
          },
        },
        previewTabOrder: ['n1'],
        activePreviewTarget: { kind: 'tab', nodeId: 'n1' },
      })
    })

    render(<DataPreviewPanel />)

    expect(screen.getByRole('tab', { name: /orders_old/i })).toBeInTheDocument()
    expect(screen.queryByRole('tab', { name: /orders_new/i })).not.toBeInTheDocument()
  })

  it('switches between materialized preview tabs', async () => {
    const user = userEvent.setup()
    act(() => {
      usePipelineStore.setState({
        nodes: [
          {
            id: 'first',
            type: 'transform',
            position: { x: 0, y: 0 },
            data: { label: 'Transform', autoLabel: 'Transform', labelMode: 'auto', tableName: 'first_table', config: { sql: 'select 1' } },
          },
          {
            id: 'second',
            type: 'transform',
            position: { x: 0, y: 0 },
            data: { label: 'Second Result', autoLabel: 'Transform', labelMode: 'custom', tableName: 'second_table', config: { sql: 'select 2' } },
          },
        ],
        previewTabsByNodeId: {
          first: { nodeId: 'first', tableNameAtLoad: 'first_table', data: makeTablePreview({ rows: [[1, 'Alice']] }), loading: false, error: null, isStale: false },
          second: { nodeId: 'second', tableNameAtLoad: 'second_table', data: makeTablePreview({ rows: [[2, 'Bob']] }), loading: false, error: null, isStale: false },
        },
        previewTabOrder: ['first', 'second'],
        activePreviewTarget: { kind: 'tab', nodeId: 'first' },
      })
    })

    render(<DataPreviewPanel />)

    expect(screen.getByText('Alice')).toBeInTheDocument()
    await user.click(screen.getByRole('tab', { name: /Second Result/i }))
    expect(screen.getByText('Bob')).toBeInTheDocument()
  })

  it('renders raw csv preview rows as a transient preview while keeping tabs visible', () => {
    seedTabPreview(makeTablePreview())
    seedTransientPreview(makeCsvPreview())
    render(<DataPreviewPanel />)

    expect(screen.getByRole('tab', { name: /my_table/i })).toBeInTheDocument()
    expect(screen.getByText(/Raw CSV preview/i)).toBeInTheDocument()
    expect(screen.getByText('orders.csv')).toBeInTheDocument()
    expect(screen.getByText('Alice')).toBeInTheDocument()
  })

  it('uses one shared horizontal scroll container for csv previews', () => {
    seedTransientPreview(makeCsvPreview({ rows: [['id', 'name', 'notes'], ['1', 'Alice', 'long value']] }))
    const { container } = render(<DataPreviewPanel />)

    expect(screen.getAllByTestId('csv-preview-scroll-region')).toHaveLength(1)
    expect(screen.getAllByTestId('csv-preview-row')[0].className).not.toContain('overflow-x-auto')
    expect(container.querySelectorAll('.overflow-x-auto')).toHaveLength(0)
  })

  it('renders transient preview errors from the store', () => {
    seedTransientPreview(null, 'csv-node', 'Unable to preview CSV: invalid delimiter')
    render(<DataPreviewPanel />)
    expect(screen.getByText(/Unable to preview CSV: invalid delimiter/i)).toBeInTheDocument()
  })

  it('shows correct page counter for active table tabs', () => {
    seedTabPreview(makeTablePreview({ offset: 100, limit: 100, total_rows: 300 }))
    render(<DataPreviewPanel />)
    expect(screen.getByText(/Page 2 of 3/)).toBeInTheDocument()
  })

  it('calls loadTablePreview with next offset on Next click', async () => {
    const loadTablePreview = vi.fn()
    act(() => usePipelineStore.setState({ loadTablePreview }))
    seedTabPreview(makeTablePreview({ offset: 0, limit: 100, total_rows: 250 }))
    render(<DataPreviewPanel />)

    await userEvent.click(screen.getByRole('button', { name: /next/i }))
    expect(loadTablePreview).toHaveBeenCalledWith('n1', 'my_table', 100)
  })

  it('calls loadTablePreview with prev offset on Prev click', async () => {
    const loadTablePreview = vi.fn()
    act(() => usePipelineStore.setState({ loadTablePreview }))
    seedTabPreview(makeTablePreview({ offset: 100, limit: 100, total_rows: 300 }))
    render(<DataPreviewPanel />)

    await userEvent.click(screen.getByRole('button', { name: /prev/i }))
    expect(loadTablePreview).toHaveBeenCalledWith('n1', 'my_table', 0)
  })
})
