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

function seedStore(
  preview: TablePreviewData | CsvTextPreviewData | null,
  nodeId = 'n1',
  tableName = 'my_table',
  previewError: string | null = null,
) {
  act(() => {
    usePipelineStore.setState({
      previewData: preview,
      previewNodeId: nodeId,
      previewLoading: false,
      previewError,
      nodes: preview ? [{
        id: nodeId,
        type: 'csv_source',
        position: { x: 0, y: 0 },
        data: { label: 'CSV', tableName, config: { original_filename: 'orders.csv' } },
      }] : [],
    })
  })
}

beforeEach(() => {
  act(() => usePipelineStore.getState().newPipeline())
})

describe('DataPreviewPanel', () => {
  it('shows empty state when no preview data', () => {
    seedStore(null)
    render(<DataPreviewPanel />)
    expect(screen.getByText(/Preview data/i)).toBeInTheDocument()
  })

  it('renders preview errors from the store', () => {
    seedStore(null, 'n1', 'my_table', 'Unable to preview CSV: invalid delimiter')
    render(<DataPreviewPanel />)
    expect(screen.getByText(/Unable to preview CSV: invalid delimiter/i)).toBeInTheDocument()
  })

  it('renders column headers for table previews', () => {
    seedStore(makeTablePreview())
    render(<DataPreviewPanel />)
    expect(screen.getByText('id')).toBeInTheDocument()
    expect(screen.getByText('name')).toBeInTheDocument()
  })

  it('renders column types for table previews', () => {
    seedStore(makeTablePreview())
    render(<DataPreviewPanel />)
    expect(screen.getByText('INTEGER')).toBeInTheDocument()
    expect(screen.getByText('VARCHAR')).toBeInTheDocument()
  })

  it('renders row data for table previews', () => {
    seedStore(makeTablePreview())
    render(<DataPreviewPanel />)
    expect(screen.getByText('Alice')).toBeInTheDocument()
    expect(screen.getByText('Bob')).toBeInTheDocument()
  })

  it('renders null values with NULL text', () => {
    seedStore(makeTablePreview({ rows: [[1, null]] }))
    render(<DataPreviewPanel />)
    expect(screen.getByText('NULL')).toBeInTheDocument()
  })

  it('renders raw csv preview rows', () => {
    seedStore(makeCsvPreview())
    render(<DataPreviewPanel />)
    expect(screen.getByText(/Raw CSV preview/i)).toBeInTheDocument()
    expect(screen.getByText('orders.csv')).toBeInTheDocument()
    expect(screen.getByText('Alice')).toBeInTheDocument()
  })

  it('uses one shared horizontal scroll container for csv previews', () => {
    seedStore(makeCsvPreview({ rows: [['id', 'name', 'notes'], ['1', 'Alice', 'long value']] }))
    const { container } = render(<DataPreviewPanel />)

    expect(screen.getAllByTestId('csv-preview-scroll-region')).toHaveLength(1)
    expect(screen.getAllByTestId('csv-preview-row')[0].className).not.toContain('overflow-x-auto')
    expect(container.querySelectorAll('.overflow-x-auto')).toHaveLength(0)
  })

  it('shows truncation text for csv previews', () => {
    seedStore(makeCsvPreview({ truncated: true }))
    render(<DataPreviewPanel />)
    expect(screen.getByText(/Truncated to preview limit/i)).toBeInTheDocument()
  })

  it('shows preprocessed preview state when viewing reviewed output', () => {
    seedStore(makeCsvPreview({ csv_stage: 'preprocessed', artifact_ready: true }))
    render(<DataPreviewPanel />)
    expect(screen.getByText(/Preprocessed CSV preview/i)).toBeInTheDocument()
    expect(screen.getByText(/Reviewed output ready for load/i)).toBeInTheDocument()
  })

  it('shows Prev button disabled on page 1', () => {
    seedStore(makeTablePreview({ offset: 0, limit: 100, total_rows: 200 }))
    render(<DataPreviewPanel />)
    expect(screen.getByRole('button', { name: /prev/i })).toBeDisabled()
  })

  it('shows Next button disabled on last page', () => {
    seedStore(makeTablePreview({ offset: 0, limit: 100, total_rows: 50 }))
    render(<DataPreviewPanel />)
    expect(screen.getByRole('button', { name: /next/i })).toBeDisabled()
  })

  it('shows Next button enabled when not on last page', () => {
    seedStore(makeTablePreview({ offset: 0, limit: 100, total_rows: 250 }))
    render(<DataPreviewPanel />)
    expect(screen.getByRole('button', { name: /next/i })).not.toBeDisabled()
  })

  it('shows correct page counter', () => {
    seedStore(makeTablePreview({ offset: 100, limit: 100, total_rows: 300 }))
    render(<DataPreviewPanel />)
    expect(screen.getByText(/Page 2 of 3/)).toBeInTheDocument()
  })

  it('calls loadTablePreview with next offset on Next click', async () => {
    const loadTablePreview = vi.fn()
    act(() => usePipelineStore.setState({ loadTablePreview }))
    seedStore(makeTablePreview({ offset: 0, limit: 100, total_rows: 250 }))
    render(<DataPreviewPanel />)

    await userEvent.click(screen.getByRole('button', { name: /next/i }))
    expect(loadTablePreview).toHaveBeenCalledWith('n1', 'my_table', 100)
  })

  it('calls loadTablePreview with prev offset on Prev click', async () => {
    const loadTablePreview = vi.fn()
    act(() => usePipelineStore.setState({ loadTablePreview }))
    seedStore(makeTablePreview({ offset: 100, limit: 100, total_rows: 300 }))
    render(<DataPreviewPanel />)

    await userEvent.click(screen.getByRole('button', { name: /prev/i }))
    expect(loadTablePreview).toHaveBeenCalledWith('n1', 'my_table', 0)
  })
})
