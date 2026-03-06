import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, expect, beforeEach, vi } from 'vitest'
import { act } from 'react'
import DataPreviewPanel from './DataPreviewPanel'
import { usePipelineStore } from '../../store/pipelineStore'
import type { DataPreview } from '../../types/pipeline'

vi.mock('../../api/client', () => ({
  executePipeline: vi.fn(),
  previewData: vi.fn(),
  savePipeline: vi.fn(),
  loadPipeline: vi.fn(),
  listPipelines: vi.fn(),
}))

function makePreview(overrides: Partial<DataPreview> = {}): DataPreview {
  return {
    columns: ['id', 'name'],
    column_types: ['INTEGER', 'VARCHAR'],
    rows: [[1, 'Alice'], [2, 'Bob'], [3, 'Carol']],
    total_rows: 3,
    offset: 0,
    limit: 100,
    ...overrides,
  }
}

function seedStore(preview: DataPreview | null, nodeId = 'n1', tableName = 'my_table') {
  act(() => {
    usePipelineStore.setState({
      previewData: preview,
      previewNodeId: nodeId,
      previewLoading: false,
      nodes: preview ? [{
        id: nodeId,
        type: 'csv_source',
        position: { x: 0, y: 0 },
        data: { label: 'CSV', tableName, config: {} },
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

  it('renders column headers', () => {
    seedStore(makePreview())
    render(<DataPreviewPanel />)
    expect(screen.getByText('id')).toBeInTheDocument()
    expect(screen.getByText('name')).toBeInTheDocument()
  })

  it('renders column types', () => {
    seedStore(makePreview())
    render(<DataPreviewPanel />)
    expect(screen.getByText('INTEGER')).toBeInTheDocument()
    expect(screen.getByText('VARCHAR')).toBeInTheDocument()
  })

  it('renders row data', () => {
    seedStore(makePreview())
    render(<DataPreviewPanel />)
    expect(screen.getByText('Alice')).toBeInTheDocument()
    expect(screen.getByText('Bob')).toBeInTheDocument()
  })

  it('renders null values with NULL text', () => {
    seedStore(makePreview({ rows: [[1, null]] }))
    render(<DataPreviewPanel />)
    expect(screen.getByText('NULL')).toBeInTheDocument()
  })

  it('shows Prev button disabled on page 1', () => {
    seedStore(makePreview({ offset: 0, limit: 100, total_rows: 200 }))
    render(<DataPreviewPanel />)
    expect(screen.getByRole('button', { name: /prev/i })).toBeDisabled()
  })

  it('shows Next button disabled on last page', () => {
    seedStore(makePreview({ offset: 0, limit: 100, total_rows: 50 }))
    render(<DataPreviewPanel />)
    expect(screen.getByRole('button', { name: /next/i })).toBeDisabled()
  })

  it('shows Next button enabled when not on last page', () => {
    seedStore(makePreview({ offset: 0, limit: 100, total_rows: 250 }))
    render(<DataPreviewPanel />)
    expect(screen.getByRole('button', { name: /next/i })).not.toBeDisabled()
  })

  it('shows correct page counter', () => {
    seedStore(makePreview({ offset: 100, limit: 100, total_rows: 300 }))
    render(<DataPreviewPanel />)
    expect(screen.getByText(/Page 2 of 3/)).toBeInTheDocument()
  })

  it('calls loadPreview with next offset on Next click', async () => {
    const loadPreview = vi.fn()
    act(() => usePipelineStore.setState({ loadPreview }))
    seedStore(makePreview({ offset: 0, limit: 100, total_rows: 250 }))
    render(<DataPreviewPanel />)

    await userEvent.click(screen.getByRole('button', { name: /next/i }))
    expect(loadPreview).toHaveBeenCalledWith('n1', 'my_table', 100)
  })

  it('calls loadPreview with prev offset on Prev click', async () => {
    const loadPreview = vi.fn()
    act(() => usePipelineStore.setState({ loadPreview }))
    seedStore(makePreview({ offset: 100, limit: 100, total_rows: 300 }))
    render(<DataPreviewPanel />)

    await userEvent.click(screen.getByRole('button', { name: /prev/i }))
    expect(loadPreview).toHaveBeenCalledWith('n1', 'my_table', 0)
  })
})
