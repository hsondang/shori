import { render, screen } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { act } from 'react'
import userEvent from '@testing-library/user-event'
import TransformNode from './TransformNode'
import { usePipelineStore } from '../../../store/pipelineStore'

vi.mock('@xyflow/react', () => ({
  Handle: ({ type, position }: { type: string; position: string }) => (
    <div data-testid={`handle-${type}-${position}`} />
  ),
  Position: { Left: 'left', Right: 'right' },
}))

vi.mock('../../../api/client', () => ({
  executeNode: vi.fn(),
  executePipeline: vi.fn(),
  previewData: vi.fn(),
  previewCsvSource: vi.fn(),
  previewPreprocessedCsvSource: vi.fn(),
  deletePreprocessedCsvArtifact: vi.fn((..._args: any[]) => Promise.resolve({ deleted: true })),
  getTableSchema: vi.fn(),
  savePipeline: vi.fn(),
  loadPipeline: vi.fn(),
  listPipelines: vi.fn(),
}))

const SHORT_SQL = 'SELECT * FROM orders'
const LONG_SQL = 'SELECT id, name, value, extra_col FROM some_very_long_table_name WHERE condition = true'

function makeProps(sql: string) {
  return {
    id: 'tx-1',
    data: { label: 'Transform', tableName: 'tx_table', config: { sql } },
    type: 'transform',
    selected: false,
    draggable: true,
    selectable: true,
    deletable: true,
    isConnectable: true,
    positionAbsoluteX: 0,
    positionAbsoluteY: 0,
    zIndex: 0,
    xPos: 0,
    yPos: 0,
    dragging: false,
  }
}

beforeEach(() => {
  act(() => usePipelineStore.getState().newPipeline())
  act(() => usePipelineStore.setState({
    nodeResults: {},
    runTransformPreview: vi.fn(),
  }))
})

describe('TransformNode', () => {
  it('renders the SQL preview for short SQL', () => {
    render(<TransformNode {...makeProps(SHORT_SQL)} />)
    expect(screen.getByText(SHORT_SQL)).toBeInTheDocument()
  })

  it('truncates long SQL at 50 characters with ellipsis', () => {
    render(<TransformNode {...makeProps(LONG_SQL)} />)
    const preview = LONG_SQL.substring(0, 50) + '...'
    expect(screen.getByText(preview)).toBeInTheDocument()
  })

  it('shows "No SQL defined" when sql is empty', () => {
    render(<TransformNode {...makeProps('')} />)
    expect(screen.getByText('No SQL defined')).toBeInTheDocument()
  })

  it('has both source (right) and target (left) handles', () => {
    render(<TransformNode {...makeProps(SHORT_SQL)} />)
    expect(screen.getByTestId('handle-target-left')).toBeInTheDocument()
    expect(screen.getByTestId('handle-source-right')).toBeInTheDocument()
  })

  it('renders the table name', () => {
    render(<TransformNode {...makeProps(SHORT_SQL)} />)
    expect(screen.getByText('tx_table')).toBeInTheDocument()
  })

  it('renders Run and Preview for transform nodes', () => {
    render(<TransformNode {...makeProps(SHORT_SQL)} />)
    expect(screen.getByRole('button', { name: 'Run and Preview' })).toBeInTheDocument()
  })

  it('invokes the shared transform preview action', async () => {
    const user = userEvent.setup()
    const runTransformPreview = vi.fn()
    act(() => usePipelineStore.setState({ runTransformPreview }))

    render(<TransformNode {...makeProps(SHORT_SQL)} />)

    await user.click(screen.getByRole('button', { name: 'Run and Preview' }))

    expect(runTransformPreview).toHaveBeenCalledWith('tx-1')
  })

  it('keeps Preview data gated on successful execution', () => {
    const { rerender } = render(<TransformNode {...makeProps(SHORT_SQL)} />)
    expect(screen.queryByRole('button', { name: 'Preview data' })).not.toBeInTheDocument()

    act(() => usePipelineStore.setState({
      nodeResults: {
        'tx-1': { node_id: 'tx-1', status: 'success', row_count: 2, column_count: 1 },
      },
    }))

    rerender(<TransformNode {...makeProps(SHORT_SQL)} />)

    expect(screen.getByRole('button', { name: 'Preview data' })).toBeInTheDocument()
  })
})
