import { render, screen } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { act } from 'react'
import userEvent from '@testing-library/user-event'
import CsvSourceNode from './CsvSourceNode'
import NodeErrorDialog from '../NodeErrorDialog'
import { usePipelineStore } from '../../../store/pipelineStore'

vi.mock('@xyflow/react', () => ({
  Handle: ({ type, position }: { type: string; position: string }) => (
    <div data-testid={`handle-${type}-${position}`} />
  ),
  Position: { Left: 'left', Right: 'right' },
}))

vi.mock('../../../api/client', () => ({
  executePipeline: vi.fn(),
  previewData: vi.fn(),
  savePipeline: vi.fn(),
  loadPipeline: vi.fn(),
  listPipelines: vi.fn(),
}))

const NODE_ID = 'csv-node-1'

const defaultProps = {
  id: NODE_ID,
  data: {
    label: 'CSV Source',
    tableName: 'my_csv_table',
    config: { file_path: '/tmp/f.csv', original_filename: 'data.csv' },
  },
  type: 'csv_source',
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

beforeEach(() => {
  act(() => usePipelineStore.getState().newPipeline())
  act(() => usePipelineStore.setState({
    nodeResults: {},
    nodes: [
      {
        id: NODE_ID,
        type: 'csv_source',
        position: { x: 0, y: 0 },
        data: defaultProps.data,
      },
    ],
  }))
})

describe('CsvSourceNode', () => {
  it('renders the original filename', () => {
    render(<CsvSourceNode {...defaultProps} />)
    expect(screen.getByText('data.csv')).toBeInTheDocument()
  })

  it('renders the table name', () => {
    render(<CsvSourceNode {...defaultProps} />)
    expect(screen.getByText('my_csv_table')).toBeInTheDocument()
  })

  it('does not show Preview data button when not yet executed', () => {
    render(<CsvSourceNode {...defaultProps} />)
    expect(screen.queryByText(/preview data/i)).not.toBeInTheDocument()
  })

  it('shows Preview data button when status is success', () => {
    act(() => usePipelineStore.setState({
      nodeResults: { [NODE_ID]: { node_id: NODE_ID, status: 'success', row_count: 5, column_count: 3 } },
    }))
    render(<CsvSourceNode {...defaultProps} />)
    expect(screen.getByText(/preview data/i)).toBeInTheDocument()
  })

  it('has a source handle on the right', () => {
    render(<CsvSourceNode {...defaultProps} />)
    expect(screen.getByTestId('handle-source-right')).toBeInTheDocument()
  })

  it('uses the error styling and opens the error dialog', async () => {
    const user = userEvent.setup()
    act(() => usePipelineStore.setState({
      nodeResults: {
        [NODE_ID]: {
          node_id: NODE_ID,
          status: 'error',
          error: 'Invalid Input Error: CSV Error on Line 2, expected 4 columns but found 1',
        },
      },
    }))

    const { container } = render(
      <>
        <CsvSourceNode {...defaultProps} />
        <NodeErrorDialog />
      </>
    )

    expect(container.firstChild).toHaveClass('border-red-500')
    expect(screen.getByText('CSV Source').parentElement).toHaveClass('font-bold')

    await user.click(screen.getByRole('button', { name: /view error/i }))

    expect(screen.getByRole('dialog')).toBeInTheDocument()
    expect(screen.getByText(/expected 4 columns but found 1/i)).toBeInTheDocument()
  })
})
