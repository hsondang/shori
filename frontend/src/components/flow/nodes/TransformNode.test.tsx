import { render, screen } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { act } from 'react'
import TransformNode from './TransformNode'
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
}))

const SHORT_SQL = 'SELECT * FROM orders'
const LONG_SQL = 'SELECT id, name, value, extra_col FROM some_very_long_table_name WHERE condition = true'

function makeProps(sql: string) {
  return {
    id: 'tx-1',
    data: { label: 'Transform', tableName: 'tx_table', config: { sql } },
    type: 'transform',
    selected: false,
    isConnectable: true,
    zIndex: 0,
    xPos: 0,
    yPos: 0,
    dragging: false,
  }
}

beforeEach(() => {
  act(() => usePipelineStore.getState().newPipeline())
  act(() => usePipelineStore.setState({ nodeResults: {} }))
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
})
