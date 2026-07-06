import { render, screen } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { act } from 'react'
import ExcelWorkbookNode from './ExcelWorkbookNode'
import { usePipelineStore } from '../../../store/pipelineStore'

vi.mock('@xyflow/react', () => ({
  Handle: ({ type, position }: { type: string; position: string }) => (
    <div data-testid={`handle-${type}-${position}`} />
  ),
  Position: { Left: 'left', Right: 'right' },
}))

vi.mock('../../../api/client', () => ({
  savePipeline: vi.fn(),
  loadPipeline: vi.fn(),
  listPipelines: vi.fn(),
}))

const HUB_ID = 'hub-1'

const defaultProps = {
  id: HUB_ID,
  data: {
    label: 'July Allocations',
    tableName: '',
    config: { file_path: '/tmp/wb.xlsx', original_filename: 'wb.xlsx', sheet_names: ['Orders', 'Summary'] },
  },
  type: 'excel_workbook',
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
})

describe('ExcelWorkbookNode', () => {
  it('renders filename and sheet count, with no table name', () => {
    render(<ExcelWorkbookNode {...defaultProps} />)
    expect(screen.getByText('wb.xlsx · 2 sheets')).toBeInTheDocument()
    const table = document.querySelector('.ds-node-card__table')
    expect(table).toBeNull()
  })

  it('prompts for upload when the config is empty', () => {
    render(
      <ExcelWorkbookNode
        {...defaultProps}
        data={{ label: 'Excel Workbook', tableName: '', config: { file_path: '', original_filename: '', sheet_names: [] } }}
      />
    )
    expect(screen.getByText('No workbook uploaded')).toBeInTheDocument()
  })

  it('shows a mixed rollup badge derived from its sheet nodes', () => {
    act(() => usePipelineStore.setState({
      edges: [
        { id: 'e1', source: HUB_ID, target: 's1' },
        { id: 'e2', source: HUB_ID, target: 's2' },
      ],
      nodeResults: {
        s1: { node_id: 's1', status: 'success' },
        s2: { node_id: 's2', status: 'error', error: 'boom' },
      },
    }))

    render(<ExcelWorkbookNode {...defaultProps} />)
    expect(screen.getByText('1/2 loaded, 1 failed')).toBeInTheDocument()
  })

  it('has only a source handle', () => {
    render(<ExcelWorkbookNode {...defaultProps} />)
    expect(screen.getByTestId('handle-source-right')).toBeInTheDocument()
    expect(screen.queryByTestId('handle-target-left')).toBeNull()
  })
})
