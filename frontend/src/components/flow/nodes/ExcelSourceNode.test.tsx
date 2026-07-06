import { render, screen } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { act } from 'react'
import ExcelSourceNode from './ExcelSourceNode'
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
  getCacheStatus: vi.fn(() => Promise.resolve({ nodes: {} })),
  deletePreprocessedCsvArtifact: vi.fn(() => Promise.resolve({ deleted: true })),
  closePreviewSession: vi.fn(() => Promise.resolve({ closed: true })),
}))

const NODE_ID = 'sheet-1'

const defaultProps = {
  id: NODE_ID,
  data: {
    label: 'Orders',
    tableName: 'orders_t',
    config: {
      file_path: '/tmp/wb.xlsx',
      original_filename: 'wb.xlsx',
      sheet_names: ['Orders'],
      selected_sheet: 'Orders',
    },
  },
  type: 'excel_source',
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

describe('ExcelSourceNode', () => {
  it('renders only a source handle when standalone (orphan or legacy node)', () => {
    render(<ExcelSourceNode {...defaultProps} />)
    expect(screen.getByTestId('handle-source-right')).toBeInTheDocument()
    expect(screen.queryByTestId('handle-target-left')).toBeNull()
  })

  it('renders a target handle when joined to a workbook hub, so the structural edge can attach', () => {
    act(() => usePipelineStore.setState({
      edges: [{ id: 'edge-structural', source: 'hub-1', target: NODE_ID }],
    }))

    render(<ExcelSourceNode {...defaultProps} />)
    expect(screen.getByTestId('handle-target-left')).toBeInTheDocument()
    expect(screen.getByTestId('handle-source-right')).toBeInTheDocument()
  })
})
