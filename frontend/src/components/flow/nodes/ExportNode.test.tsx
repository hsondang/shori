import { render, screen } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { act } from 'react'
import ExportNode from './ExportNode'
import { usePipelineStore } from '../../../store/pipelineStore'
import type { Edge, Node } from '@xyflow/react'

vi.mock('@xyflow/react', () => ({
  Handle: ({ type, position }: { type: string; position: string }) => (
    <div data-testid={`handle-${type}-${position}`} />
  ),
  Position: { Left: 'left', Right: 'right' },
}))

vi.mock('../../../api/client', () => ({
  exportData: vi.fn(),
}))

const EXPORT_NODE_ID = 'exp-1'
const SOURCE_NODE_ID = 'src-1'

const exportProps = {
  id: EXPORT_NODE_ID,
  data: { label: 'Export', tableName: 'export_table', config: { format: 'csv' } },
  type: 'export',
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

function seedWithSource() {
  const sourceNode: Node = {
    id: SOURCE_NODE_ID,
    type: 'csv_source',
    position: { x: 0, y: 0 },
    data: { label: 'CSV', tableName: 'source_table', config: {} },
  }
  const edge: Edge = { id: 'e1', source: SOURCE_NODE_ID, target: EXPORT_NODE_ID }
  act(() => usePipelineStore.setState({ nodes: [sourceNode], edges: [edge] }))
}

beforeEach(() => {
  act(() => usePipelineStore.getState().newPipeline())
  act(() => usePipelineStore.setState({ nodeResults: {} }))
})

describe('ExportNode', () => {
  it('does not show Download CSV button when no source is connected', () => {
    render(<ExportNode {...exportProps} />)
    expect(screen.queryByRole('button', { name: /download csv/i })).not.toBeInTheDocument()
  })

  it('shows the source table name when connected', () => {
    seedWithSource()
    render(<ExportNode {...exportProps} />)
    expect(screen.getByText(/source_table/)).toBeInTheDocument()
  })

  it('shows Download CSV button when source is connected', () => {
    seedWithSource()
    render(<ExportNode {...exportProps} />)
    expect(screen.getByRole('button', { name: /download csv/i })).toBeInTheDocument()
  })

  it('has a target handle on the left', () => {
    render(<ExportNode {...exportProps} />)
    expect(screen.getByTestId('handle-target-left')).toBeInTheDocument()
  })
})
