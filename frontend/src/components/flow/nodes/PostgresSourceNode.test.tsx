import { render, screen } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { act } from 'react'
import DatabaseSourceNode from './DatabaseSourceNode'
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
  previewCsvSource: vi.fn(),
  previewPreprocessedCsvSource: vi.fn(),
  deletePreprocessedCsvArtifact: vi.fn((..._args: any[]) => Promise.resolve({ deleted: true })),
}))

const makeProps = (dbType: string, connection: Record<string, unknown>) => ({
  id: 'db-node-1',
  data: {
    label: 'Database Source',
    tableName: 'db_table',
    config: {
      db_type: dbType,
      connection,
      query: 'SELECT 1',
    },
  },
  type: 'db_source',
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
})

beforeEach(() => {
  act(() => usePipelineStore.getState().newPipeline())
  act(() => usePipelineStore.setState({ nodeResults: {} }))
})

describe('DatabaseSourceNode — postgres', () => {
  const props = makeProps('postgres', { host: 'localhost', port: 5432, database: 'mydb', user: 'u', password: 'p' })

  it('renders the node label', () => {
    render(<DatabaseSourceNode {...{ ...props, data: { ...props.data, label: 'Analytics Postgres' } }} />)
    expect(screen.getByText('Analytics Postgres')).toBeInTheDocument()
  })

  it('renders the connection string', () => {
    render(<DatabaseSourceNode {...props} />)
    expect(screen.getByText('localhost:5432/mydb')).toBeInTheDocument()
  })

  it('renders the table name', () => {
    render(<DatabaseSourceNode {...props} />)
    expect(screen.getByText('db_table')).toBeInTheDocument()
  })

  it('has a source handle on the right', () => {
    render(<DatabaseSourceNode {...props} />)
    expect(screen.getByTestId('handle-source-right')).toBeInTheDocument()
  })
})

describe('DatabaseSourceNode — oracle', () => {
  const props = makeProps('oracle', { host: 'orahost', port: 1521, service_name: 'ORCL', user: 'u', password: 'p' })

  it('renders oracle connection string', () => {
    render(<DatabaseSourceNode {...props} />)
    expect(screen.getByText('orahost:1521/ORCL')).toBeInTheDocument()
  })
})
