import { render, screen } from '@testing-library/react'
import { act } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import NodeConfigPanel from './NodeConfigPanel'
import { usePipelineStore } from '../../store/pipelineStore'

vi.mock('@monaco-editor/react', () => ({
  default: ({ value, onChange }: { value: string; onChange?: (value: string) => void }) => (
    <textarea aria-label="sql-editor" value={value} onChange={(event) => onChange?.(event.target.value)} />
  ),
}))

vi.mock('../../api/client', () => ({
  uploadCsv: vi.fn(),
  executePipeline: vi.fn(),
  previewData: vi.fn(),
  savePipeline: vi.fn(),
  loadPipeline: vi.fn(),
  listPipelines: vi.fn(),
}))

describe('NodeConfigPanel', () => {
  beforeEach(() => {
    act(() => {
      usePipelineStore.getState().newPipeline()
      usePipelineStore.setState({
        nodes: [
          {
            id: 'db-node',
            type: 'db_source',
            position: { x: 0, y: 0 },
            data: {
              label: 'Analytics Postgres',
              tableName: 'db_table',
              config: {
                db_type: 'postgres',
                connection: {
                  host: 'localhost',
                  port: 5432,
                  database: 'analytics',
                  user: 'user',
                  password: 'secret',
                },
                query: 'SELECT 1',
              },
            },
          },
        ],
        selectedNodeId: 'db-node',
      })
    })
  })

  it('shows SQL editing only for database nodes', () => {
    render(<NodeConfigPanel />)

    expect(screen.getByText('Analytics Postgres')).toBeInTheDocument()
    expect(screen.getByText('SQL Query')).toBeInTheDocument()
    expect(screen.getByLabelText('sql-editor')).toHaveValue('SELECT 1')

    expect(screen.queryByText('Label')).not.toBeInTheDocument()
    expect(screen.queryByText('Table Name')).not.toBeInTheDocument()
    expect(screen.queryByText('Database Type')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /test connection/i })).not.toBeInTheDocument()
  })
})
