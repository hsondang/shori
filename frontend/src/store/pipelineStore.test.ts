import { act } from '@testing-library/react'
import { describe, it, expect, beforeEach, vi } from 'vitest'
import { usePipelineStore } from './pipelineStore'

const mockExecutePipeline = vi.fn()
const mockExecuteNode = vi.fn()
const mockPreviewData = vi.fn()
const mockPreviewCsvSource = vi.fn()
const mockPreviewPreprocessedCsvSource = vi.fn()
const mockGetTableSchema = vi.fn()
const mockDeleteTable = vi.fn((..._args: any[]) => Promise.resolve({ deleted: true }))
const mockDeletePreprocessedCsvArtifact = vi.fn((..._args: any[]) => Promise.resolve({ deleted: true }))
const mockSavePipeline = vi.fn()
const mockLoadPipeline = vi.fn()
const mockListPipelines = vi.fn()

// Prevent real API calls
vi.mock('../api/client', () => ({
  executePipeline: (...args: any[]) => mockExecutePipeline(...args),
  executeNode: (...args: any[]) => mockExecuteNode(...args),
  previewData: (...args: any[]) => mockPreviewData(...args),
  previewCsvSource: (...args: any[]) => mockPreviewCsvSource(...args),
  previewPreprocessedCsvSource: (...args: any[]) => mockPreviewPreprocessedCsvSource(...args),
  getTableSchema: (...args: any[]) => mockGetTableSchema(...args),
  deleteTable: (...args: any[]) => mockDeleteTable(...args),
  deletePreprocessedCsvArtifact: (...args: any[]) => mockDeletePreprocessedCsvArtifact(...args),
  savePipeline: (...args: any[]) => mockSavePipeline(...args),
  loadPipeline: (...args: any[]) => mockLoadPipeline(...args),
  listPipelines: (...args: any[]) => mockListPipelines(...args),
}))

function makeTablePreview(overrides: Record<string, unknown> = {}) {
  return {
    kind: 'table' as const,
    columns: ['id', 'name'],
    column_types: ['INTEGER', 'VARCHAR'],
    rows: [[1, 'Alice']],
    total_rows: 1,
    offset: 0,
    limit: 100,
    ...overrides,
  }
}

function makeCsvTextPreview(overrides: Record<string, unknown> = {}) {
  return {
    kind: 'csv_text' as const,
    csv_stage: 'raw' as const,
    rows: [['id', 'name'], ['1', 'Alice']],
    limit: 100,
    truncated: false,
    artifact_ready: false,
    ...overrides,
  }
}

function resetStore() {
  act(() => {
    usePipelineStore.getState().newPipeline()
  })
}

describe('pipelineStore', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    resetStore()
  })

  describe('addNode', () => {
    it('adds a csv_source node with correct default config', () => {
      act(() => usePipelineStore.getState().addNode('csv_source', { x: 0, y: 0 }))
      const { nodes } = usePipelineStore.getState()
      expect(nodes).toHaveLength(1)
      expect(nodes[0].type).toBe('csv_source')
      const config = (nodes[0].data as Record<string, unknown>).config as Record<string, unknown>
      expect(config).toHaveProperty('file_path', '')
      expect(config).toHaveProperty('original_filename', '')
      expect(config).toHaveProperty('preprocessing')
    })

    it('adds a db_source node with connection and query defaults', () => {
      act(() => usePipelineStore.getState().addNode('db_source', { x: 0, y: 0 }))
      const { nodes } = usePipelineStore.getState()
      expect(nodes[0].type).toBe('db_source')
      const config = (nodes[0].data as Record<string, unknown>).config as Record<string, unknown>
      expect(config).toHaveProperty('db_type', 'postgres')
      expect(config).toHaveProperty('query', '')
      const conn = config.connection as Record<string, unknown>
      expect(conn).toHaveProperty('port', 5432)
      expect(conn).toHaveProperty('database', '')
    })

    it('adds a db_source node from a saved connection with copied config', () => {
      let connectionId = ''
      act(() => {
        connectionId = usePipelineStore.getState().addDatabaseConnection({
          name: 'Analytics Postgres',
          db_type: 'postgres',
          host: 'localhost',
          port: 5432,
          database: 'analytics',
          user: 'user',
          password: 'secret',
        })
      })

      act(() => usePipelineStore.getState().addDatabaseSourceFromConnection(connectionId, { x: 10, y: 20 }))

      const { nodes } = usePipelineStore.getState()
      expect(nodes).toHaveLength(1)
      expect((nodes[0].data as Record<string, unknown>).label).toBe('Analytics Postgres')
      const config = (nodes[0].data as Record<string, unknown>).config as Record<string, unknown>
      expect(config).toHaveProperty('db_type', 'postgres')
      expect(config).toHaveProperty('query', '')
      expect(config.connection).toEqual({
        host: 'localhost',
        port: 5432,
        database: 'analytics',
        user: 'user',
        password: 'secret',
      })
    })

    it('adds a transform node with empty sql default', () => {
      act(() => usePipelineStore.getState().addNode('transform', { x: 0, y: 0 }))
      const { nodes } = usePipelineStore.getState()
      const config = (nodes[0].data as Record<string, unknown>).config as Record<string, unknown>
      expect(config).toHaveProperty('sql', '')
    })

    it('adds an export node with csv format default', () => {
      act(() => usePipelineStore.getState().addNode('export', { x: 0, y: 0 }))
      const { nodes } = usePipelineStore.getState()
      const config = (nodes[0].data as Record<string, unknown>).config as Record<string, unknown>
      expect(config).toHaveProperty('format', 'csv')
    })
  })

  describe('databaseConnections', () => {
    it('adds, updates, and deletes saved database connections', () => {
      let connectionId = ''
      act(() => {
        connectionId = usePipelineStore.getState().addDatabaseConnection({
          name: 'Warehouse',
          db_type: 'oracle',
          host: 'orahost',
          port: 1521,
          service_name: 'ORCL',
          user: 'user',
          password: 'secret',
        })
      })

      expect(usePipelineStore.getState().databaseConnections).toHaveLength(1)

      act(() => {
        usePipelineStore.getState().updateDatabaseConnection(connectionId, {
          name: 'Warehouse Prod',
          db_type: 'oracle',
          host: 'prod-host',
          port: 1521,
          service_name: 'PROD',
          user: 'admin',
          password: 'updated',
        })
      })

      expect(usePipelineStore.getState().databaseConnections[0]).toMatchObject({
        id: connectionId,
        name: 'Warehouse Prod',
        host: 'prod-host',
        service_name: 'PROD',
      })

      act(() => usePipelineStore.getState().deleteDatabaseConnection(connectionId))
      expect(usePipelineStore.getState().databaseConnections).toEqual([])
    })
  })

  describe('updateNodeData', () => {
    it('patches specific keys without removing others', () => {
      act(() => usePipelineStore.getState().addNode('csv_source', { x: 0, y: 0 }))
      const { nodes } = usePipelineStore.getState()
      const nodeId = nodes[0].id

      act(() => usePipelineStore.getState().updateNodeData(nodeId, { label: 'My CSV' }))

      const updated = usePipelineStore.getState().nodes[0]
      expect((updated.data as Record<string, unknown>).label).toBe('My CSV')
      // config should still be present
      expect((updated.data as Record<string, unknown>).config).toBeDefined()
    })

    it('drops the previous materialized table and clears stale state when table name changes', () => {
      act(() => {
        usePipelineStore.setState({
          nodes: [{
            id: 'csv-node',
            type: 'csv_source',
            position: { x: 0, y: 0 },
            data: {
              label: 'Orders CSV',
              tableName: 'orders_table',
              config: { file_path: '/tmp/orders.csv', original_filename: 'orders.csv' },
            },
          }],
          nodeResults: {
            'csv-node': { node_id: 'csv-node', status: 'success', row_count: 5, column_count: 2 },
          },
          previewNodeId: 'csv-node',
          previewLoading: true,
          previewData: makeTablePreview({ columns: ['id'], column_types: ['INTEGER'], rows: [[1]] }),
        })
      })

      act(() => usePipelineStore.getState().updateNodeData('csv-node', { tableName: 'orders_new' }))

      expect(mockDeleteTable).toHaveBeenCalledWith('orders_table')
      expect((usePipelineStore.getState().nodes[0].data as Record<string, unknown>).tableName).toBe('orders_new')
      expect(usePipelineStore.getState().nodeResults['csv-node']).toBeUndefined()
      expect(usePipelineStore.getState().previewNodeId).toBeNull()
      expect(usePipelineStore.getState().previewData).toBeNull()
      expect(usePipelineStore.getState().previewLoading).toBe(false)
    })

    it('invalidates reviewed preprocess artifacts when csv preprocessing inputs change', () => {
      act(() => {
        usePipelineStore.setState({
          nodes: [{
            id: 'csv-node',
            type: 'csv_source',
            position: { x: 0, y: 0 },
            data: {
              label: 'Orders CSV',
              tableName: 'orders_table',
              config: {
                file_path: '/tmp/orders.csv',
                original_filename: 'orders.csv',
                preprocessing: { enabled: true, runtime: 'python', script: 'print(1)' },
              },
            },
          }],
          nodeResults: {
            'csv-node': { node_id: 'csv-node', status: 'success', row_count: 5, column_count: 2 },
          },
          csvPreprocessArtifacts: {
            'csv-node': JSON.stringify({
              file_path: '/tmp/orders.csv',
              runtime: 'python',
              script: 'print(1)',
            }),
          },
        })
      })

      act(() => usePipelineStore.getState().updateNodeData('csv-node', {
        config: {
          file_path: '/tmp/orders.csv',
          original_filename: 'orders.csv',
          preprocessing: { enabled: true, runtime: 'python', script: 'print(2)' },
        },
      }))

      expect(mockDeletePreprocessedCsvArtifact).toHaveBeenCalledWith('csv-node')
      expect(usePipelineStore.getState().csvPreprocessArtifacts['csv-node']).toBeUndefined()
      expect(usePipelineStore.getState().nodeResults['csv-node']).toBeUndefined()
    })
  })

  describe('deleteNode', () => {
    it('removes the node from the nodes array', () => {
      act(() => usePipelineStore.getState().addNode('csv_source', { x: 0, y: 0 }))
      const nodeId = usePipelineStore.getState().nodes[0].id

      act(() => usePipelineStore.getState().deleteNode(nodeId))
      expect(usePipelineStore.getState().nodes).toHaveLength(0)
    })

    it('removes edges connected to the deleted node', () => {
      act(() => {
        usePipelineStore.getState().addNode('csv_source', { x: 0, y: 0 })
        usePipelineStore.getState().addNode('transform', { x: 200, y: 0 })
      })
      const [src, tgt] = usePipelineStore.getState().nodes
      act(() => usePipelineStore.getState().onConnect({ source: src.id, target: tgt.id, sourceHandle: null, targetHandle: null }))
      expect(usePipelineStore.getState().edges).toHaveLength(1)

      act(() => usePipelineStore.getState().deleteNode(src.id))
      expect(usePipelineStore.getState().edges).toHaveLength(0)
    })

    it('drops the materialized table and clears stale state for the deleted node', () => {
      act(() => {
        usePipelineStore.setState({
          nodes: [{
            id: 'csv-node',
            type: 'csv_source',
            position: { x: 0, y: 0 },
            data: {
              label: 'Orders CSV',
              tableName: 'orders_table',
              config: { file_path: '/tmp/orders.csv', original_filename: 'orders.csv' },
            },
          }],
          selectedNodeId: 'csv-node',
          errorDialogNodeId: 'csv-node',
          nodeResults: {
            'csv-node': { node_id: 'csv-node', status: 'success', row_count: 5, column_count: 2 },
          },
          previewNodeId: 'csv-node',
          previewLoading: true,
          previewData: makeTablePreview({ columns: ['id'], column_types: ['INTEGER'], rows: [[1]] }),
        })
      })

      act(() => usePipelineStore.getState().deleteNode('csv-node'))

      expect(mockDeleteTable).toHaveBeenCalledWith('orders_table')
      expect(usePipelineStore.getState().nodes).toHaveLength(0)
      expect(usePipelineStore.getState().nodeResults['csv-node']).toBeUndefined()
      expect(usePipelineStore.getState().selectedNodeId).toBeNull()
      expect(usePipelineStore.getState().errorDialogNodeId).toBeNull()
      expect(usePipelineStore.getState().previewNodeId).toBeNull()
      expect(usePipelineStore.getState().previewData).toBeNull()
      expect(usePipelineStore.getState().previewLoading).toBe(false)
    })
  })

  describe('onConnect', () => {
    it('creates an edge with the correct source and target', () => {
      act(() => {
        usePipelineStore.getState().addNode('csv_source', { x: 0, y: 0 })
        usePipelineStore.getState().addNode('transform', { x: 200, y: 0 })
      })
      const [a, b] = usePipelineStore.getState().nodes
      act(() => usePipelineStore.getState().onConnect({ source: a.id, target: b.id, sourceHandle: null, targetHandle: null }))

      const { edges } = usePipelineStore.getState()
      expect(edges).toHaveLength(1)
      expect(edges[0].source).toBe(a.id)
      expect(edges[0].target).toBe(b.id)
    })
  })

  describe('newPipeline', () => {
    it('resets nodes, edges, and nodeResults', () => {
      act(() => {
        usePipelineStore.getState().addNode('csv_source', { x: 0, y: 0 })
        usePipelineStore.getState().addDatabaseConnection({
          name: 'Analytics',
          db_type: 'postgres',
          host: 'localhost',
          port: 5432,
          database: 'analytics',
          user: 'user',
          password: 'secret',
        })
        usePipelineStore.getState().newPipeline()
      })
      const state = usePipelineStore.getState()
      expect(state.nodes).toHaveLength(0)
      expect(state.edges).toHaveLength(0)
      expect(state.databaseConnections).toEqual([])
      expect(state.nodeResults).toEqual({})
      expect(state.errorDialogNodeId).toBeNull()
    })

    it('resets pipeline name to Untitled Pipeline', () => {
      act(() => {
        usePipelineStore.getState().setPipelineName('My Pipeline')
        usePipelineStore.getState().newPipeline()
      })
      expect(usePipelineStore.getState().pipelineName).toBe('Untitled Pipeline')
    })
  })

  describe('pipeline persistence', () => {
    it('savePipeline includes database connections', async () => {
      act(() => {
        usePipelineStore.getState().addDatabaseConnection({
          name: 'Analytics',
          db_type: 'postgres',
          host: 'localhost',
          port: 5432,
          database: 'analytics',
          user: 'user',
          password: 'secret',
        })
      })

      await act(async () => {
        await usePipelineStore.getState().savePipeline()
      })

      expect(mockSavePipeline).toHaveBeenCalledWith(expect.objectContaining({
        database_connections: [
          expect.objectContaining({
            name: 'Analytics',
            db_type: 'postgres',
            database: 'analytics',
          }),
        ],
      }))
      expect(usePipelineStore.getState().hasUnsavedChanges).toBe(false)
    })

    it('loadPipeline restores database connections', async () => {
      mockLoadPipeline.mockResolvedValueOnce({
        id: 'pipeline-1',
        name: 'Loaded Pipeline',
        database_connections: [
          {
            id: 'conn-1',
            name: 'Analytics',
            db_type: 'postgres',
            host: 'localhost',
            port: 5432,
            database: 'analytics',
            user: 'user',
            password: 'secret',
          },
        ],
        nodes: [],
        edges: [],
      })

      await act(async () => {
        await usePipelineStore.getState().loadPipeline('pipeline-1')
      })

      expect(usePipelineStore.getState().databaseConnections).toEqual([
        expect.objectContaining({ id: 'conn-1', name: 'Analytics' }),
      ])
      expect(usePipelineStore.getState().hasUnsavedChanges).toBe(false)
    })

    it('marks the pipeline dirty after metadata changes and clears dirty state after save', async () => {
      act(() => {
        usePipelineStore.getState().setPipelineName('Renamed Pipeline')
      })

      expect(usePipelineStore.getState().hasUnsavedChanges).toBe(true)

      await act(async () => {
        await usePipelineStore.getState().savePipeline()
      })

      expect(usePipelineStore.getState().hasUnsavedChanges).toBe(false)
    })

    it('confirms discard only when there are unsaved changes', () => {
      const confirmSpy = vi.spyOn(window, 'confirm').mockReturnValue(true)

      expect(usePipelineStore.getState().confirmDiscardChanges('Warehouse')).toBe(true)
      expect(confirmSpy).not.toHaveBeenCalled()

      act(() => {
        usePipelineStore.getState().setPipelineName('Changed')
      })

      expect(usePipelineStore.getState().confirmDiscardChanges('Warehouse')).toBe(true)
      expect(confirmSpy).toHaveBeenCalledWith('You have unsaved changes. Discard them and open "Warehouse"?')
    })
  })

  describe('executeSingleNode', () => {
    it('stores the execution result and loads preview on success', async () => {
      act(() => {
        usePipelineStore.getState().addNode('csv_source', { x: 0, y: 0 })
        usePipelineStore.getState().updateNodeData(usePipelineStore.getState().nodes[0].id, {
          label: 'Orders CSV',
          tableName: 'orders_table',
          config: { file_path: '/tmp/orders.csv', original_filename: 'orders.csv' },
        })
      })

      const node = usePipelineStore.getState().nodes[0]
      mockExecuteNode.mockResolvedValueOnce({
        node_id: node.id,
        status: 'success',
        row_count: 3,
        column_count: 2,
        columns: ['id', 'name'],
        execution_time_ms: 12,
      })
      mockPreviewData.mockResolvedValueOnce({
        kind: 'table',
        columns: ['id', 'name'],
        column_types: ['INTEGER', 'VARCHAR'],
        rows: [[1, 'Alice']],
        total_rows: 1,
        offset: 0,
        limit: 100,
      })

      await act(async () => {
        await usePipelineStore.getState().executeSingleNode(node.id, { loadPreviewOnSuccess: true })
      })

      expect(mockExecuteNode).toHaveBeenCalledWith(expect.objectContaining({
        id: node.id,
        label: 'Orders CSV',
        table_name: 'orders_table',
      }))
      expect(mockPreviewData).toHaveBeenCalledWith('orders_table', 0)
      expect(usePipelineStore.getState().nodeResults[node.id]).toEqual(expect.objectContaining({
        node_id: node.id,
        status: 'success',
      }))
      expect(usePipelineStore.getState().previewData).toEqual(expect.objectContaining({
        columns: ['id', 'name'],
      }))
    })

    it('stores an error result and does not load preview when execution fails', async () => {
      act(() => {
        usePipelineStore.getState().addNode('csv_source', { x: 0, y: 0 })
      })

      const node = usePipelineStore.getState().nodes[0]
      act(() => {
        usePipelineStore.setState({
          previewData: makeTablePreview({ columns: ['existing'], column_types: ['VARCHAR'], rows: [['value']] }),
        })
      })
      mockExecuteNode.mockRejectedValueOnce(new Error('boom'))

      await act(async () => {
        await usePipelineStore.getState().executeSingleNode(node.id, { loadPreviewOnSuccess: true })
      })

      expect(usePipelineStore.getState().nodeResults[node.id]).toEqual({
        node_id: node.id,
        status: 'error',
        error: 'boom',
      })
      expect(mockPreviewData).not.toHaveBeenCalled()
      expect(usePipelineStore.getState().previewData).toEqual(expect.objectContaining({
        columns: ['existing'],
      }))
    })
  })

  describe('loadCsvPreview', () => {
    it('loads raw csv preview into the shared preview state', async () => {
      act(() => {
        usePipelineStore.setState({
          nodes: [{
            id: 'csv-node',
            type: 'csv_source',
            position: { x: 0, y: 0 },
            data: {
              label: 'Orders CSV',
              tableName: 'orders_table',
              config: { file_path: '/tmp/orders.csv', original_filename: 'orders.csv' },
            },
          }],
        })
      })

      mockPreviewCsvSource.mockResolvedValueOnce(makeCsvTextPreview())

      await act(async () => {
        await usePipelineStore.getState().loadCsvPreview('csv-node', '/tmp/orders.csv')
      })

      expect(mockPreviewCsvSource).toHaveBeenCalledWith('/tmp/orders.csv')
      expect(usePipelineStore.getState().previewData).toEqual(makeCsvTextPreview())
      expect(usePipelineStore.getState().previewNodeId).toBe('csv-node')
    })

    it('stores the backend preview error message when the request fails', async () => {
      mockPreviewCsvSource.mockRejectedValueOnce({
        response: {
          data: {
            detail: 'Unable to preview CSV: unsupported encoding',
          },
        },
      })

      await act(async () => {
        await usePipelineStore.getState().loadCsvPreview('csv-node', '/tmp/orders.csv')
      })

      expect(usePipelineStore.getState().previewData).toBeNull()
      expect(usePipelineStore.getState().previewError).toBe(
        'Unable to preview CSV: unsupported encoding'
      )
    })
  })

  describe('loadPreprocessedCsvPreview', () => {
    it('stores reviewed preprocess readiness after preview succeeds', async () => {
      act(() => {
        usePipelineStore.setState({
          nodes: [{
            id: 'csv-node',
            type: 'csv_source',
            position: { x: 0, y: 0 },
            data: {
              label: 'Orders CSV',
              tableName: 'orders_table',
              config: {
                file_path: '/tmp/orders.csv',
                original_filename: 'orders.csv',
                preprocessing: { enabled: true, runtime: 'python', script: 'print(1)' },
              },
            },
          }],
        })
      })

      mockPreviewPreprocessedCsvSource.mockResolvedValueOnce(makeCsvTextPreview({
        csv_stage: 'preprocessed',
        artifact_ready: true,
      }))

      await act(async () => {
        await usePipelineStore.getState().loadPreprocessedCsvPreview(
          'csv-node',
          '/tmp/orders.csv',
          { enabled: true, runtime: 'python', script: 'print(1)' },
        )
      })

      expect(mockPreviewPreprocessedCsvSource).toHaveBeenCalledWith(
        'csv-node',
        '/tmp/orders.csv',
        { enabled: true, runtime: 'python', script: 'print(1)' },
      )
      expect(usePipelineStore.getState().previewData).toEqual(makeCsvTextPreview({
        csv_stage: 'preprocessed',
        artifact_ready: true,
      }))
      expect(usePipelineStore.getState().csvPreprocessArtifacts['csv-node']).toBe(
        JSON.stringify({
          file_path: '/tmp/orders.csv',
          runtime: 'python',
          script: 'print(1)',
        })
      )
    })
  })

  describe('runTransformPreview', () => {
    beforeEach(() => {
      vi.spyOn(window, 'confirm').mockReturnValue(true)
    })

    it('runs only the selected transform when upstream tables are already materialized', async () => {
      act(() => {
        usePipelineStore.setState({
          nodes: [
            {
              id: 'src-node',
              type: 'csv_source',
              position: { x: 0, y: 0 },
              data: {
                label: 'Orders CSV',
                tableName: 'orders_table',
                config: { file_path: '/tmp/orders.csv', original_filename: 'orders.csv' },
              },
            },
            {
              id: 'tx-node',
              type: 'transform',
              position: { x: 200, y: 0 },
              data: {
                label: 'Orders Transform',
                tableName: 'orders_filtered',
                config: { sql: 'SELECT * FROM orders_table WHERE id > 1' },
              },
            },
          ],
          edges: [{ id: 'edge-1', source: 'src-node', target: 'tx-node' }],
        })
      })

      mockGetTableSchema.mockResolvedValueOnce({
        table_name: 'orders_table',
        columns: ['id'],
        column_types: ['INTEGER'],
        total_rows: 5,
      })
      mockExecuteNode.mockResolvedValueOnce({
        node_id: 'tx-node',
        status: 'success',
        row_count: 4,
        column_count: 1,
        columns: ['id'],
      })
      mockPreviewData.mockResolvedValueOnce({
        kind: 'table',
        columns: ['id'],
        column_types: ['INTEGER'],
        rows: [[2], [3]],
        total_rows: 4,
        offset: 0,
        limit: 100,
      })

      await act(async () => {
        await usePipelineStore.getState().runTransformPreview('tx-node')
      })

      expect(mockGetTableSchema).toHaveBeenCalledWith('orders_table')
      expect(mockExecuteNode).toHaveBeenCalledWith(expect.objectContaining({
        id: 'tx-node',
        table_name: 'orders_filtered',
      }))
      expect(mockExecutePipeline).not.toHaveBeenCalled()
      expect(mockPreviewData).toHaveBeenCalledWith('orders_filtered', 0)
    })

    it('runs the minimal missing upstream chain after confirmation and preserves unrelated node results', async () => {
      act(() => {
        usePipelineStore.setState({
          nodes: [
            {
              id: 'src-node',
              type: 'csv_source',
              position: { x: 0, y: 0 },
              data: {
                label: 'Orders CSV',
                tableName: 'orders_table',
                config: { file_path: '/tmp/orders.csv', original_filename: 'orders.csv' },
              },
            },
            {
              id: 'mid-node',
              type: 'transform',
              position: { x: 200, y: 0 },
              data: {
                label: 'Orders Mid',
                tableName: 'orders_mid',
                config: { sql: 'SELECT * FROM orders_table' },
              },
            },
            {
              id: 'tx-node',
              type: 'transform',
              position: { x: 400, y: 0 },
              data: {
                label: 'Orders Final',
                tableName: 'orders_final',
                config: { sql: 'SELECT * FROM orders_mid WHERE id > 1' },
              },
            },
          ],
          edges: [
            { id: 'edge-1', source: 'src-node', target: 'mid-node' },
            { id: 'edge-2', source: 'mid-node', target: 'tx-node' },
          ],
          nodeResults: {
            other: { node_id: 'other', status: 'success', row_count: 10, column_count: 2 },
          },
        })
      })

      mockGetTableSchema.mockImplementation(async (tableName: string) => {
        if (tableName === 'orders_table') {
          return {
            table_name: 'orders_table',
            columns: ['id'],
            column_types: ['INTEGER'],
            total_rows: 5,
          }
        }
        if (tableName === 'orders_mid') {
          return null
        }
        return null
      })
      mockExecutePipeline.mockResolvedValueOnce({
        'mid-node': { node_id: 'mid-node', status: 'success', row_count: 5, column_count: 1, columns: ['id'] },
        'tx-node': { node_id: 'tx-node', status: 'success', row_count: 4, column_count: 1, columns: ['id'] },
      })
      mockPreviewData.mockResolvedValueOnce({
        kind: 'table',
        columns: ['id'],
        column_types: ['INTEGER'],
        rows: [[2], [3]],
        total_rows: 4,
        offset: 0,
        limit: 100,
      })

      await act(async () => {
        await usePipelineStore.getState().runTransformPreview('tx-node')
      })

      expect(window.confirm).toHaveBeenCalled()
      expect(mockExecutePipeline).toHaveBeenCalledWith(expect.objectContaining({
        nodes: [
          expect.objectContaining({ id: 'mid-node' }),
          expect.objectContaining({ id: 'tx-node' }),
        ],
        edges: [
          { id: 'edge-2', source: 'mid-node', target: 'tx-node' },
        ],
      }), true)
      expect(mockExecuteNode).not.toHaveBeenCalled()
      expect(mockPreviewData).toHaveBeenCalledWith('orders_final', 0)
      expect(usePipelineStore.getState().nodeResults.other).toEqual(
        expect.objectContaining({ node_id: 'other', status: 'success' })
      )
    })

    it('does not execute or load preview when the user cancels missing upstream execution', async () => {
      vi.mocked(window.confirm).mockReturnValueOnce(false)

      act(() => {
        usePipelineStore.setState({
          nodes: [
            {
              id: 'src-node',
              type: 'csv_source',
              position: { x: 0, y: 0 },
              data: {
                label: 'Orders CSV',
                tableName: 'orders_table',
                config: { file_path: '/tmp/orders.csv', original_filename: 'orders.csv' },
              },
            },
            {
              id: 'tx-node',
              type: 'transform',
              position: { x: 200, y: 0 },
              data: {
                label: 'Orders Final',
                tableName: 'orders_final',
                config: { sql: 'SELECT * FROM orders_table WHERE id > 1' },
              },
            },
          ],
          edges: [{ id: 'edge-1', source: 'src-node', target: 'tx-node' }],
        })
      })

      mockGetTableSchema.mockResolvedValueOnce(null)

      await act(async () => {
        await usePipelineStore.getState().runTransformPreview('tx-node')
      })

      expect(mockExecutePipeline).not.toHaveBeenCalled()
      expect(mockExecuteNode).not.toHaveBeenCalled()
      expect(mockPreviewData).not.toHaveBeenCalled()
    })
  })

  describe('node error dialog', () => {
    it('opens and closes the error dialog for a node', () => {
      act(() => {
        usePipelineStore.getState().addNode('csv_source', { x: 0, y: 0 })
      })

      const node = usePipelineStore.getState().nodes[0]

      act(() => {
        usePipelineStore.getState().openNodeError(node.id)
      })
      expect(usePipelineStore.getState().errorDialogNodeId).toBe(node.id)

      act(() => {
        usePipelineStore.getState().closeNodeError()
      })
      expect(usePipelineStore.getState().errorDialogNodeId).toBeNull()
    })
  })
})
