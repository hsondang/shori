import { act } from '@testing-library/react'
import { describe, it, expect, beforeEach, vi } from 'vitest'
import { usePipelineStore } from './pipelineStore'

const mockExecutePipeline = vi.fn()
const mockPreviewData = vi.fn()
const mockSavePipeline = vi.fn()
const mockLoadPipeline = vi.fn()
const mockListPipelines = vi.fn()

// Prevent real API calls
vi.mock('../api/client', () => ({
  executePipeline: (...args: unknown[]) => mockExecutePipeline(...args),
  previewData: (...args: unknown[]) => mockPreviewData(...args),
  savePipeline: (...args: unknown[]) => mockSavePipeline(...args),
  loadPipeline: (...args: unknown[]) => mockLoadPipeline(...args),
  listPipelines: (...args: unknown[]) => mockListPipelines(...args),
}))

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
    })
  })
})
