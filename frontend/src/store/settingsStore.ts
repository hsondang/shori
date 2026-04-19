import { create } from 'zustand'
import * as api from '../api/client'
import type { SavedDatabaseConnection, SavedDatabaseConnectionInput } from '../types/pipeline'

interface SettingsState {
  globalDatabaseConnections: SavedDatabaseConnection[]
  globalConnectionsLoaded: boolean
  globalConnectionsLoading: boolean
  loadGlobalDatabaseConnections: (options?: { force?: boolean }) => Promise<void>
  createGlobalDatabaseConnection: (connection: SavedDatabaseConnectionInput) => Promise<SavedDatabaseConnection>
  updateGlobalDatabaseConnection: (id: string, connection: SavedDatabaseConnectionInput) => Promise<SavedDatabaseConnection>
  deleteGlobalDatabaseConnection: (id: string) => Promise<void>
}

export const useSettingsStore = create<SettingsState>((set, get) => ({
  globalDatabaseConnections: [],
  globalConnectionsLoaded: false,
  globalConnectionsLoading: false,

  loadGlobalDatabaseConnections: async (options) => {
    if (get().globalConnectionsLoading) return
    if (get().globalConnectionsLoaded && !options?.force) return

    set({ globalConnectionsLoading: true })
    try {
      const connections = await api.listGlobalDatabaseConnections()
      set({
        globalDatabaseConnections: connections,
        globalConnectionsLoaded: true,
        globalConnectionsLoading: false,
      })
    } catch (error) {
      set({ globalConnectionsLoading: false })
      throw error
    }
  },

  createGlobalDatabaseConnection: async (connection) => {
    const created = await api.createGlobalDatabaseConnection(connection)
    set((state) => ({
      globalDatabaseConnections: [...state.globalDatabaseConnections, created].sort((a, b) =>
        a.name.localeCompare(b.name, undefined, { sensitivity: 'base' })
      ),
      globalConnectionsLoaded: true,
    }))
    return created
  },

  updateGlobalDatabaseConnection: async (id, connection) => {
    const updated = await api.updateGlobalDatabaseConnection(id, connection)
    set((state) => ({
      globalDatabaseConnections: state.globalDatabaseConnections
        .map((item) => item.id === id ? updated : item)
        .sort((a, b) => a.name.localeCompare(b.name, undefined, { sensitivity: 'base' })),
      globalConnectionsLoaded: true,
    }))
    return updated
  },

  deleteGlobalDatabaseConnection: async (id) => {
    await api.deleteGlobalDatabaseConnection(id)
    set((state) => ({
      globalDatabaseConnections: state.globalDatabaseConnections.filter((item) => item.id !== id),
      globalConnectionsLoaded: true,
    }))
  },
}))
