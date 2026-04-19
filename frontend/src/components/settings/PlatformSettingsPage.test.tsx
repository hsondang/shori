import { fireEvent, render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { act } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import PlatformSettingsPage from './PlatformSettingsPage'
import { useSettingsStore } from '../../store/settingsStore'

const mockListGlobalDatabaseConnections = vi.fn()
const mockCreateGlobalDatabaseConnection = vi.fn()
const mockUpdateGlobalDatabaseConnection = vi.fn()
const mockDeleteGlobalDatabaseConnection = vi.fn()
const mockTestDbConnection = vi.fn()

vi.mock('../../api/client', () => ({
  listGlobalDatabaseConnections: (...args: unknown[]) => mockListGlobalDatabaseConnections(...args),
  createGlobalDatabaseConnection: (...args: unknown[]) => mockCreateGlobalDatabaseConnection(...args),
  updateGlobalDatabaseConnection: (...args: unknown[]) => mockUpdateGlobalDatabaseConnection(...args),
  deleteGlobalDatabaseConnection: (...args: unknown[]) => mockDeleteGlobalDatabaseConnection(...args),
  testDbConnection: (...args: unknown[]) => mockTestDbConnection(...args),
}))

describe('PlatformSettingsPage', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockListGlobalDatabaseConnections.mockResolvedValue([])
    mockCreateGlobalDatabaseConnection.mockImplementation(async (connection) => ({ id: 'global-1', ...connection }))
    mockUpdateGlobalDatabaseConnection.mockImplementation(async (id, connection) => ({ id, ...connection }))
    mockDeleteGlobalDatabaseConnection.mockResolvedValue(undefined)
    act(() => {
      useSettingsStore.setState({
        globalDatabaseConnections: [],
        globalConnectionsLoaded: false,
        globalConnectionsLoading: false,
      })
    })
  })

  it('creates, edits, and deletes global database connections', async () => {
    const user = userEvent.setup()
    render(<PlatformSettingsPage />)

    await user.click(await screen.findByRole('button', { name: 'Add Global Connection' }))
    const modal = screen.getByTestId('global-connection-modal')
    fireEvent.change(within(modal).getByLabelText(/connection name/i), { target: { value: 'Shared Warehouse' } })
    fireEvent.change(screen.getByPlaceholderText('Host'), { target: { value: 'db.internal' } })
    fireEvent.change(screen.getByPlaceholderText('Database'), { target: { value: 'warehouse' } })
    await user.click(within(modal).getByRole('button', { name: 'Save' }))

    expect(await screen.findByText('Shared Warehouse')).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'Edit' }))
    const editModal = screen.getByTestId('global-connection-modal')
    await user.clear(within(editModal).getByLabelText(/connection name/i))
    await user.type(within(editModal).getByLabelText(/connection name/i), 'Shared Warehouse Prod')
    await user.click(within(editModal).getByRole('button', { name: 'Save' }))

    expect(await screen.findByText('Shared Warehouse Prod')).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'Delete' }))
    expect(mockDeleteGlobalDatabaseConnection).toHaveBeenCalledWith('global-1')
  })
})
