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

  it('offers the export permission toggle for oracle connections only', async () => {
    const user = userEvent.setup()
    render(<PlatformSettingsPage />)

    await user.click(await screen.findByRole('button', { name: 'Add Global Connection' }))
    const modal = screen.getByTestId('global-connection-modal')

    // Postgres is the default type and has no export concept.
    expect(within(modal).queryByRole('switch', { name: /allow exports/i })).not.toBeInTheDocument()

    fireEvent.change(within(modal).getByLabelText(/database type/i), { target: { value: 'oracle' } })
    expect(within(modal).getByRole('switch', { name: /allow exports/i })).toBeInTheDocument()
  })

  it('defaults the export permission to off and saves it when granted', async () => {
    const user = userEvent.setup()
    render(<PlatformSettingsPage />)

    await user.click(await screen.findByRole('button', { name: 'Add Global Connection' }))
    const modal = screen.getByTestId('global-connection-modal')
    fireEvent.change(within(modal).getByLabelText(/connection name/i), { target: { value: 'Oracle Prod' } })
    fireEvent.change(within(modal).getByLabelText(/database type/i), { target: { value: 'oracle' } })

    const toggle = within(modal).getByRole('switch', { name: /allow exports/i })
    expect(toggle).toHaveAttribute('aria-checked', 'false')

    await user.click(toggle)
    await user.click(within(modal).getByRole('button', { name: 'Save' }))

    expect(mockCreateGlobalDatabaseConnection).toHaveBeenCalledWith(
      expect.objectContaining({ db_type: 'oracle', allow_export: true }),
    )
  })

  it('does not carry export permission across a database type switch', async () => {
    const user = userEvent.setup()
    render(<PlatformSettingsPage />)

    await user.click(await screen.findByRole('button', { name: 'Add Global Connection' }))
    const modal = screen.getByTestId('global-connection-modal')
    fireEvent.change(within(modal).getByLabelText(/database type/i), { target: { value: 'oracle' } })
    await user.click(within(modal).getByRole('switch', { name: /allow exports/i }))
    expect(within(modal).getByRole('switch', { name: /allow exports/i })).toHaveAttribute('aria-checked', 'true')

    // Away to postgres and back: the approval was granted to a different
    // database, so it must not silently survive.
    fireEvent.change(within(modal).getByLabelText(/database type/i), { target: { value: 'postgres' } })
    fireEvent.change(within(modal).getByLabelText(/database type/i), { target: { value: 'oracle' } })

    expect(within(modal).getByRole('switch', { name: /allow exports/i })).toHaveAttribute('aria-checked', 'false')
  })

  it('badges connections that are approved as export destinations', async () => {
    mockListGlobalDatabaseConnections.mockResolvedValue([
      { id: 'g1', name: 'Oracle Prod', db_type: 'oracle', host: 'h', port: 1521, service_name: 'KM', user: 'u', password: 'p', allow_export: true },
      { id: 'g2', name: 'Oracle Dev', db_type: 'oracle', host: 'h', port: 1521, service_name: 'KM', user: 'u', password: 'p', allow_export: false },
    ])
    render(<PlatformSettingsPage />)

    const approved = (await screen.findByText('Oracle Prod')).closest('div.flex') as HTMLElement
    expect(within(approved).getByText('Export enabled')).toBeInTheDocument()

    const notApproved = screen.getByText('Oracle Dev').closest('div.flex') as HTMLElement
    expect(within(notApproved).queryByText('Export enabled')).not.toBeInTheDocument()
  })
})
