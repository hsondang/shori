import { render, screen, waitFor, fireEvent } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import ConnectionForm from './ConnectionForm'

const mockTestDbConnection = vi.fn()

vi.mock('../../api/client', () => ({
  testDbConnection: (...args: unknown[]) => mockTestDbConnection(...args),
}))

beforeEach(() => {
  vi.clearAllMocks()
})

describe('ConnectionForm — postgres', () => {
  it('renders Database field label', () => {
    render(<ConnectionForm config={{}} onChange={() => {}} dbType="postgres" />)
    expect(screen.getByPlaceholderText('Database')).toBeInTheDocument()
  })

  it('renders default port 5432', () => {
    render(<ConnectionForm config={{}} onChange={() => {}} dbType="postgres" />)
    const portInput = screen.getByPlaceholderText('Port') as HTMLInputElement
    expect(portInput.value).toBe('5432')
  })

  it('fires onChange with updated host', () => {
    const onChange = vi.fn()
    render(<ConnectionForm config={{ host: '' }} onChange={onChange} dbType="postgres" />)
    fireEvent.change(screen.getByPlaceholderText('Host'), { target: { value: 'localhost' } })
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ host: 'localhost' }))
  })

  it('shows Test Connection button', () => {
    render(<ConnectionForm config={{}} onChange={() => {}} dbType="postgres" />)
    expect(screen.getByRole('button', { name: /test connection/i })).toBeInTheDocument()
  })

  it('shows success message after successful test', async () => {
    mockTestDbConnection.mockResolvedValueOnce({ success: true })
    render(<ConnectionForm config={{}} onChange={() => {}} dbType="postgres" />)
    await userEvent.click(screen.getByRole('button', { name: /test connection/i }))
    await waitFor(() => expect(screen.getByText(/connected successfully/i)).toBeInTheDocument())
  })

  it('shows error message after failed test', async () => {
    mockTestDbConnection.mockResolvedValueOnce({ success: false, error: 'Connection refused' })
    render(<ConnectionForm config={{}} onChange={() => {}} dbType="postgres" />)
    await userEvent.click(screen.getByRole('button', { name: /test connection/i }))
    await waitFor(() => expect(screen.getByText('Connection refused')).toBeInTheDocument())
  })
})

describe('ConnectionForm — oracle', () => {
  it('renders Service Name field label', () => {
    render(<ConnectionForm config={{}} onChange={() => {}} dbType="oracle" />)
    expect(screen.getByPlaceholderText('Service Name')).toBeInTheDocument()
  })

  it('renders default port 1521', () => {
    render(<ConnectionForm config={{}} onChange={() => {}} dbType="oracle" />)
    const portInput = screen.getByPlaceholderText('Port') as HTMLInputElement
    expect(portInput.value).toBe('1521')
  })

  it('calls test with oracle dbType', async () => {
    mockTestDbConnection.mockResolvedValueOnce({ success: true })
    render(<ConnectionForm config={{}} onChange={() => {}} dbType="oracle" />)
    await userEvent.click(screen.getByRole('button', { name: /test connection/i }))
    await waitFor(() => expect(mockTestDbConnection).toHaveBeenCalledWith('oracle', expect.any(Object)))
  })
})
