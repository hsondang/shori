import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { act } from 'react'
import { MemoryRouter, Route, Routes, useLocation } from 'react-router-dom'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import ProjectSidebar from './ProjectSidebar'
import { usePipelineStore } from '../../store/pipelineStore'

const mockListPipelines = vi.fn()
const mockSavePipeline = vi.fn()

vi.mock('../../api/client', () => ({
  listPipelines: (...args: unknown[]) => mockListPipelines(...args),
  savePipeline: (...args: unknown[]) => mockSavePipeline(...args),
}))

function LocationProbe() {
  const location = useLocation()
  return <div data-testid="location">{location.pathname}</div>
}

function renderSidebar(initialPath = '/') {
  return render(
    <MemoryRouter initialEntries={[initialPath]}>
      <div className="flex h-full">
        <ProjectSidebar />
        <Routes>
          <Route path="/" element={<LocationProbe />} />
          <Route path="/projects/:projectId" element={<LocationProbe />} />
        </Routes>
      </div>
    </MemoryRouter>
  )
}

describe('ProjectSidebar', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    act(() => usePipelineStore.getState().newPipeline())
    vi.spyOn(window, 'confirm').mockReturnValue(true)
  })

  it('renders projects in the order returned by the API', async () => {
    mockListPipelines.mockResolvedValueOnce([
      { id: 'p2', name: 'Beta', created_at: '2026-03-24T10:00:00Z', updated_at: '2026-03-24T10:00:00Z' },
      { id: 'p1', name: 'Alpha', created_at: '2026-03-23T10:00:00Z', updated_at: '2026-03-23T10:00:00Z' },
    ])

    const { container } = renderSidebar()

    await waitFor(() => {
      expect(screen.getByRole('button', { name: /beta/i })).toBeInTheDocument()
      expect(screen.getByRole('button', { name: /alpha/i })).toBeInTheDocument()
    })

    const buttons = [...container.querySelectorAll('aside button')]
      .map((button) => button.textContent?.trim())
      .filter((text): text is string => Boolean(text) && text.includes('Updated'))

    expect(buttons[0]).toContain('Beta')
    expect(buttons[1]).toContain('Alpha')
  })

  it('navigates to the selected project when clicked', async () => {
    const user = userEvent.setup()
    mockListPipelines.mockResolvedValueOnce([
      { id: 'p1', name: 'Warehouse', created_at: '2026-03-24T10:00:00Z', updated_at: '2026-03-24T10:00:00Z' },
    ])

    renderSidebar()

    await user.click(await screen.findByRole('button', { name: /warehouse/i }))

    expect(screen.getByTestId('location')).toHaveTextContent('/projects/p1')
  })

  it('creates an untitled project and navigates to it', async () => {
    const user = userEvent.setup()
    mockListPipelines.mockResolvedValue([])
    mockSavePipeline.mockResolvedValue(undefined)

    renderSidebar()

    await user.click(screen.getByRole('button', { name: 'New Project' }))

    expect(mockSavePipeline).toHaveBeenCalledWith(expect.objectContaining({
      name: 'Untitled Pipeline',
      nodes: [],
      edges: [],
      database_connections: [],
    }))

    const createdId = mockSavePipeline.mock.calls[0][0].id
    expect(screen.getByTestId('location')).toHaveTextContent(`/projects/${createdId}`)
  })

  it('warns before switching projects when the current project has unsaved changes', async () => {
    const user = userEvent.setup()
    mockListPipelines.mockResolvedValueOnce([
      { id: 'p2', name: 'Finance', created_at: '2026-03-24T10:00:00Z', updated_at: '2026-03-24T10:00:00Z' },
    ])
    act(() => {
      usePipelineStore.setState({
        pipelineId: 'p1',
        pipelineName: 'Current',
        savedPipelineSnapshot: JSON.stringify({
          id: 'p1',
          name: 'Current',
          database_connections: [],
          nodes: [],
          edges: [],
        }),
        hasUnsavedChanges: true,
      })
    })

    renderSidebar('/projects/p1')

    await user.click(await screen.findByRole('button', { name: /finance/i }))

    expect(window.confirm).toHaveBeenCalledWith('You have unsaved changes. Discard them and open "Finance"?')
    expect(screen.getByTestId('location')).toHaveTextContent('/projects/p2')
  })
})
