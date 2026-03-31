import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { act } from 'react'
import { MemoryRouter, Route, Routes, useLocation } from 'react-router-dom'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import ProjectSidebar from './ProjectSidebar'
import { usePipelineStore } from '../../store/pipelineStore'
import { PROJECT_BROWSER_TRIGGER_SLOT_PX } from './projectLayout'

const mockListPipelines = vi.fn()
const mockSavePipeline = vi.fn()
const mockDeletePipeline = vi.fn()
const mockSetPipelineStar = vi.fn()

vi.mock('../../api/client', () => ({
  listPipelines: (...args: unknown[]) => mockListPipelines(...args),
  savePipeline: (...args: unknown[]) => mockSavePipeline(...args),
  deletePipeline: (...args: unknown[]) => mockDeletePipeline(...args),
  setPipelineStar: (...args: unknown[]) => mockSetPipelineStar(...args),
}))

function LocationProbe() {
  const location = useLocation()
  return <div data-testid="location">{location.pathname}</div>
}

function renderSidebar(initialPath = '/', variant: 'docked' | 'overlay' = 'overlay') {
  return render(
    <MemoryRouter initialEntries={[initialPath]}>
      <div className="flex h-full">
        <ProjectSidebar open onClose={() => {}} variant={variant} />
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
      { id: 'p2', name: 'Beta', starred: false, created_at: '2026-03-24T10:00:00Z', updated_at: '2026-03-24T10:00:00Z' },
      { id: 'p1', name: 'Alpha', starred: false, created_at: '2026-03-23T10:00:00Z', updated_at: '2026-03-23T10:00:00Z' },
    ])

    const { container } = renderSidebar()

    await waitFor(() => {
      expect(screen.getByRole('button', { name: /open project beta/i })).toBeInTheDocument()
      expect(screen.getByRole('button', { name: /open project alpha/i })).toBeInTheDocument()
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
      { id: 'p1', name: 'Warehouse', starred: false, created_at: '2026-03-24T10:00:00Z', updated_at: '2026-03-24T10:00:00Z' },
    ])

    renderSidebar()

    await user.click(await screen.findByRole('button', { name: /open project warehouse/i }))

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
    await waitFor(() => {
      expect(screen.getByTestId('location')).toHaveTextContent(`/projects/${createdId}`)
    })
  })

  it('warns before switching projects when the current project has unsaved changes', async () => {
    const user = userEvent.setup()
    mockListPipelines.mockResolvedValueOnce([
      { id: 'p2', name: 'Finance', starred: false, created_at: '2026-03-24T10:00:00Z', updated_at: '2026-03-24T10:00:00Z' },
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

    await user.click(await screen.findByRole('button', { name: /open project finance/i }))

    expect(window.confirm).toHaveBeenCalledWith('You have unsaved changes. Discard them and open "Finance"?')
    expect(screen.getByTestId('location')).toHaveTextContent('/projects/p2')
  })

  it('toggles a project star from the more options menu', async () => {
    const user = userEvent.setup()
    mockListPipelines.mockResolvedValueOnce([
      { id: 'p1', name: 'Warehouse', starred: false, created_at: '2026-03-24T10:00:00Z', updated_at: '2026-03-24T10:00:00Z' },
    ])
    mockSetPipelineStar.mockResolvedValueOnce({ id: 'p1', starred: true })

    renderSidebar()

    await user.click(await screen.findByRole('button', { name: /more options for warehouse/i }))
    await user.click(screen.getByRole('button', { name: 'Star' }))

    expect(mockSetPipelineStar).toHaveBeenCalledWith('p1', true)
  })

  it('deletes the active project and returns to the home route', async () => {
    const user = userEvent.setup()
    mockListPipelines.mockResolvedValueOnce([
      { id: 'p1', name: 'Warehouse', starred: true, created_at: '2026-03-24T10:00:00Z', updated_at: '2026-03-24T10:00:00Z' },
    ])
    mockDeletePipeline.mockResolvedValueOnce(undefined)

    renderSidebar('/projects/p1')

    await user.click(await screen.findByRole('button', { name: /more options for warehouse/i }))
    await user.click(screen.getByRole('button', { name: 'Delete' }))

    expect(window.confirm).toHaveBeenCalledWith('Delete "Warehouse"? This cannot be undone.')
    expect(mockDeletePipeline).toHaveBeenCalledWith('p1')
    expect(screen.getByTestId('location')).toHaveTextContent('/')
  })

  it('renders the sidebar header with shori offset to the right of the trigger area', async () => {
    mockListPipelines.mockResolvedValueOnce([])

    renderSidebar()

    await waitFor(() => {
      expect(screen.getByText('Projects')).toBeInTheDocument()
    })

    expect(screen.getByText('Shori').parentElement).toHaveStyle({ paddingLeft: `${PROJECT_BROWSER_TRIGGER_SLOT_PX}px` })
  })

  it('uses docked styling when rendered in docked mode', async () => {
    mockListPipelines.mockResolvedValueOnce([])

    renderSidebar('/', 'docked')

    const sidebar = await screen.findByRole('complementary')
    expect(sidebar).toHaveAttribute('data-variant', 'docked')
    expect(sidebar.className).toContain('relative')
    expect(sidebar.className).not.toContain('absolute inset-y-0 left-0')
  })

  it('uses slide-over styling when rendered in overlay mode', async () => {
    mockListPipelines.mockResolvedValueOnce([])

    renderSidebar('/', 'overlay')

    const sidebar = await screen.findByRole('complementary')
    expect(sidebar).toHaveAttribute('data-variant', 'overlay')
    expect(sidebar.className).toContain('absolute inset-y-0 left-0')
    expect(sidebar.className).toContain('transition-transform')
  })
})
