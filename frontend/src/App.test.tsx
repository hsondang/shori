import type { ReactNode } from 'react'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import { describe, expect, it, vi } from 'vitest'
import App from './App'
import {
  PROJECT_BROWSER_SIDEBAR_WIDTH_PX,
  PROJECT_BROWSER_TRIGGER_SLOT_PX,
} from './components/projects/projectLayout'

const mockLoadGlobalDatabaseConnections = vi.fn(() => Promise.resolve())
const mockProjectSidebar = vi.fn(
  ({ open, variant }: { open: boolean; variant?: 'docked' | 'overlay' }) => (
    <aside data-testid={`sidebar-${variant ?? 'overlay'}`} data-open={open ? 'true' : 'false'}>
      {open ? 'Sidebar Open' : 'Sidebar Closed'}
    </aside>
  )
)

vi.mock('@xyflow/react', () => ({
  ReactFlowProvider: ({ children }: { children: ReactNode }) => <>{children}</>,
}))

vi.mock('./components/projects/ProjectSidebar', () => ({
  default: (props: { open: boolean; variant?: 'docked' | 'overlay' }) => mockProjectSidebar(props),
}))

vi.mock('./components/projects/ProjectHome', () => ({
  default: () => <div>Home View</div>,
}))

vi.mock('./components/projects/PipelineEditorPage', () => ({
  default: () => <div>Editor View</div>,
}))

vi.mock('./components/settings/PlatformSettingsPage', () => ({
  default: () => <div>Platform Settings View</div>,
}))

vi.mock('./components/toolbar/Toolbar', () => ({
  default: () => <div data-testid="toolbar">Toolbar</div>,
}))

vi.mock('./store/settingsStore', () => ({
  useSettingsStore: (selector: (state: { loadGlobalDatabaseConnections: () => Promise<void> }) => unknown) =>
    selector({ loadGlobalDatabaseConnections: mockLoadGlobalDatabaseConnections }),
}))

describe('App routes', () => {
  it('renders the home route at /', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <App />
      </MemoryRouter>
    )

    expect(screen.getByTestId('sidebar-overlay')).toHaveTextContent('Sidebar Open')
    expect(screen.getByText('Home View')).toBeInTheDocument()
  })

  it('renders the platform settings route', () => {
    render(
      <MemoryRouter initialEntries={['/settings/platform']}>
        <App />
      </MemoryRouter>
    )

    expect(screen.getByTestId('sidebar-overlay')).toHaveTextContent('Sidebar Closed')
    expect(screen.getByText('Platform Settings View')).toBeInTheDocument()
  })

  it('renders the editor route with a docked sidebar rail and mobile-only backdrop', async () => {
    const user = userEvent.setup()

    render(
      <MemoryRouter initialEntries={['/projects/project-1']}>
        <App />
      </MemoryRouter>
    )

    expect(screen.getByTestId('sidebar-docked')).toHaveTextContent('Sidebar Closed')
    expect(screen.getByTestId('project-editor-header')).toBeInTheDocument()
    expect(screen.getByTestId('project-browser-trigger-slot')).toHaveStyle({ width: `${PROJECT_BROWSER_TRIGGER_SLOT_PX}px` })
    expect(screen.getByTestId('project-editor-body')).toBeInTheDocument()
    expect(screen.getByTestId('project-sidebar-rail')).toHaveStyle({ width: '0px' })
    expect(screen.getByRole('button', { name: /open project browser/i })).toBeInTheDocument()
    expect(screen.getByTestId('toolbar')).toBeInTheDocument()
    expect(screen.getByText('Editor View')).toBeInTheDocument()
    expect(screen.queryByTestId('project-browser-mobile-backdrop')).not.toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: /open project browser/i }))

    expect(screen.getByTestId('sidebar-docked')).toHaveTextContent('Sidebar Open')
    expect(screen.getByTestId('project-sidebar-rail')).toHaveStyle({
      width: `${PROJECT_BROWSER_SIDEBAR_WIDTH_PX}px`,
    })
    expect(screen.getByTestId('project-browser-mobile-backdrop')).toHaveClass('md:hidden')
  })
})
