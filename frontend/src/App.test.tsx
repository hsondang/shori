import type { ReactNode } from 'react'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { describe, expect, it, vi } from 'vitest'
import App from './App'

vi.mock('@xyflow/react', () => ({
  ReactFlowProvider: ({ children }: { children: ReactNode }) => <>{children}</>,
}))

vi.mock('./components/projects/ProjectSidebar', () => ({
  default: () => <aside>Sidebar</aside>,
}))

vi.mock('./components/projects/ProjectHome', () => ({
  default: () => <div>Home View</div>,
}))

vi.mock('./components/projects/PipelineEditorPage', () => ({
  default: () => <div>Editor View</div>,
}))

describe('App routes', () => {
  it('renders the home route at /', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <App />
      </MemoryRouter>
    )

    expect(screen.getByText('Sidebar')).toBeInTheDocument()
    expect(screen.getByText('Home View')).toBeInTheDocument()
  })

  it('renders the editor route at /projects/:projectId', () => {
    render(
      <MemoryRouter initialEntries={['/projects/project-1']}>
        <App />
      </MemoryRouter>
    )

    expect(screen.getByText('Sidebar')).toBeInTheDocument()
    expect(screen.getByText('Editor View')).toBeInTheDocument()
  })
})
