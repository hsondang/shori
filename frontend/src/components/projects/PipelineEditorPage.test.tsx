import { act } from 'react'
import { fireEvent, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import PipelineEditorPage from './PipelineEditorPage'
import { usePipelineStore } from '../../store/pipelineStore'

vi.mock('../flow/FlowCanvas', () => ({
  default: () => <div data-testid="flow-canvas">Flow Canvas</div>,
}))

vi.mock('../flow/NodeErrorDialog', () => ({
  default: () => <div data-testid="node-error-dialog">Node Error Dialog</div>,
}))

vi.mock('../panels/NodeConfigPanel', () => ({
  default: () => <aside data-testid="node-config-panel">Node Config Panel</aside>,
}))

vi.mock('../panels/DataPreviewPanel', () => ({
  default: () => <div data-testid="data-preview-panel">Data Preview Panel</div>,
}))

function renderEditor(projectId = 'project-1') {
  return render(
    <MemoryRouter initialEntries={[`/projects/${projectId}`]}>
      <Routes>
        <Route path="/projects/:projectId" element={<PipelineEditorPage />} />
      </Routes>
    </MemoryRouter>
  )
}

describe('PipelineEditorPage', () => {
  beforeEach(() => {
    Object.defineProperty(HTMLElement.prototype, 'clientHeight', {
      configurable: true,
      get: () => 800,
    })

    act(() => usePipelineStore.getState().newPipeline())
  })

  it('renders the editor content without the old left-padding toolbar wrapper', () => {
    act(() => {
      usePipelineStore.setState({
        pipelineId: 'project-1',
      })
    })

    const { container } = renderEditor()

    expect(screen.getByTestId('flow-canvas')).toBeInTheDocument()
    expect(screen.getByTestId('node-config-panel')).toBeInTheDocument()
    expect(screen.getByTestId('data-preview-panel')).toBeInTheDocument()
    expect(screen.getByTestId('preview-panel-shell')).toHaveAttribute('data-layout-state', 'expanded')
    expect(screen.getByTestId('preview-resize-handle')).toBeInTheDocument()
    expect(screen.getByTestId('node-error-dialog')).toBeInTheDocument()
    expect(container.querySelector('.pl-\\[3\\.5rem\\]')).toBeNull()
  })

  it('collapses the preview to a header-only bar and restores it', async () => {
    const user = userEvent.setup()

    act(() => {
      usePipelineStore.setState({
        pipelineId: 'project-1',
      })
    })

    renderEditor()

    const previewShell = screen.getByTestId('preview-panel-shell')
    expect(previewShell).toHaveAttribute('data-layout-state', 'expanded')
    expect(screen.getByTestId('data-preview-panel')).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'Collapse data preview' }))

    expect(previewShell).toHaveAttribute('data-layout-state', 'collapsed')
    expect(screen.queryByTestId('data-preview-panel')).not.toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'Expand data preview' }))

    expect(previewShell).toHaveAttribute('data-layout-state', 'expanded')
    expect(screen.getByTestId('data-preview-panel')).toBeInTheDocument()
  })

  it('resizes the preview vertically from the drag handle', () => {
    act(() => {
      usePipelineStore.setState({
        pipelineId: 'project-1',
      })
    })

    renderEditor()

    const previewShell = screen.getByTestId('preview-panel-shell')
    const resizeHandle = screen.getByTestId('preview-resize-handle')

    expect(previewShell).toHaveStyle({ height: '256px' })

    fireEvent.mouseDown(resizeHandle, { clientY: 500 })
    fireEvent.mouseMove(window, { clientY: 400 })
    fireEvent.mouseUp(window)

    expect(previewShell).toHaveStyle({ height: '356px' })
  })
})
