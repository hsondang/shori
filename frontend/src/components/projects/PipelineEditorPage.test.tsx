import { act } from 'react'
import { render, screen } from '@testing-library/react'
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
    expect(screen.getByTestId('node-error-dialog')).toBeInTheDocument()
    expect(container.querySelector('.pl-\\[3\\.5rem\\]')).toBeNull()
  })
})
