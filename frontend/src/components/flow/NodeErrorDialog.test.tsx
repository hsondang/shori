import { act } from 'react'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, it } from 'vitest'
import NodeErrorDialog from './NodeErrorDialog'
import { usePipelineStore } from '../../store/pipelineStore'

describe('NodeErrorDialog', () => {
  beforeEach(() => {
    act(() => {
      usePipelineStore.getState().newPipeline()
      usePipelineStore.getState().addNode('csv_source', { x: 0, y: 0 })
    })

    const node = usePipelineStore.getState().nodes[0]
    act(() => {
      usePipelineStore.getState().updateNodeData(node.id, { label: 'Broken CSV' })
      usePipelineStore.setState({
        nodeResults: {
          [node.id]: {
            node_id: node.id,
            status: 'error',
            error: 'Invalid Input Error: CSV Error on Line 2, expected 4 columns but found 1',
          },
        },
        errorDialogNodeId: node.id,
      })
    })
  })

  it('shows the full error text for the selected node', () => {
    render(<NodeErrorDialog />)

    expect(screen.getByRole('dialog')).toBeInTheDocument()
    expect(screen.getByText('Broken CSV')).toBeInTheDocument()
    expect(screen.getByText('CSV Source')).toBeInTheDocument()
    expect(screen.getByText(/expected 4 columns but found 1/i)).toBeInTheDocument()
  })

  it('closes when the dismiss button is clicked', async () => {
    const user = userEvent.setup()
    render(<NodeErrorDialog />)

    await user.click(screen.getByRole('button', { name: /dismiss/i }))

    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
    expect(usePipelineStore.getState().errorDialogNodeId).toBeNull()
  })

  it('closes when escape is pressed', async () => {
    const user = userEvent.setup()
    render(<NodeErrorDialog />)

    await user.keyboard('{Escape}')

    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
    expect(usePipelineStore.getState().errorDialogNodeId).toBeNull()
  })
})
