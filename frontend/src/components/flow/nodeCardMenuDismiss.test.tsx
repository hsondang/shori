import { render, screen } from '@testing-library/react'
import { act } from 'react'
import { describe, it, expect } from 'vitest'
import userEvent from '@testing-library/user-event'
import { NodeCard, NODE_CARD_DISMISS_EVENT } from '@shori/design-system'

// A node-card action menu is portaled to <body> at a screen position captured
// when it opens, so a canvas pan/zoom would strand it away from its button.
// FlowCanvas fires NODE_CARD_DISMISS_EVENT on viewport change so the open menu
// dismisses instead. That coupling is a bare event-name string with no
// compile-time link between the two packages, so pin the listener half of the
// contract here (@shori/design-system ships no test harness of its own).
describe('NodeCard action menu — dismiss on viewport change', () => {
  const actions = [
    { label: 'Preview' },
    { label: 'Load to memory' },
    { label: 'Materialize' },
  ]

  it('closes an open menu when NODE_CARD_DISMISS_EVENT fires on window', async () => {
    const user = userEvent.setup()
    render(<NodeCard kind="db" accent="postgres" title="KM Prod" actions={actions} />)

    await user.click(screen.getByRole('button', { name: /more actions/i }))
    expect(screen.getByRole('menu')).toBeInTheDocument()

    act(() => {
      window.dispatchEvent(new Event(NODE_CARD_DISMISS_EVENT))
    })

    expect(screen.queryByRole('menu')).not.toBeInTheDocument()
  })
})
