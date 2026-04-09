import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import SqlEditor from './SqlEditor'

const mockEditor = vi.fn(
  ({
    value,
    onChange,
    options,
  }: {
    value: string
    onChange?: (value: string) => void
    options?: Record<string, unknown>
  }) => (
    <div data-testid="sql-editor-shell" data-options={JSON.stringify(options)}>
      <textarea aria-label="sql-editor" value={value} onChange={(event) => onChange?.(event.target.value)} />
    </div>
  )
)

vi.mock('@monaco-editor/react', () => ({
  default: (props: {
    value: string
    onChange?: (value: string) => void
    options?: Record<string, unknown>
  }) => mockEditor(props),
}))

describe('SqlEditor', () => {
  it('disables Monaco suggestions that can interfere with typing spaces', () => {
    render(<SqlEditor value="SELECT 1" onChange={() => {}} upstreamTables={[]} />)

    expect(screen.getByLabelText('sql-editor')).toHaveValue('SELECT 1')
    expect(mockEditor).toHaveBeenCalledTimes(1)

    const [{ options }] = mockEditor.mock.calls[0] as [{ options: Record<string, unknown> }]
    expect(options.quickSuggestions).toBe(false)
    expect(options.suggestOnTriggerCharacters).toBe(false)
    expect(options.parameterHints).toEqual({ enabled: false })
    expect(options.inlineSuggest).toEqual({ enabled: false })
    expect(options.wordBasedSuggestions).toBe('off')
    expect(options.tabCompletion).toBe('off')
    expect(options.acceptSuggestionOnEnter).toBe('off')
  })

  it('marks the editor container as exempt from React Flow keyboard and pan handling', () => {
    render(<SqlEditor value="SELECT 1" onChange={() => {}} upstreamTables={[]} />)

    expect(screen.getByLabelText('sql-editor')).toHaveValue('SELECT 1')
    expect(mockEditor).toHaveBeenCalledTimes(1)

    expect(screen.getByTestId('sql-editor-shell').parentElement).toHaveClass('nokey', 'nopan', 'nowheel')
  })

  it('stops key events from bubbling out of the editor container', () => {
    const onKeyDown = vi.fn()
    const onKeyUp = vi.fn()

    render(
      <div onKeyDown={onKeyDown} onKeyUp={onKeyUp}>
        <SqlEditor value="SELECT 1" onChange={() => {}} upstreamTables={[]} />
      </div>
    )

    const editorInput = screen.getByLabelText('sql-editor')

    fireEvent.keyDown(editorInput, { key: ' ' })
    fireEvent.keyUp(editorInput, { key: ' ' })

    expect(onKeyDown).not.toHaveBeenCalled()
    expect(onKeyUp).not.toHaveBeenCalled()
  })
})
