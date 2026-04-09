import Editor from '@monaco-editor/react'
import type { KeyboardEvent as ReactKeyboardEvent } from 'react'

interface SqlEditorProps {
  value: string
  onChange: (value: string) => void
  upstreamTables: string[]
  height?: string
  containerClassName?: string
}

export default function SqlEditor({
  value,
  onChange,
  height = '200px',
  containerClassName = '',
}: SqlEditorProps) {
  const stopKeyboardPropagation = (event: ReactKeyboardEvent<HTMLDivElement>) => {
    event.stopPropagation()
  }

  return (
    <div
      className={`nokey nopan nowheel overflow-hidden rounded border border-gray-300 ${containerClassName}`.trim()}
      onKeyDown={stopKeyboardPropagation}
      onKeyUp={stopKeyboardPropagation}
    >
      <Editor
        height={height}
        defaultLanguage="sql"
        value={value}
        onChange={(v) => onChange(v || '')}
        options={{
          minimap: { enabled: false },
          fontSize: 12,
          lineNumbers: 'on',
          scrollBeyondLastLine: false,
          wordWrap: 'on',
          tabSize: 2,
          automaticLayout: true,
          quickSuggestions: false,
          suggestOnTriggerCharacters: false,
          parameterHints: { enabled: false },
          inlineSuggest: { enabled: false },
          wordBasedSuggestions: 'off',
          tabCompletion: 'off',
          acceptSuggestionOnEnter: 'off',
        }}
        theme="vs-dark"
      />
    </div>
  )
}
