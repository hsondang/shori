import Editor from '@monaco-editor/react'

interface SqlEditorProps {
  value: string
  onChange: (value: string) => void
  upstreamTables: string[]
}

export default function SqlEditor({ value, onChange }: SqlEditorProps) {
  return (
    <div className="border border-gray-300 rounded overflow-hidden">
      <Editor
        height="200px"
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
        }}
        theme="vs-dark"
      />
    </div>
  )
}
