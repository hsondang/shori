import { useState } from 'react'
import { testDbConnection } from '../../api/client'

interface ConnectionFormProps {
  config: Record<string, unknown>
  onChange: (config: Record<string, unknown>) => void
  dbType: 'oracle' | 'postgres'
}

const dbDefaults = {
  oracle: { port: 1521, dbFieldName: 'service_name', dbFieldLabel: 'Service Name', color: 'bg-orange-100 text-orange-700 hover:bg-orange-200' },
  postgres: { port: 5432, dbFieldName: 'database', dbFieldLabel: 'Database', color: 'bg-teal-100 text-teal-700 hover:bg-teal-200' },
}

export default function ConnectionForm({ config, onChange, dbType }: ConnectionFormProps) {
  const [testing, setTesting] = useState(false)
  const [testResult, setTestResult] = useState<{ success: boolean; error?: string } | null>(null)
  const defaults = dbDefaults[dbType]

  const update = (field: string, value: unknown) => {
    onChange({ ...config, [field]: value })
  }

  const handleTest = async () => {
    setTesting(true)
    setTestResult(null)
    try {
      const result = await testDbConnection(dbType, config)
      setTestResult(result)
    } catch {
      setTestResult({ success: false, error: 'Connection failed' })
    }
    setTesting(false)
  }

  return (
    <div className="space-y-2">
      <label className="block text-xs text-gray-500">Connection</label>
      <input
        type="text"
        placeholder="Host"
        value={(config.host as string) || ''}
        onChange={(e) => update('host', e.target.value)}
        className="w-full border border-gray-300 rounded px-2 py-1 text-sm"
      />
      <input
        type="number"
        placeholder="Port"
        value={(config.port as number) || defaults.port}
        onChange={(e) => update('port', parseInt(e.target.value) || defaults.port)}
        className="w-full border border-gray-300 rounded px-2 py-1 text-sm"
      />
      <input
        type="text"
        placeholder={defaults.dbFieldLabel}
        value={(config[defaults.dbFieldName] as string) || ''}
        onChange={(e) => update(defaults.dbFieldName, e.target.value)}
        className="w-full border border-gray-300 rounded px-2 py-1 text-sm"
      />
      <input
        type="text"
        placeholder="Username"
        value={(config.user as string) || ''}
        onChange={(e) => update('user', e.target.value)}
        className="w-full border border-gray-300 rounded px-2 py-1 text-sm"
      />
      <input
        type="password"
        placeholder="Password"
        value={(config.password as string) || ''}
        onChange={(e) => update('password', e.target.value)}
        className="w-full border border-gray-300 rounded px-2 py-1 text-sm"
      />
      <button
        onClick={handleTest}
        disabled={testing}
        className={`w-full py-1 rounded text-sm disabled:opacity-50 ${defaults.color}`}
      >
        {testing ? 'Testing...' : 'Test Connection'}
      </button>
      {testResult && (
        <div className={`text-xs ${testResult.success ? 'text-green-600' : 'text-red-600'}`}>
          {testResult.success ? 'Connected successfully' : testResult.error}
        </div>
      )}
    </div>
  )
}
