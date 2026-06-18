import { StatusBadge } from '@shori/design-system'

export function AllStatuses() {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12, padding: 16 }}>
      <StatusBadge result={{ status: 'idle' }} />
      <StatusBadge result={{ status: 'connecting' }} />
      <StatusBadge result={{ status: 'running', elapsedLabel: '3s' }} />
      <StatusBadge result={{ status: 'success', executionTimeMs: 1250 }} />
      <StatusBadge result={{ status: 'success', cached: true, executionTimeMs: 1250 }} />
      <StatusBadge result={{ status: 'error' }} />
      <StatusBadge result={{ status: 'cancelled' }} />
    </div>
  )
}

export function WithMeta() {
  return (
    <div style={{ padding: 16 }}>
      <StatusBadge
        showMeta
        result={{
          status: 'success',
          executionTimeMs: 850,
          rowCount: 12500,
          columnCount: 8,
        }}
      />
    </div>
  )
}
