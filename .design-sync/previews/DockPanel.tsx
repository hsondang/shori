import { DockPanel, Button } from '@shori/design-system'

export function RightPanel() {
  return (
    <div style={{ display: 'flex', height: 360, border: '1px solid #e5e7eb', borderRadius: 8, overflow: 'hidden' }}>
      <div style={{ flex: 1, background: '#f9fafb', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#9ca3af', fontSize: 14 }}>
        Canvas area
      </div>
      <DockPanel side="right" size={260} title="Node Configuration" resizable={false}>
        <div style={{ padding: 16, display: 'flex', flexDirection: 'column', gap: 12 }}>
          <div>
            <label style={{ display: 'block', fontSize: 12, fontWeight: 600, marginBottom: 4 }}>Table name</label>
            <input style={{ width: '100%', padding: '6px 8px', border: '1px solid #d1d5db', borderRadius: 4, fontSize: 13 }} defaultValue="sales_data" readOnly />
          </div>
          <div>
            <label style={{ display: 'block', fontSize: 12, fontWeight: 600, marginBottom: 4 }}>SQL</label>
            <textarea style={{ width: '100%', padding: '6px 8px', border: '1px solid #d1d5db', borderRadius: 4, fontSize: 12, fontFamily: 'monospace', resize: 'none', height: 80 }} defaultValue="SELECT * FROM sales_data WHERE region = 'West'" readOnly />
          </div>
          <Button variant="primary" fullWidth>Execute</Button>
        </div>
      </DockPanel>
    </div>
  )
}

export function Collapsed() {
  return (
    <div style={{ display: 'flex', height: 200, border: '1px solid #e5e7eb', borderRadius: 8, overflow: 'hidden' }}>
      <div style={{ flex: 1, background: '#f9fafb', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#9ca3af', fontSize: 14 }}>
        Canvas area
      </div>
      <DockPanel side="right" size={260} title="Node Configuration" collapsed={true} collapsedSize={40} resizable={false}>
        <div style={{ padding: 16 }}>Content hidden when collapsed</div>
      </DockPanel>
    </div>
  )
}

export function BottomPanel() {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: 300, border: '1px solid #e5e7eb', borderRadius: 8, overflow: 'hidden' }}>
      <div style={{ flex: 1, background: '#f9fafb', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#9ca3af', fontSize: 14 }}>
        Canvas area
      </div>
      <DockPanel side="bottom" size={140} title="Data Preview" resizable={false}>
        <div style={{ padding: '8px 16px', fontSize: 13, color: '#6b7280' }}>42,000 rows · 7 columns</div>
      </DockPanel>
    </div>
  )
}
