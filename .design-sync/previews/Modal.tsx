import { Modal, Button } from '@shori/design-system'

export function Default() {
  return (
    <Modal
      open
      onClose={() => {}}
      title="Node Configuration"
      description="Edit the settings for this transform node."
      footer={
        <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
          <Button variant="secondary" size="sm">Cancel</Button>
          <Button variant="primary" size="sm">Save changes</Button>
        </div>
      }
    >
      <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
        <div>
          <label style={{ display: 'block', fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Table name</label>
          <input style={{ width: '100%', padding: '6px 8px', border: '1px solid #d1d5db', borderRadius: 4, fontSize: 13 }} defaultValue="aggregated_sales" readOnly />
        </div>
        <div>
          <label style={{ display: 'block', fontSize: 13, fontWeight: 600, marginBottom: 4 }}>SQL</label>
          <textarea style={{ width: '100%', padding: '6px 8px', border: '1px solid #d1d5db', borderRadius: 4, fontSize: 12, fontFamily: 'monospace', resize: 'none', height: 80 }} defaultValue={`SELECT region, SUM(revenue) AS total\nFROM sales_data\nGROUP BY region`} readOnly />
        </div>
      </div>
    </Modal>
  )
}

export function DangerTone() {
  return (
    <Modal
      open
      onClose={() => {}}
      title="Delete project"
      description="This action cannot be undone. All pipeline nodes and saved data will be permanently removed."
      tone="danger"
      footer={
        <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
          <Button variant="secondary" size="sm">Cancel</Button>
          <Button variant="danger" size="sm">Delete</Button>
        </div>
      }
    />
  )
}
