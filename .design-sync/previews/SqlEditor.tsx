import { SqlEditor } from '@shori/design-system'

export function WithQuery() {
  return (
    <div style={{ padding: 16, width: 500 }}>
      <SqlEditor minHeight={160}>
        <span style={{ color: '#7c3aed' }}>SELECT</span>{' '}
        <span style={{ color: '#374151' }}>region,</span>{' '}
        <span style={{ color: '#7c3aed' }}>SUM</span>
        <span style={{ color: '#374151' }}>(revenue)</span>{' '}
        <span style={{ color: '#7c3aed' }}>AS</span>{' '}
        <span style={{ color: '#374151' }}>total_revenue,</span>
        <br />
        {'  '}
        <span style={{ color: '#7c3aed' }}>COUNT</span>
        <span style={{ color: '#374151' }}>(*)</span>{' '}
        <span style={{ color: '#7c3aed' }}>AS</span>{' '}
        <span style={{ color: '#374151' }}>order_count</span>
        <br />
        <span style={{ color: '#7c3aed' }}>FROM</span>{' '}
        <span style={{ color: '#374151' }}>sales_data</span>
        <br />
        <span style={{ color: '#7c3aed' }}>WHERE</span>{' '}
        <span style={{ color: '#374151' }}>period = </span>
        <span style={{ color: '#059669' }}>'2024-Q4'</span>
        <br />
        <span style={{ color: '#7c3aed' }}>GROUP BY</span>{' '}
        <span style={{ color: '#374151' }}>region</span>
        <br />
        <span style={{ color: '#7c3aed' }}>ORDER BY</span>{' '}
        <span style={{ color: '#374151' }}>total_revenue</span>{' '}
        <span style={{ color: '#7c3aed' }}>DESC</span>
      </SqlEditor>
    </div>
  )
}

export function Placeholder() {
  return (
    <div style={{ padding: 16, width: 400 }}>
      <SqlEditor minHeight={120}>
        <span style={{ color: '#9ca3af', fontStyle: 'italic' }}>-- Write your SQL query here&#10;SELECT * FROM …</span>
      </SqlEditor>
    </div>
  )
}
