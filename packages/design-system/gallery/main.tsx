import { StrictMode, useState, type ReactNode } from 'react'
import { createRoot } from 'react-dom/client'
import {
  Button,
  Switch,
  StatusBadge,
  NodeCard,
  DataStateDots,
  NodeStateTable,
  LoadDestinationDialog,
  DockPanel,
  Modal,
  Toolbar,
  ToolbarChip,
  ToolbarSeparator,
  DataGrid,
  SqlEditor,
  type ButtonVariant,
  type ButtonSize,
  type NodeResultLike,
  type ToolbarChipAccent,
  type NodeStateRow,
  type LoadDestinationCandidate,
  type LoadDestination,
} from '../src'
import '../src/styles.css'

const RESULTS: Record<string, NodeResultLike> = {
  idle: { status: 'idle' },
  connecting: { status: 'connecting' },
  running: { status: 'running', elapsedLabel: '3s' },
  success: { status: 'success', rowCount: 1240, columnCount: 8, executionTimeMs: 190 },
  cached: { status: 'success', cached: true, rowCount: 1240, columnCount: 8 },
  error: { status: 'error' },
  cancelled: { status: 'cancelled' },
}

const NODE_ACTIONS = [
  { label: 'Preview' },
  { label: 'Materialize' },
  { label: 'View table', tone: 'muted' as const },
]

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="section">
      <div className="section__head">{title}</div>
      {children}
    </section>
  )
}

function Swatch({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="swatch">
      {children}
      <span className="swatch__label">{label}</span>
    </div>
  )
}

function InteractiveSwitch({ size }: { size: 'sm' | 'md' }) {
  const [on, setOn] = useState(false)
  return <Switch checked={on} onChange={setOn} size={size} label={`${size} switch`} />
}

const BUTTON_VARIANTS: ButtonVariant[] = ['primary', 'secondary', 'ghost', 'danger']
const BUTTON_SIZES: ButtonSize[] = ['sm', 'md', 'lg']

function DockDemo() {
  const [rightWidth, setRightWidth] = useState(280)
  const [rightCollapsed, setRightCollapsed] = useState(false)
  const [bottomHeight, setBottomHeight] = useState(120)
  const [bottomCollapsed, setBottomCollapsed] = useState(false)

  return (
    <div
      style={{
        height: 360,
        display: 'flex',
        flexDirection: 'column',
        border: '1px solid var(--ds-color-border)',
        borderRadius: 12,
        overflow: 'hidden',
        background: '#fff',
      }}
    >
      <div style={{ flex: 1, display: 'flex', minHeight: 0 }}>
        <div
          style={{
            flex: 1,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            color: 'var(--ds-color-text-subtle)',
            fontSize: 13,
          }}
        >
          Canvas area
        </div>
        <DockPanel
          side="right"
          size={rightWidth}
          onResize={setRightWidth}
          minSize={200}
          maxSize={420}
          collapsed={rightCollapsed}
          onToggleCollapsed={() => setRightCollapsed((c) => !c)}
          collapsedSize={44}
          title="Configure"
        >
          <div style={{ padding: 16, fontSize: 13, color: 'var(--ds-color-text-muted)' }}>
            Right config panel — drag the left edge to resize, Collapse to hide. (Fixes F3.)
          </div>
        </DockPanel>
      </div>
      <DockPanel
        side="bottom"
        size={bottomHeight}
        onResize={setBottomHeight}
        minSize={80}
        maxSize={240}
        collapsed={bottomCollapsed}
        onToggleCollapsed={() => setBottomCollapsed((c) => !c)}
        collapsedSize={40}
        title="Data Preview"
      >
        <div style={{ padding: 16, fontSize: 13, color: 'var(--ds-color-text-muted)' }}>
          Bottom preview panel — same dock pattern, collapsible and resizable.
        </div>
      </DockPanel>
    </div>
  )
}

function ModalDemo() {
  const [open, setOpen] = useState(false)
  const [errorOpen, setErrorOpen] = useState(false)
  return (
    <div className="row">
      <Button onClick={() => setOpen(true)}>Open modal</Button>
      <Button variant="danger" onClick={() => setErrorOpen(true)}>Open error dialog</Button>

      <Modal
        open={open}
        onClose={() => setOpen(false)}
        title="Create Database Source"
        description="Review and adjust the node configuration before it is added to the canvas."
        footer={
          <>
            <Button variant="ghost" onClick={() => setOpen(false)}>Cancel</Button>
            <Button onClick={() => setOpen(false)}>Create</Button>
          </>
        }
      >
        <div style={{ fontSize: 14, color: 'var(--ds-color-text-muted)' }}>
          Modal body content — form fields, editors, anything. Esc and backdrop both dismiss.
        </div>
      </Modal>

      <Modal
        open={errorOpen}
        onClose={() => setErrorOpen(false)}
        tone="danger"
        size="sm"
        title="Execution error"
        footer={<Button variant="danger" onClick={() => setErrorOpen(false)}>Dismiss</Button>}
      >
        <div style={{ fontSize: 14, color: 'var(--ds-color-text)' }}>
          Preprocessing is enabled for this CSV source. Click Preprocess and review the output before loading data.
        </div>
      </Modal>
    </div>
  )
}

const NODE_STATE_ROWS: NodeStateRow[] = [
  {
    id: 'n1', kind: 'csv', name: 'Sales Export', result: RESULTS.success,
    python: undefined, memory: 'empty', disk: 'fresh',
    schema: 'main', table: 'sales_data', rowCount: 15420,
    createdAtLabel: 'Oct 2, 2024', updatedAtLabel: '3d ago', lastRunLabel: '3d',
  },
  {
    id: 'n2', kind: 'excel', name: 'Regional Targets', result: RESULTS.idle,
    python: undefined, memory: 'empty', disk: 'empty',
    schema: null, table: 'targets', rowCount: null,
    createdAtLabel: 'Oct 7, 2024', updatedAtLabel: '—', lastRunLabel: 'Never',
  },
  {
    id: 'n3', kind: 'db', name: 'Production DB', result: RESULTS.cached,
    python: 'live', memory: 'empty', disk: 'empty',
    schema: null, table: 'orders', rowCount: 10000,
    createdAtLabel: 'Sep 18, 2024', updatedAtLabel: '12m ago', lastRunLabel: '12m',
  },
  {
    id: 'n4', kind: 'transform', name: 'Revenue by Region', result: RESULTS.success,
    python: 'live', memory: 'stale', disk: 'empty',
    schema: 'memory', table: 'revenue_by_region', rowCount: 5,
    createdAtLabel: 'Oct 5, 2024', updatedAtLabel: '2m ago', lastRunLabel: '18m',
  },
  {
    id: 'n5', kind: 'export', name: 'Quarterly Report', result: RESULTS.success,
    python: undefined, memory: 'empty', disk: 'empty',
    schema: null, table: null, rowCount: 5,
    createdAtLabel: 'Oct 8, 2024', updatedAtLabel: '1h ago', lastRunLabel: '1h',
  },
]

function LoadDestinationDialogDemo() {
  const [open, setOpen] = useState(false)
  const candidates: LoadDestinationCandidate[] = [
    { nodeId: 'a', label: 'Customers (Postgres)', tableName: 'node_pg_customers' },
    { nodeId: 'b', label: 'Orders (CSV)', tableName: 'node_csv_orders' },
  ]
  const [choices, setChoices] = useState<Record<string, LoadDestination>>({ a: 'in_memory', b: 'in_memory' })

  return (
    <div className="row">
      <Button onClick={() => setOpen(true)}>Open load-destination prompt</Button>
      <LoadDestinationDialog
        open={open}
        candidates={candidates}
        choices={choices}
        onChoiceChange={(nodeId, mode) => setChoices((c) => ({ ...c, [nodeId]: mode }))}
        onApplyToAll={(mode) => setChoices(Object.fromEntries(candidates.map((c) => [c.nodeId, mode])))}
        onConfirm={() => setOpen(false)}
        onCancel={() => setOpen(false)}
      />
    </div>
  )
}

const CHIP_ACCENTS: { accent: ToolbarChipAccent; label: string }[] = [
  { accent: 'csv', label: 'CSV Source' },
  { accent: 'excel', label: 'Excel Source' },
  { accent: 'transform', label: 'Transform' },
  { accent: 'export', label: 'Export' },
  { accent: 'db', label: 'Database Source' },
]

function ToolbarDemo() {
  const [saving, setSaving] = useState(false)
  const handleSave = () => {
    setSaving(true)
    setTimeout(() => setSaving(false), 800)
  }
  return (
    <Toolbar>
      <div style={{ marginRight: 4 }}>
        <div style={{ fontSize: 10, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.2em', color: 'var(--ds-color-text-subtle)' }}>Project</div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <input
            type="text"
            defaultValue="My Pipeline"
            style={{ border: '1px solid var(--ds-color-border-strong)', borderRadius: 'var(--ds-radius-sm)', padding: '2px 8px', fontSize: 13, width: 160 }}
          />
          <span style={{ fontSize: 12, color: 'var(--ds-status-success-fg)', fontWeight: 500 }}>Saved</span>
        </div>
      </div>
      <ToolbarSeparator />
      {CHIP_ACCENTS.map(({ accent, label }) => (
        <ToolbarChip key={accent} accent={accent} label={label} draggable />
      ))}
      <ToolbarSeparator />
      <Button size="sm" variant="secondary" onClick={handleSave}>
        {saving ? 'Saving…' : 'Save'}
      </Button>
      <Button size="sm" variant="secondary">⚙︎</Button>
      <ToolbarSeparator />
      <Button size="sm" variant="primary">Execute</Button>
      <Button size="sm" variant="secondary">Force Refresh</Button>
    </Toolbar>
  )
}

function Gallery() {
  return (
    <div className="gallery">
      <h1 className="gallery__title">@shori/design-system</h1>
      <p className="gallery__sub">v0.3 — tokens · Button · Switch · StatusBadge · NodeCard · DockPanel · Modal · Toolbar · DataGrid · SqlEditor. Rendered from the live source.</p>

      <Section title="Button — variants × sizes">
        {BUTTON_SIZES.map((size) => (
          <div className="row" key={size}>
            {BUTTON_VARIANTS.map((variant) => (
              <Swatch key={variant} label={`${variant} / ${size}`}>
                <Button variant={variant} size={size}>
                  {variant[0].toUpperCase() + variant.slice(1)}
                </Button>
              </Swatch>
            ))}
            <Swatch label={`disabled / ${size}`}>
              <Button variant="primary" size={size} disabled>
                Disabled
              </Button>
            </Swatch>
          </div>
        ))}
        <div className="row">
          <div style={{ width: 280 }}>
            <Button variant="primary" fullWidth>
              Execute
            </Button>
          </div>
          <div style={{ width: 280 }}>
            <Button variant="danger" fullWidth>
              Abort
            </Button>
          </div>
        </div>
      </Section>

      <Section title="Switch — sizes × states">
        <div className="row">
          <Swatch label="md (interactive)"><InteractiveSwitch size="md" /></Swatch>
          <Swatch label="md on"><Switch checked onChange={() => {}} label="on" /></Swatch>
          <Swatch label="sm (interactive)"><InteractiveSwitch size="sm" /></Swatch>
          <Swatch label="disabled off"><Switch checked={false} onChange={() => {}} disabled label="disabled" /></Swatch>
          <Swatch label="disabled on"><Switch checked onChange={() => {}} disabled label="disabled on" /></Swatch>
        </div>
      </Section>

      <Section title="StatusBadge — one model, every state">
        <div className="panel">
          <div className="row">
            {Object.entries(RESULTS).map(([key, result]) => (
              <Swatch key={key} label={key}>
                <StatusBadge result={result} />
              </Swatch>
            ))}
          </div>
        </div>
      </Section>

      <Section title="StatusBadge — run modes (Loading / Materializing / Live preview)">
        <div className="panel">
          <div className="row">
            <Swatch label="load"><StatusBadge result={{ status: 'running', mode: 'load', elapsedLabel: '2s' }} /></Swatch>
            <Swatch label="materialize"><StatusBadge result={{ status: 'running', mode: 'materialize', elapsedLabel: '2s' }} /></Swatch>
            <Swatch label="preview"><StatusBadge result={{ status: 'running', mode: 'preview' }} /></Swatch>
            <Swatch label="running (no mode)"><StatusBadge result={{ status: 'running', elapsedLabel: '2s' }} /></Swatch>
          </div>
        </div>
      </Section>

      <Section title="DataStateDots — live · in-memory · materialized (spec §7)">
        <div className="panel">
          <div className="row">
            <Swatch label="new (DB, all empty)"><DataStateDots python="empty" memory="empty" disk="empty" /></Swatch>
            <Swatch label="live preview"><DataStateDots python="live" memory="empty" disk="empty" /></Swatch>
            <Swatch label="in memory"><DataStateDots python="empty" memory="fresh" disk="empty" /></Swatch>
            <Swatch label="materialized"><DataStateDots python="empty" memory="empty" disk="fresh" /></Swatch>
            <Swatch label="both fresh + live"><DataStateDots python="live" memory="fresh" disk="fresh" /></Swatch>
            <Swatch label="mem stale, disk fresh"><DataStateDots python="empty" memory="stale" disk="fresh" /></Swatch>
            <Swatch label="both stale"><DataStateDots python="empty" memory="stale" disk="stale" /></Swatch>
            <Swatch label="loading → mem"><DataStateDots python="empty" memory="loading" disk="empty" /></Swatch>
            <Swatch label="no preview (CSV/Excel)"><DataStateDots memory="fresh" disk="empty" /></Swatch>
          </div>
          <div className="row" style={{ marginTop: 12 }}>
            <DataStateDots python="live" memory="fresh" disk="stale" showLabels />
          </div>
        </div>
      </Section>

      <Section title="NodeStateTable — node-state overview (docs/node-state-model.md)">
        <NodeStateTable rows={NODE_STATE_ROWS} />
      </Section>

      <Section title="LoadDestinationDialog — batched load/materialize prompt (spec §6)">
        <div className="panel">
          <LoadDestinationDialogDemo />
        </div>
      </Section>

      <Section title="NodeCard — five kinds">
        <div className="card-grid">
          <NodeCard kind="csv" title="CSV Source" tableName="new_list" subtitle="test.csv" icon="📄" actions={[{ label: 'Preview data' }]} result={RESULTS.success} />
          <NodeCard kind="excel" title="Excel Source" tableName="orders" subtitle="orders.xlsx" icon="📊" actions={[{ label: 'Preview data' }]} />
          <NodeCard kind="db" accent="postgres" title="Database Source" tableName="node_pg" subtitle="localhost:5432" icon="🐘" actions={NODE_ACTIONS} result={RESULTS.success} />
          <NodeCard kind="db" accent="oracle" title="Database Source" tableName="node_ora" subtitle="db.internal:1521" icon="🗄️" actions={NODE_ACTIONS} />
          <NodeCard kind="transform" title="Update Amount" tableName="node_tx" subtitle={<span>SELECT full_name, …</span>} icon="🔧" actions={[{ label: 'Run and Preview' }]} result={RESULTS.cancelled} />
          <NodeCard kind="export" title="Export" tableName="out.csv" icon="📤" />
        </div>
      </Section>

      <Section title="NodeCard — full status range (db · postgres)">
        <div className="card-grid">
          {(['idle', 'connecting', 'running', 'success', 'cached', 'error'] as const).map((key) => (
            <NodeCard
              key={key}
              kind="db"
              accent="postgres"
              title="Database Source"
              tableName="customers"
              subtitle="localhost:5432"
              icon="🐘"
              actions={NODE_ACTIONS}
              result={RESULTS[key]}
              selected={key === 'success'}
              onViewError={key === 'error' ? () => {} : undefined}
            />
          ))}
        </div>
      </Section>

      <Section title="DockPanel — collapsible & resizable (fixes F3)">
        <DockDemo />
      </Section>

      <Section title="Modal — node editor / error dialog base">
        <div className="panel">
          <ModalDemo />
        </div>
      </Section>

      <Section title="Toolbar — node chips + action buttons">
        <ToolbarDemo />
      </Section>

      <Section title="DataGrid — tabular data with types and null">
        <div style={{ height: 240, display: 'flex', flexDirection: 'column', border: '1px solid var(--ds-color-border)', borderRadius: 8, overflow: 'hidden' }}>
          <DataGrid
            columns={[
              { name: 'id', type: 'int' },
              { name: 'full_name', type: 'varchar' },
              { name: 'email', type: 'varchar' },
              { name: 'amount', type: 'numeric' },
              { name: 'notes', type: 'text' },
            ]}
            rows={[
              [1, 'Alice Lê', 'alice@example.com', 1240.5, null],
              [2, 'Bob Kim', 'bob@example.com', 88.0, 'Legacy account'],
              [3, 'Carol Wu', null, 530.25, null],
              [4, 'David Ng', 'david@example.com', null, 'Pending review'],
            ]}
          />
        </div>
        <div style={{ marginTop: 12 }}>
          <DataGrid columns={[]} rows={[]} emptyMessage="No preview available — run a node first" />
        </div>
      </Section>

      <Section title="SqlEditor frame — Monaco host (placeholder content shown)">
        <SqlEditor minHeight={160}>
          <div style={{
            flex: 1,
            padding: 'var(--ds-space-3)',
            fontFamily: 'var(--ds-font-mono)',
            fontSize: 13,
            color: 'var(--ds-color-text-muted)',
            whiteSpace: 'pre',
            lineHeight: 1.6,
          }}>
            {`SELECT\n  c.full_name,\n  SUM(o.amount) AS total\nFROM customers c\nJOIN orders o ON o.customer_id = c.id\nGROUP BY 1\nORDER BY 2 DESC`}
          </div>
        </SqlEditor>
      </Section>
    </div>
  )
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <Gallery />
  </StrictMode>,
)
