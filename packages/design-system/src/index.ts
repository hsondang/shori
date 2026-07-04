// @shori/design-system — public surface.
// Consumers also import the stylesheet once: `import '@shori/design-system/styles.css'`.

export { tokens } from './tokens/tokens'
export type { Tokens } from './tokens/tokens'

export { Button } from './components/Button'
export type { ButtonProps, ButtonVariant, ButtonSize } from './components/Button'

export { Switch } from './components/Switch'
export type { SwitchProps, SwitchSize } from './components/Switch'

export { StatusBadge, statusPresentation, formatExecutionMs } from './components/StatusBadge'
export type {
  StatusBadgeProps,
  NodeStatus,
  NodeResultLike,
  StatusTone,
  StatusPresentation,
} from './components/StatusBadge'

export { NodeCard, NODE_CARD_DISMISS_EVENT } from './components/NodeCard'
export type { NodeCardProps, NodeKind, DbAccent, NodeAction } from './components/NodeCard'

export { DataStateDots } from './components/DataStateDots'
export type { DataStateDotsProps, LocationDot, PythonDot } from './components/DataStateDots'

export { NodeStateTable } from './components/NodeStateTable'
export type { NodeStateTableProps, NodeStateRow, NodeStateKind } from './components/NodeStateTable'

export { LoadDestinationDialog } from './components/LoadDestinationDialog'
export type {
  LoadDestinationDialogProps,
  LoadDestinationCandidate,
  LoadDestination,
} from './components/LoadDestinationDialog'

export { DockPanel } from './components/DockPanel'
export type { DockPanelProps, DockSide } from './components/DockPanel'

export { Modal } from './components/Modal'
export type { ModalProps, ModalSize, ModalTone } from './components/Modal'

export { Toolbar, ToolbarChip, ToolbarSeparator } from './components/Toolbar'
export type { ToolbarProps, ToolbarChipProps, ToolbarChipAccent } from './components/Toolbar'

export { DataGrid } from './components/DataGrid'
export type { DataGridProps, DataGridColumn } from './components/DataGrid'

export { SqlEditor } from './components/SqlEditor'
export type { SqlEditorProps } from './components/SqlEditor'
