export type SwitchSize = 'sm' | 'md'

export interface SwitchProps {
  checked: boolean
  onChange: (checked: boolean) => void
  size?: SwitchSize
  disabled?: boolean
  /** Accessible name; also used as the `aria-label` when no visible label exists. */
  label?: string
  id?: string
}

/**
 * One switch component, one size scale. Replaces the three hand-rolled toggles
 * (edit-mode, CSV preprocessing, Excel preprocessing) that each shipped a
 * different size and colour in the audit (F4). The "on" colour is a token, so
 * intent is set by theme, not per-call-site Tailwind.
 */
export function Switch({ checked, onChange, size = 'md', disabled = false, label, id }: SwitchProps) {
  const classes = [
    'ds-switch',
    `ds-switch--${size}`,
    checked ? 'is-on' : '',
    disabled ? 'is-disabled' : '',
  ]
    .filter(Boolean)
    .join(' ')

  return (
    <button
      type="button"
      role="switch"
      id={id}
      aria-checked={checked}
      aria-label={label}
      disabled={disabled}
      className={classes}
      onClick={() => {
        if (!disabled) onChange(!checked)
      }}
    >
      <span className="ds-switch__thumb" />
    </button>
  )
}
