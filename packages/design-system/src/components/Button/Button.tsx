import type { ButtonHTMLAttributes } from 'react'

export type ButtonVariant = 'primary' | 'secondary' | 'ghost' | 'danger'
export type ButtonSize = 'sm' | 'md' | 'lg'

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  /** Visual weight / intent. `danger` is destructive, `ghost` is borderless. */
  variant?: ButtonVariant
  size?: ButtonSize
  /** Stretch to the container width (the panel "Execute"/"Run" pattern). */
  fullWidth?: boolean
}

/**
 * The single button primitive. Replaces the divergent filled-pill (sidebar) and
 * underline text-link (node card) styles called out in the audit (F2). Node-type
 * accent colours live on `NodeCard`, not here — this stays brand/neutral.
 */
export function Button({
  variant = 'primary',
  size = 'md',
  fullWidth = false,
  type = 'button',
  className,
  ...rest
}: ButtonProps) {
  const classes = [
    'ds-btn',
    `ds-btn--${variant}`,
    `ds-btn--${size}`,
    fullWidth ? 'ds-btn--block' : '',
    className,
  ]
    .filter(Boolean)
    .join(' ')

  return <button type={type} className={classes} {...rest} />
}
