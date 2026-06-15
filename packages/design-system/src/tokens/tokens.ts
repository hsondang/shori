// Programmatic mirror of the CSS custom properties declared in `../styles.css`.
// The source of truth for token *values* is `styles.css` (`:root`); this object
// exposes the same names as `var(--ds-*)` references so TypeScript call sites
// (inline styles, chart fixtures, story props) stay in lockstep with the CSS.

export const tokens = {
  color: {
    bg: 'var(--ds-color-bg)',
    surface: 'var(--ds-color-surface)',
    surfaceMuted: 'var(--ds-color-surface-muted)',
    border: 'var(--ds-color-border)',
    borderStrong: 'var(--ds-color-border-strong)',
    text: 'var(--ds-color-text)',
    textMuted: 'var(--ds-color-text-muted)',
    textSubtle: 'var(--ds-color-text-subtle)',
    primary: 'var(--ds-color-primary)',
    primaryHover: 'var(--ds-color-primary-hover)',
    onPrimary: 'var(--ds-color-on-primary)',
    danger: 'var(--ds-color-danger)',
    dangerHover: 'var(--ds-color-danger-hover)',
  },
  accent: {
    oracle: 'var(--ds-accent-oracle)',
    postgres: 'var(--ds-accent-postgres)',
    transform: 'var(--ds-accent-transform)',
    csv: 'var(--ds-accent-csv)',
    excel: 'var(--ds-accent-excel)',
    export: 'var(--ds-accent-export)',
  },
  chip: {
    csv:       { bg: 'var(--ds-chip-csv-bg)',       border: 'var(--ds-chip-csv-border)',       text: 'var(--ds-chip-csv-text)' },
    excel:     { bg: 'var(--ds-chip-excel-bg)',     border: 'var(--ds-chip-excel-border)',     text: 'var(--ds-chip-excel-text)' },
    transform: { bg: 'var(--ds-chip-transform-bg)', border: 'var(--ds-chip-transform-border)', text: 'var(--ds-chip-transform-text)' },
    export:    { bg: 'var(--ds-chip-export-bg)',    border: 'var(--ds-chip-export-border)',    text: 'var(--ds-chip-export-text)' },
    db:        { bg: 'var(--ds-chip-db-bg)',        border: 'var(--ds-chip-db-border)',        text: 'var(--ds-chip-db-text)' },
  },
  radius: {
    sm: 'var(--ds-radius-sm)',
    md: 'var(--ds-radius-md)',
    lg: 'var(--ds-radius-lg)',
    xl: 'var(--ds-radius-xl)',
    pill: 'var(--ds-radius-pill)',
  },
  space: (step: 1 | 2 | 3 | 4 | 5 | 6 | 8): string => `var(--ds-space-${step})`,
  font: {
    sans: 'var(--ds-font-sans)',
    mono: 'var(--ds-font-mono)',
    serif: 'var(--ds-font-serif)',
  },
} as const

export type Tokens = typeof tokens
