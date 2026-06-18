# Shori Design System — conventions for building

A React design system for data-pipeline / node-graph UIs (sources, transforms, SQL, exports). Import components from `@shori/design-system` (rendered here from `window.ShoriDS`).

## Setup — one stylesheet, no provider

There is **no theme provider or root wrapper**. The only setup is loading the design system's stylesheet: `styles.css`, which `@import`s `_ds_bundle.css` where the `--ds-*` design tokens are defined on `:root`. Render any component directly — no `<ThemeProvider>`, no context. If components come up unstyled (browser-default fonts, no color), the stylesheet isn't loaded; nothing else is needed.

## Styling idiom — props for components, `--ds-*` tokens for your own layout

Two distinct layers, don't mix them up:

- **Components are styled through props, not classes.** Each component owns its internal BEM CSS (`ds-btn--primary`, `ds-node-card--csv`, …) — those are implementation detail, never author them by hand. You drive appearance with the documented props: e.g. `Button` takes `variant` (`primary | secondary | ghost | danger`), `size` (`sm | md | lg`), `fullWidth`; `StatusBadge` takes `result` + `showMeta`. Read each component's `.d.ts` for its prop contract before using it.
- **Your own surrounding layout uses the `--ds-*` CSS variables** so glue matches the system. Never hardcode hex/px — reference tokens:

| Family | Tokens (real names) |
|---|---|
| Color | `--ds-color-primary` `--ds-color-primary-hover` `--ds-color-on-primary` `--ds-color-text` `--ds-color-text-muted` `--ds-color-text-subtle` `--ds-color-bg` `--ds-color-surface` `--ds-color-surface-muted` `--ds-color-border` `--ds-color-border-strong` `--ds-color-danger` `--ds-color-danger-hover` |
| Space | `--ds-space-1` … `--ds-space-6`, `--ds-space-8` |
| Radius | `--ds-radius-sm` `--ds-radius-md` `--ds-radius-lg` `--ds-radius-xl` `--ds-radius-pill` |
| Shadow | `--ds-shadow-sm` `--ds-shadow-md` |
| Font | `--ds-font-sans` `--ds-font-mono` `--ds-font-serif` |
| Status (node run state) | `--ds-status-{idle,connecting,running,success,cached,error,cancelled}-{bg,fg}` |
| Node / chip accents (by source kind) | `--ds-accent-{csv,excel,export,oracle,postgres,transform}[-text]`, `--ds-chip-{csv,excel,db,transform,export}-{bg,border,text}` |

Most components also accept `className` and `style` for positioning glue (see each `.d.ts`).

## Where the truth lives

- Tokens + all component CSS: **`styles.css`** and the **`_ds_bundle.css`** it imports — read these before styling.
- Per-component API + usage: **`components/general/<Name>/<Name>.d.ts`** (props contract) and **`<Name>.prompt.md`** (usage).

## Components

`Button`, `Switch`, `StatusBadge`, `NodeCard` (source/transform/export node tiles, accent by kind), `DockPanel` (canvas + dockable side/bottom panels), `Modal`, `Toolbar` / `ToolbarChip` / `ToolbarSeparator` (the node palette), `DataGrid` (typed columns, null/empty/loading states), `SqlEditor` (syntax-highlighted SQL).

## Idiomatic snippet

```tsx
import { Modal, Button } from '@shori/design-system';

// Component appearance via props (tone, variant); your own glue via --ds-* tokens.
<Modal
  open
  onClose={() => {}}
  tone="danger"
  title="Delete project"
  description="This action cannot be undone."
  footer={
    <div style={{ display: 'flex', gap: 'var(--ds-space-3)', justifyContent: 'flex-end' }}>
      <Button variant="secondary">Cancel</Button>
      <Button variant="danger">Delete</Button>
    </div>
  }
/>
```
