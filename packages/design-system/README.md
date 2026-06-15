# @shori/design-system

The on-brand source of truth for Shori's UI — design **tokens** and **React components**.
It is the durable code home for the UI overhaul described in [`docs/design-audit.md`](../../docs/design-audit.md),
and the package that [`/design-sync`](https://claude.ai/design) converts so the Claude Design agent
builds with Shori's *actual* components.

## Why this exists

The app grew feature-first and accumulated systemic inconsistencies (see the audit): duplicated
action surfaces, divergent status vocabulary, three hand-rolled toggles, and a real Preview-vs-Execute
state desync. This package collapses those into one tokenized, tested set of primitives.

## Usage

```ts
import { Button, Switch, StatusBadge, NodeCard, statusPresentation } from '@shori/design-system'
import '@shori/design-system/styles.css' // once, at the app root
```

The single most important export is `statusPresentation(result)` — the **one** function that maps a
node result to `{ tone, label, dotAnimated, isBusy }`. Every surface that shows status or gates a
control on "is this node busy?" must read from it, so the badge, the panel header, and the
Execute/Run buttons can never disagree again (audit F1, F5).

## Components (v0.2)

| Component | Replaces | Audit finding |
|---|---|---|
| `Button` (primary/secondary/ghost/danger × sm/md/lg) | filled-pill vs. underline-link split | F2 |
| `Switch` (sm/md) | three differently-sized, differently-coloured toggles | F4 |
| `StatusBadge` + `statusPresentation` | status label logic duplicated in 3 places | F1, F5 |
| `NodeCard` (5 kinds × all states) | inline accent maps, per-card status logic | F6 |
| `DockPanel` (right/left/bottom/top, collapsible + resizable) | the un-hideable right config panel | F3 |
| `Modal` (sm/md/lg, default/danger) | ad-hoc node-editor / error / settings dialogs | — |

## Build

```bash
npm install
npm run build      # tsup → dist/index.js (+ .d.ts), then copies styles.css → dist/styles.css
npm run typecheck
```

Styling is plain CSS driven by `--ds-*` custom properties — no Tailwind/preprocessor dependency — so
the package is portable and the whole theme is reachable from `styles.css` (the form `/design-sync`
expects).

## Roadmap (next increments)

`Toolbar` · `DataGrid` · `SqlEditor` frame · then port the app's components onto these against the
unchanged store/API (starting with the Database Source node + config panel, where the F1 desync lives).

## Adding a component (the established recipe)

Follow any existing component (e.g. `Switch`) as the template:

1. `src/components/<Name>/<Name>.tsx` — typed props, classNames only (`ds-<name>…`), no inline colors.
2. `src/components/<Name>/index.ts` — re-export the component + its types.
3. Append the component's styles to `src/styles.css` under a new `=== <Name> ===` banner; use `--ds-*`
   tokens for every value (add a token to `:root` rather than hard-coding a new color/size).
4. Add the export to `src/index.ts` (the public barrel) and mirror any token additions in `src/tokens/tokens.ts`.
5. Add a `<Section>` to `gallery/main.tsx` rendering the component across its states.
6. Verify: `npm run typecheck && npm run build`, then `npm run gallery` (port 5180) and eyeball it.

Keep it framework-agnostic CSS (no Tailwind), controlled props where the app already owns the state,
and one semantic token per visual decision.
