# design-sync NOTES — @shori/design-system

## First-sync context (2026-06-15)

- **Package**: `packages/design-system/` in a npm monorepo (no workspace hoisting — DS has its own `node_modules/`)
- **Build command**: `cd packages/design-system && npm run build` (tsup → dist/index.js + styles.css copied)
- **Entry**: `./packages/design-system/dist/index.js`; node_modules for converter: `./packages/design-system/node_modules`
- **CSS**: `dist/styles.css` — already `@import`s `_ds_bundle.css`; no additional token pkg needed
- **Shape**: package (no Storybook)
- **11 components**: Button, Switch, StatusBadge, NodeCard, DockPanel, Modal, Toolbar, ToolbarChip, ToolbarSeparator, DataGrid, SqlEditor
- **Global name**: `ShoriDS` (window.ShoriDS)

## Solved issues

- **`[FONT_MISSING]` Cambria**: `--ds-font-serif` uses `ui-serif, Georgia, Cambria, …` — Cambria is third in fallback, not primary. Suppressed via `cfg.runtimeFontPrefixes: ["Cambria"]`. No woff2 needed.
- **`[RENDER_BLANK]` DockPanel**: `ds-dock--right/left` sets `height: 100%` — needs fixed-height parent. Preview wraps it in a div with explicit height. Three cells (RightPanel, Collapsed, BottomPanel) all grade good.
- **`[RENDER_BLANK]` ToolbarSeparator**: Renders as 1px divider — blank in isolation. Authored as `InToolbar` cell (full Toolbar context), which renders clearly.
- **Modal portal**: Uses `createPortal(…, document.body)` — renders fine in the preview iframe's body. `cfg.overrides.Modal: {cardMode:"single", viewport:"700x500"}` applied.

## Preview status

All 11 components authored in `.design-sync/previews/`, 27 cells graded `good`. Grades stored in `.design-sync/.cache/review/<Name>.grade.json` (gitignored — carry forward via uploaded `_ds_sync.json`).

## Upload status (2026-06-16)

- **Uploaded** to Claude Design project `1d1d05ce-06fb-4e3e-9b19-933f61662b19` ("Shori Design System") — https://claude.ai/design/p/1d1d05ce-06fb-4e3e-9b19-933f61662b19
- 11 components, 29 cells all graded `good`, render check clean. `projectId` pinned in config.json.
- Conventions header authored at `.design-sync/conventions.md`, wired via `cfg.readmeHeader`.

## Auth note (RESOLVED)

First upload (prior session) was blocked because it used `CLAUDE_CODE_OAUTH_TOKEN` env var which can't get design scopes. Resolved by running from a session with disk credentials (no env token) after `/login` — `list_projects` then auto-upgraded the login with `user:design:read/write`. **If upload fails with a scope error again: ensure `CLAUDE_CODE_OAUTH_TOKEN` is NOT set and that `/login` was done in an interactive terminal.**

## Re-sync checklist

1. `cp -r <skill-base>/package-build.mjs <skill-base>/package-validate.mjs <skill-base>/package-capture.mjs <skill-base>/lib <skill-base>/storybook .ds-sync/` (re-stage — takes seconds)
2. `cd .ds-sync && npm i esbuild ts-morph @types/react` (only if `.ds-sync/node_modules` missing)
3. Install playwright if `~/.cache/ms-playwright/` missing: `cd .ds-sync && npm i playwright && node node_modules/.bin/playwright install chromium`
4. Rebuild DS: `cd packages/design-system && npm run build`
5. Run converter: `node .ds-sync/package-build.mjs --config design-sync.config.json --node-modules ./packages/design-system/node_modules --entry ./packages/design-system/dist/index.js --out ./ds-bundle`
6. Fetch remote anchor: `DesignSync(get_file, path:"_ds_sync.json")` → save to `.design-sync/.cache/remote-sync.json`
7. Run diff: `node .ds-sync/lib/remote-diff.mjs --local ./ds-bundle --remote .design-sync/.cache/remote-sync.json`
8. Validate + capture only the `changed`/`added` partition
9. Upload per §5 (full writes, `deletePaths` from diff)

## Grid-overflow card modes (applied 2026-06-16)

Button, SqlEditor, Toolbar tripped `[GRID_OVERFLOW]` (`wide`) — their stories render wider than a grid cell and crop in the product pane. Fixed with `cfg.overrides.<Name>: {"cardMode": "column"}` (full card width, one story per row). Modal stays `{"cardMode":"single","viewport":"700x500"}`. If a future restyle narrows these, the column mode is harmless; if a new component renders wide, expect the same warn and apply the same override.

## Re-sync risks

- **DS version bump without rebuild**: `dist/` can lag `src/` if `npm run build` wasn't run before the converter. Always rebuild.
- **Authored previews tied to component API**: if a component prop is renamed (e.g. `NodeCard.subtitle` type changes from `ReactNode` to `string`), the preview `.tsx` will silently compile to a floor card. Check build log for `! preview build failed:` lines.
- **Switch label**: `label` prop is aria-only — no visible text rendered. Previews correctly show toggles only. If a future version adds visible labels, update `Switch.tsx` preview.
- **DockPanel height**: previews use fixed-height parent wrappers. If DS adds `height` prop or changes to `min-height`, revisit.
- **Playwright version vs chromium cache**: installed playwright@1.61.0 into `.ds-sync/` (chromium-1228). On macOS the browser cache is `~/Library/Caches/ms-playwright/` (NOT `~/.cache/`); `playwright install chromium` may report "already downloaded" while `~/.cache` looks empty — check the Library path. If `npm i playwright` bumps the version, re-run `playwright install chromium`.
- **`.d.ts` contract change clears ALL grades on rebuild**: on 2026-06-16 a `src/styles.css` edit + rebuild regenerated `dist/index.d.ts`; the capture step then cleared every grade ("contract changed") and forced a full re-grade of all 29 cells, even though component logic was unchanged. Expect this whenever the DS is rebuilt and tsup emits a structurally different `.d.ts` — budget for re-grading from the fresh review sheets. Grades are carried forward only when the contract is byte-stable.
