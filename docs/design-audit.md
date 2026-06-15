# Shori — UI/UX Design Audit & Redesign Brief

> Source brief for a full design-system overhaul (intended for use with Claude Design / `claude.ai/design`).
> Produced 2026-06-15 from **(a)** live capture of the running app and **(b)** review of the frontend source.

---

## 1. Purpose & how to use this document

Shori is a node-graph ETL/pipeline editor (React 19 + Vite + Tailwind v4 + Zustand + React Flow
`@xyflow` + Monaco + TanStack Table). The UI has grown feature-first and now carries systemic
inconsistencies — duplicated action surfaces, divergent status vocabulary, hand-rolled one-off
components, and at least one real state-desync bug.

This brief is the **"before"**: it inventories the current surfaces and their states, names the
systemic problems, and specifies the **target design system** to build. It is written to stand alone
— a designer (human or Claude Design) can act on it without the running app.

Three-party workflow this brief feeds:

| Role | Who | Output |
|---|---|---|
| See the running app | Claude Code + browser preview tools | Live screenshots + this audit |
| Design the system | Claude Design (`claude.ai/design`) | Tokens + component library (preview cards) |
| Port into the app | Claude Code | Re-skinned React components on the **unchanged** store/API |

**Key boundary:** Claude Design produces *visuals/markup*, not wired React. It cannot, on its own,
preserve or break app behavior — it never touches the Zustand store, the axios client, or the React
Flow wiring. Functionality preservation happens in the port step and is guaranteed by holding the
**behavior contracts** in §5 fixed, with the vitest suite as the regression net.

---

## 2. Current architecture (frontend)

- **Routing:** `react-router-dom` — Project Home (`/`), Pipeline Editor (`/projects/:id`), Platform Settings (`/settings/platform`).
- **State:** `zustand` store [`pipelineStore.ts`](../frontend/src/store/pipelineStore.ts) (pipeline graph, execution results, live previews, CSV preview artifacts) + [`settingsStore.ts`](../frontend/src/store/settingsStore.ts) (global DB connections).
- **Canvas:** React Flow with 5 custom node types — `csv_source`, `excel_source`, `db_source`, `transform`, `export`.
- **Editors:** Monaco (`@monaco-editor/react`) for SQL; TanStack Table for data preview.
- **Layout:** Editor = canvas (flex-1) + always-mounted right config panel; a bottom Data Preview panel.

---

## 3. Systemic inconsistencies (findings)

Each finding is code-grounded with `file:line` references and was confirmed visually in the live session (see §6).

### F1 — Two disjoint "node is busy" state systems (the Preview vs. Execute desync) — **HIGH**

There are **two independent notions of "this node is working"** that never synchronize:

- `nodeResults[id].status` — `connecting | running | success | error | cancelled`. Read by the sidebar
  **Execute** button ([`NodeConfigPanel.tsx:421`](../frontend/src/components/panels/NodeConfigPanel.tsx)),
  the `NodeStatusBadge`, and the node's **Materialize** action.
- `livePreviewsByNodeId[id]` — `loading | materializing`. Written by `startLivePreview`
  ([`pipelineStore.ts:1590`](../frontend/src/store/pipelineStore.ts)), read by the live preview panel.

The node's **Preview** button calls `startLivePreview`
([`DatabaseSourceNode.tsx:84`](../frontend/src/components/flow/nodes/DatabaseSourceNode.tsx)), which writes
**only** to `livePreviewsByNodeId` and never touches `nodeResults`. Result: clicking **Preview** spins
up a live preview (and can error) while the sidebar **Execute** button stays emerald and reads
"Execute" — never "Running"/"Connecting" — and the node badge shows nothing. Confirmed live: Preview
triggered a `LIVE — NOT MATERIALIZED` panel with a connection error while Execute stayed green.

**Target:** one derived "node activity" selector (combining execution + live-preview + materialize)
that every control reads. No control computes busy-ness on its own.

### F2 — Duplicated action surfaces with divergent verbs — **HIGH**

- **Materialize** (node card) and **Execute** (sidebar) invoke the *identical* action
  `executeSingleNode(id, { loadPreviewOnSuccess: true })` — two different verbs for one behavior
  ([`DatabaseSourceNode.tsx:90`](../frontend/src/components/flow/nodes/DatabaseSourceNode.tsx),
  [`NodeConfigPanel.tsx:640`](../frontend/src/components/panels/NodeConfigPanel.tsx)).
- The sidebar has **two** db_source renderers, each with its own Execute button: the `renderQueryPanel`
  path ([`NodeConfigPanel.tsx:599`](../frontend/src/components/panels/NodeConfigPanel.tsx)) and a fallback
  block ([`:733`](../frontend/src/components/panels/NodeConfigPanel.tsx)) — duplicate logic that can drift.
- Visual language differs for the same logical action: node = underline **text-links**, sidebar = filled **pill button**.

**Target:** one canonical verb per action; a single `<NodeActions>` component shared by card and panel; one button taxonomy.

### F3 — Right config panel cannot be hidden — **MEDIUM**

`NodeConfigPanel` is always mounted ([`PipelineEditorPage.tsx:178`](../frontend/src/components/projects/PipelineEditorPage.tsx)).
It is resizable (320–704px) but has **no collapse/hide control** — even though the Data Preview panel
directly below it *does* ([`PipelineEditorPage.tsx:203`](../frontend/src/components/projects/PipelineEditorPage.tsx)).
With no node selected it still occupies 320px to show "Select a node to configure."

**Target:** a consistent dockable-panel pattern — both side and bottom panels collapse/expand the same way, with persisted size.

### F4 — Three hand-rolled toggle switches, three styles — **MEDIUM**

| Toggle | Size | On color | Ref |
|---|---|---|---|
| Edit mode | h-7 w-14 | `bg-stone-900` | [`NodeConfigPanel.tsx:535`](../frontend/src/components/panels/NodeConfigPanel.tsx) |
| CSV preprocessing | h-6 w-11 | `bg-blue-500` | [`NodeConfigPanel.tsx:809`](../frontend/src/components/panels/NodeConfigPanel.tsx) |
| Excel preprocessing | h-6 w-11 | `bg-emerald-500` | [`NodeConfigPanel.tsx:992`](../frontend/src/components/panels/NodeConfigPanel.tsx) |

**Target:** one `<Switch>` component, one size scale, color from a semantic token.

### F5 — Status label logic computed in three places — **MEDIUM**

"Running / Connecting / Cancelled / Cached / …" is derived independently in:
- [`NodeStatusBadge.tsx:26`](../frontend/src/components/flow/NodeStatusBadge.tsx)
- `nodeStatusLabel` at [`NodeConfigPanel.tsx:425`](../frontend/src/components/panels/NodeConfigPanel.tsx)
- the button-label ternary at [`NodeConfigPanel.tsx:587`](../frontend/src/components/panels/NodeConfigPanel.tsx)

These can and do diverge (e.g. sidebar shows `Status: cancelled` while the badge shows a styled `Cancelled` chip).

**Target:** one `statusPresentation(result)` helper → `{ label, tone, dotAnimated }`, consumed everywhere.

### F6 — Unsystematized visual language — **MEDIUM**

- **Color semantics** are inline, not tokenized: db accents hardcoded (`oracle`=orange, `postgres`=teal,
  [`DatabaseSourceNode.tsx:14`](../frontend/src/components/flow/nodes/DatabaseSourceNode.tsx)); action colors
  scattered (emerald = DB execute *and* CSV load *and* Excel load; purple = transform; blue = CSV preview).
- **Radius** drifts across `rounded` → `rounded-3xl` for similar surfaces.
- **Type:** ≥4 uppercase letter-spacings (`tracking-wide`, `tracking-[0.18em]`, `tracking-[0.24em]`) and
  mixed `text-[10px]/[11px]/xs` for the same label role; serif (`font-serif`) appears only on Project Home
  + "not found", never in the editor (partial brand language).

**Target:** a token layer (color/space/radius/type) and one label/heading scale.

---

## 4. Component & state inventory (what the design system must cover)

The redesign must reproduce **every** surface and state below. States marked ✅ were captured live this session.

**Nodes (React Flow cards), 5 types** — `csv_source`, `excel_source`, `db_source`, `transform`, `export`.
Each needs the full status range:
- idle · connecting · running (animated) · success ✅ · **cached** ✅ · error ✅ · cancelled ✅
- per-type accent + icon; row/col count line; inline actions (Preview, Materialize/Load, View table); error → "View error".

**Right config panel** — collapsed (320px) and expanded (≤704px) widths; "no selection" empty state ✅;
per-type bodies: db_source (query + Execute/Abort) ✅, transform (query + Run and Preview, Available Tables chips) ✅,
csv_source (upload, preprocessing toggle + runtime + script, Preview/Preprocess/Load) ✅,
excel_source (workbook upload, sheet select + grid, preprocessing, Preview/Preprocess/Load),
export (format). Edit-mode toggle; "⋯" actions menu (Edit/Delete).

**Bottom Data Preview panel** — collapsed/expanded; table view (TanStack); **live preview** tab incl.
`LIVE — NOT MATERIALIZED` + Materialize/Close + error row ✅; "Click Preview data…" empty state ✅.

**Modals / dialogs** — Create/Edit Node ✅ ([`NodeEditorModal`](../frontend/src/components/panels/NodeEditorModal.tsx)),
Saved Connection ([`SavedConnectionModal`](../frontend/src/components/connections/SavedConnectionModal.tsx)),
Project Settings ([`ProjectSettingsModal`](../frontend/src/components/panels/ProjectSettingsModal.tsx)),
Node Error dialog ✅ ([`NodeErrorDialog`](../frontend/src/components/flow/NodeErrorDialog.tsx)).

**Chrome** — Toolbar (add-node buttons, Save, Execute ✅, Force Refresh ✅, Database Source picker dropdown),
Project Home ✅ ([`ProjectHome`](../frontend/src/components/projects/ProjectHome.tsx)) + sidebar,
Platform Settings ([`PlatformSettingsPage`](../frontend/src/components/settings/PlatformSettingsPage.tsx)).

---

## 5. Functionality-preservation contract (must survive the redesign)

The redesign is a **presentation-layer** change over these stable contracts. Hold them fixed and behavior is preserved by construction:

- **Store actions** in [`pipelineStore.ts`](../frontend/src/store/pipelineStore.ts): `addNode`,
  `updateNodeData`, `deleteNode`, `executeSingleNode`, `runTransformPreview`, `startLivePreview`,
  `loadTablePreview`, `loadCsvPreview`/`loadPreprocessedCsvPreview`, `abortDatabaseNodeExecution`,
  `setSelectedNodeId`, `open*Editor`, `onNodesChange`/`onEdgesChange`/`onConnect`.
- **API surface** in [`client.ts`](../frontend/src/api/client.ts).
- **React Flow wiring** in [`FlowCanvas.tsx`](../frontend/src/components/flow/FlowCanvas.tsx) — node `type`s,
  drag-drop MIME contract ([`dragData.ts`](../frontend/src/lib/dragData.ts)), connect/select handlers.
- **Layout math** in [`pipelineEditorLayout.ts`](../frontend/src/components/projects/pipelineEditorLayout.ts).
- **Tests** as the net: [`pipelineStore.test.ts`](../frontend/src/store/pipelineStore.test.ts),
  [`NodeConfigPanel.test.tsx`](../frontend/src/components/panels/NodeConfigPanel.test.tsx). **Expand these
  before porting** so "behavior survived" is provable, not hoped.

---

## 6. Target design system (build in Claude Design)

1. **Tokens** — color (brand, surface, border, 6 status tones, 2 db accents), space scale, radius scale, type scale (incl. the serif display role).
2. **Core components** — `Button` (primary/secondary/ghost/danger × sizes), `Switch`, `StatusBadge`
   (one status model, F5), `NodeCard` (5 types × all states, F4 inputs), `DockPanel` (collapsible, F3),
   `Modal`, `Toolbar`, `DataGrid`, `SqlEditor` frame.
3. **One node-activity state model** (F1) — single source of truth feeding card badge, sidebar button label/color, and live-preview indicator.
4. **One action taxonomy** (F2) — canonical verbs, shared `<NodeActions>`.

---

## 7. Appendix — screenshots captured live (2026-06-15 session)

Captured against the running app (`localhost:5173`) via the preview tooling. Re-capturable on demand.

1. **Project Home** — left project sidebar (New Project / Platform Settings / project list), serif hero card.
2. **Editor — idle** — canvas with CSV/transform nodes; right panel empty ("Select a node to configure"); bottom preview empty.
3. **Transform config panel** — Edit-mode toggle, Table, Available Tables chips, Monaco SQL, "Run and Preview" (purple).
4. **Multi-state run** — `new_list` CSV **error** (red + "View error"), `prod` CSV **success** ("24 rows × 4 cols"), both transforms **cancelled** (gray); sidebar "Status: cancelled".
5. **Node error dialog** — "Preprocessing is enabled… Click Preprocess and review…"; sidebar shows CSV preprocessing toggle (blue) + Python script.
6. **Cached state** — `prod` CSV shows sky-blue **Cached** badge after re-run.
7. **Create Database Source modal** — Label, Table Name, DB Type, connection fields, Test Connection, Monaco query.
8. **Database Source node + sidebar** — node card with **Preview / Materialize** text-links; sidebar with filled **Execute** (dual action surfaces, F2).
9. **Execute enabled** — query set, sidebar **Execute** emerald/enabled (baseline).
10. **Preview→Execute desync (live)** — node **Preview** triggered `LIVE — NOT MATERIALIZED` + connection error in the bottom panel, while sidebar **Execute** stayed green "Execute" and the node badge unchanged (F1).
