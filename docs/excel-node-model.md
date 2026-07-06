# Excel Node Model — Canonical Domain Spec

> **Status: canonical / authoritative — implemented (see §7).** This is the
> single source of truth for how Excel workbooks and their sheets are represented on the
> canvas, how they execute, and how they relate to the pipeline DAG. When code, UI copy,
> API fields, or conversation disagree about Excel semantics, **this document wins** —
> fix the divergence to match it, or change this document first.
>
> This spec builds on [node-state-model.md](node-state-model.md): Sheet nodes obey that
> model completely; the Excel node is deliberately *outside* it. Read both before
> touching Excel import, sheet-node creation, or the engine's DAG construction.

**Audience:** humans and AI agents working on Excel import. **Why it exists:** Excel
files are unlike other sources — one file ubiquitously carries many independent tables
(sheets). Modeling that inside a single node breaks the invariant "one node = one table"
that the engine, cache keys, and node-state model all depend on. This doc records the
design that preserves the invariant instead.

---

## 1. The two node types

Multi-sheet import is modeled as **two node types**, not one multi-table node:

```
  ┌────────────┐  structural   ┌────────────┐  data edge   ┌───────────┐
  │ EXCEL NODE │══════════════▶│ SHEET NODE │─────────────▶│ Transform │
  │ (workbook  │  structural   ├────────────┤              └───────────┘
  │   hub)     │══════════════▶│ SHEET NODE │─── ...
  └────────────┘               └────────────┘
   no table                     one table each
   not in the DAG               ordinary DAG nodes
```

| | **Excel node** (workbook hub) | **Sheet node** |
|---|---|---|
| Represents | The uploaded workbook — a *connection*, like a tiny database | One extraction from one sheet — one table |
| `table_name` | **None** (exempt from the requirement and uniqueness validation) | Required, unique, as for any node |
| Data state (Axis 3) | **None** — no `python_mem` / `duckdb_mem` / `duckdb_disk` | All three locations, per [node-state-model.md](node-state-model.md) |
| In the execution DAG | **No** — invisible to the engine | Yes — ordinary node |
| Outgoing edges | Structural edges to its Sheet nodes only | Normal data edges to downstream nodes |
| Config | `file_path`, `original_filename`, `sheet_names` (+ cheap per-sheet metadata) | `file_path`, `sheet`, `cell_range`, `header`, `all_varchar`, `load_mode` |

**The database analogy is the mental model:** the Excel node is the *connection* (the
workbook is a forever-changing little database), each Sheet node is a *table extraction*.
Downstream nodes know only their Sheet-node upstream; the Excel node's existence is
irrelevant to them.

> **Migration note.** A Sheet node is functionally today's `excel_source` node
> (file + one sheet + read options + one table). Existing `excel_source` nodes migrate as
> **parentless Sheet nodes** (see §5 orphans) — no data or config is lost.

---

## 2. Ubiquitous Language (glossary)

| Term | Canonical meaning |
|---|---|
| **Excel node** / **workbook hub** | The connection-like node representing one uploaded workbook. No table, no data state, outside the DAG. |
| **Sheet node** | An ordinary data node extracting one table from one sheet of a workbook. Fully obeys the node-state model. |
| **Structural edge** | The always-paired Excel→Sheet edge. Visual lineage only; **not** a data dependency; filtered out of the engine's DAG by *source node type*. |
| **Data edge** | Every other edge. Means "upstream feeds downstream", exactly as today. |
| **Sheet picker** | The navigator UI on the Excel node: checkbox list of sheets → confirm → n Sheet nodes are created. |
| **Rollup status** | The Excel node's displayed status, *derived* in the frontend from its children's results. Never an engine result. |
| **Orphan (Sheet node)** | A Sheet node whose Excel node was deleted. Keeps working (its config carries everything needed to re-load). |

**Terminology rule:** "Excel node" always means the hub. Never say the hub "loads data"
or "has a table" — it doesn't. Extraction verbs (load, materialize, preview) belong to
Sheet nodes only.

---

## 3. Execution semantics

### 3.1 The Excel node is invisible to the engine

- **DAG construction:** when building `upstream_ids`, the engine **skips edges whose
  source node is an Excel node** (type-filter on the source node's type — deliberately
  *not* a `kind` field on `EdgeDefinition`, so the rule cannot drift out of sync with the
  node type).
- **Scheduling:** the Excel node is **skipped** during pipeline execution — no task, no
  done-event, no `NodeExecutionResult`. It is not a no-op success; it simply does not
  participate.
- **Validation:** `_validate_table_names` and any "every node has a table" assumption
  exempt the Excel node type. `NodeDefinition.table_name` becomes optional, allowed to be
  absent **only** for this type.

**Consequence (the point of all this):** a Sheet node's downstream sees an ordinary
single-table upstream. Failure, staleness, and resolution flow exactly as
[node-state-model.md §5–6](node-state-model.md) already specify. Nothing downstream ever
waits on, or is poisoned by, the Excel node.

### 3.2 "Executing" the Excel node is a canvas action, not engine execution

Triggering the Excel node (from its panel / sheet picker) does two things:

1. **Creates Sheet nodes** for the user's sheet selection — a frontend/store operation,
   plus the structural edges, auto-laid-out in a column to the right of the hub.
2. **Optionally batch-triggers** each new Sheet node's own load/materialize — the same
   per-node run they would get individually (`runNodeWithLoadMode`), nothing more.

The engine's execution path is never entered *by the Excel node itself*.

### 3.3 Partial failure & the rollup

Each Sheet node owns its result: its own status chip, error dialog, elapsed time, and
cache state. Sheet 5 failing has **zero effect** on sheets 1–4 — they stay consumable if
their data is loaded.

The Excel node's displayed status is a **display-only rollup**, computed in the frontend
from its children:

| Excel node shows | When |
|---|---|
| `error` | Its *own* action failed (file missing/unreadable at upload or re-parse), **or** all of its Sheet nodes' last runs failed |
| mixed badge (e.g. "3/5 loaded, 1 failed") | Some children succeeded, some failed |
| neutral | Otherwise |

**Locked rule:** the rollup is never persisted, never an engine result, and never
propagates anywhere. (Same doctrine as `NodeLifecycle` in
[node-state-model.md §7](node-state-model.md): a projection, not a fourth source of
truth.)

---

## 4. Creation workflow (the sheet picker)

1. User creates an Excel node and uploads a workbook (`.xlsx`/`.xlsm`). Backend copies it
   to `UPLOAD_DIR` and returns `sheet_names` parsed from `xl/workbook.xml` — no
   spreadsheet engine, no preview roundtrip.
2. The Excel node's panel shows the **sheet picker**: checkbox list of all sheets, with
   any *cheaply* extractable metadata (see note below).
3. Per selected sheet, the picker shows the extraction options: **table name**, **range**
   (optional A1), **header** (default on), **all_varchar** (default off).
   - **Table names are prefilled** from the slugified sheet name (`DS Active` →
     `ds_active`), deduped with a numeric suffix, validated inline against
     `validate_user_table_name` + project-wide uniqueness. Empty-name errors should be
     the rare case, not the default state.
4. Confirm → n Sheet nodes + n structural edges are created; optionally batch-load (§3.2).
5. Reopening the picker later **adds more Sheet nodes** for still-unimported sheets.
   Multiple Sheet nodes **may target the same sheet** (e.g. two ranges extracting two
   tables that share a sheet — a legitimate, common Excel layout).

> **Sheet metadata is best-effort only.** Row×column counts *may* be read from each
> worksheet's optional `<dimension ref>` in the zip, but that attribute is absent or
> stale in some writers. Show it when available; show nothing when not. **Never** add a
> full-file parse or preview roundtrip just to decorate the picker — the cheap-zip-read
> design of [excel_service.py](../backend/app/services/excel_service.py) is deliberate.

---

## 5. Lifecycle rules

| Event | Rule |
|---|---|
| **Delete Excel node** (with children) | **Orphan** the Sheet nodes, after a confirm dialog. Copy must say the Sheet nodes *keep working* (they carry `file_path` + options and can still re-load) but lose the workbook grouping and the add-sheets / replace affordances. |
| **Delete a Sheet node** | Its structural edge dies with it. The Excel node stays, even with zero children (it remains the place to add sheets). |
| **Delete a structural edge alone** | **Forbidden.** The edge exists exactly as long as the Sheet node exists — the canvas must not allow deleting it independently. |
| **Delete a Sheet node's data / go stale** | Ordinary node-state-model behavior. No effect on siblings or the hub (beyond the rollup display). |
| **Replace workbook** (re-upload on the Excel node) | Every Sheet node still joined by a structural edge is **re-pointed to the new upload** (`file_path`, `sheet_names`, `original_filename`); their cache keys go stale automatically. New `sheet_names` are diffed against children. Sheet nodes referencing now-missing sheets are **flagged proactively in the Excel node's panel** ("2 sheet nodes reference sheets not in this file") and will fail at their next load with a clear error — like a dropped table in a database. No automatic remapping. Orphans (no structural edge) are never re-pointed. |

> **Known pre-existing fragility (out of scope here):** uploads land at
> `UPLOAD_DIR / filename`, so a later same-named upload silently overwrites the file that
> orphaned or sibling nodes point at. Fix separately (content-hashed upload paths); this
> spec does not depend on it.

---

## 6. Relationship to the node-state model

- **Sheet nodes:** obey [node-state-model.md](node-state-model.md) *in full* — three
  axes, three locations, Merkle cache keys, downstream resolution, load/materialize
  prompt. Nothing in this spec overrides it. (`python_mem` live preview for Excel remains
  deferred, per that doc's §8.2.)
- **Excel node:** has **no Axis 3** (no locations, no dots) and **no Axis 1 engine
  status** — its displayed state is upload/parse validity plus the rollup (§3.3). It has
  a minimal Axis-2-like editable state (its file + picker selections), but since it
  produces no table there is nothing for a "modified" gate to protect.
- **Cache keys:** a Sheet node's key hashes its *own* result-affecting config
  (`file_path`, `sheet`, `cell_range`, `header`, `all_varchar`) exactly like today's
  `excel_source`. The Excel node contributes nothing to any key — editing one sheet's
  range never invalidates a sibling.

---

## 7. Implementation status

| Concept | Status | Notes |
|---|---|---|
| Single-sheet `excel_source` node (≈ Sheet node) | ✅ Implemented | Upload, sheet dropdown, range/header/all_varchar, load/materialize via `register_excel`. |
| Sheet-name listing from zip | ✅ Implemented | [excel_service.py](../backend/app/services/excel_service.py). |
| Excel node (hub) type | ✅ Implemented | `NodeType.EXCEL_WORKBOOK`; `table_name` optional (hub-only, enforced by validators); exemptions in engine + routers mirror the EXPORT precedent. |
| Structural-edge type-filter in DAG build | ✅ Implemented | [pipeline_graph.py](../backend/app/services/pipeline_graph.py) `is_structural_edge`/`data_upstream_ids`, used by the engine, cache keys, upstream resolution, and the preview gate. `topological_sort` deliberately stays unfiltered (cycle-validation needs every node). |
| Skip hub in execution scheduling | ✅ Implemented | No task/done-event/result in pipeline runs; excluded from execution-registry run tracking; single-node endpoints return 400; `/cache-status` omits hubs. |
| Sheet picker UI (navigator modal) | ✅ Implemented | `SheetPickerModal` — checkbox rows, prefilled slugified/deduped table names (`lib/tableNames.ts`), inline validation, imported tags, batch-load choice. Dimensions still pending (§4 note). |
| Sheet-node creation + auto-layout + batch load | ✅ Implemented | `addWorkbookSheets` store action: column right of hub, reopen stacks below existing children, sequential batch load survives per-sheet failures. Toolbar chip now creates hubs; create-modal auto-opens the picker. |
| Rollup status on hub card | ✅ Implemented | `lib/workbookRollup.ts` (pure projection), rendered by `ExcelWorkbookNode` + hub config panel. |
| Orphan-with-confirm deletion | ✅ Implemented | Hub-specific danger Modal in `NodeConfigPanel`; copy states children keep working. Structural edges not user-deletable (store + canvas guards). |
| Replace-workbook diff in hub panel | ✅ Implemented | Replace button on the hub panel → `replaceWorkbookFile` re-points structural children (orphans untouched); missing-sheet children listed proactively with click-to-select. |
| Best-effort sheet dimensions in picker | ✅ Implemented | `read_sheet_dimensions` (zip-stream `<dimension ref>` read, stops at `sheetData`, never a full parse); shown as "rows × cols" in picker rows when present. |
| Migration of existing `excel_source` nodes | ✅ N/A by construction | Sheet node type id *is* `excel_source`; existing nodes are already valid parentless Sheet nodes. |
| Code type ids for the two node types | ✅ Decided | Hub = `excel_workbook`; sheet keeps `excel_source` (keeps the engine branch, cache-key branch, and `selected_sheet` config key untouched; makes migration a no-op). |

---

## 8. Decisions log

Locked decisions (so they don't get relitigated):

1. **Two node types, not one multi-table node.** The invariant "one node = one table"
   is preserved; multi-sheet import multiplies *nodes*, not tables-per-node.
2. **The Excel node is outside the execution DAG entirely.** No task, no result, no
   done-event. Its "execution" is a canvas action: create Sheet nodes + optionally
   batch-trigger their loads.
3. **Structural edges are filtered by source node *type*** — no `kind` field on
   `EdgeDefinition`.
4. **The hub's status is a display-only rollup**, computed frontend-side from children;
   `error` only when its own action failed or *all* children failed. Never persisted,
   never propagates.
5. **`table_name` is optional only for the Excel node type**; uniqueness validation
   skips it.
6. **Deleting the hub orphans its Sheet nodes** (confirm dialog; copy must not imply the
   children stop working). Deleting the last Sheet node does not delete the hub.
7. **Structural edges are not independently deletable** — they live and die with their
   Sheet node.
8. **Sheet metadata in the picker is best-effort zip-reads only** — never a parse or
   preview roundtrip.
9. **Table names are prefilled** from slugified sheet names, deduped, validated inline.
10. **Multiple Sheet nodes may extract the same sheet** (different ranges are a real
    Excel use case).
11. **Failure never travels hub-ward or between siblings.** Sheets 1–4 stay consumable
    when sheet 5 fails.

---

*Keep this document and the code mutually honest. If you change Excel node semantics,
update this file in the same change.*
