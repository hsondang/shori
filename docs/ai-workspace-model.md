# AI Workspace Model — Canonical Domain Spec

> **Status: canonical / authoritative — target, not yet implemented.** This is the
> single source of truth for the AI workspace: the sandboxed, per-project DuckDB
> database that a terminal coding agent (Claude Code / Codex) explores through an
> MCP server. When code, UI copy, API fields, or conversation disagree about AI
> workspace semantics, **this document wins** — fix the divergence to match it, or
> change this document first.
>
> This spec deliberately sits *outside* [node-state-model.md](node-state-model.md):
> the AI workspace is not part of the pipeline DAG, has no nodes, and never touches
> the user project's DuckDB file. Its only connection to the pipeline world is the
> one-way clone described in §3.

**Audience:** humans and AI agents implementing or reviewing the AI integration.
**Why it exists:** giving an AI agent access to a database demands explicit,
reviewable boundaries. This doc records where every boundary is and why.

---

## 1. Shape of the system

```
 ┌─────────────┐  stdio (MCP)   ┌──────────────────┐   HTTP (localhost)
 │ claude code │◀──────────────▶│  shori_mcp.py     │◀────────────────────┐
 │ / codex     │                │  (thin shim,      │                     │
 │ (terminal)  │                │   NO database)    │                     ▼
 └─────────────┘                └──────────────────┘        ┌───────────────────────┐
                                                            │  backend process       │
 ┌─────────────┐  HTTP: /ai/* (SPA + API)                   │  ┌──────────────────┐  │
 │ browser tab │◀──────────────────────────────────────────▶│  │ ai_workspace     │  │
 │ (AI wkspc   │                                            │  │ sub-app (mounted)│  │
 │  UI)        │                                            │  └──────────────────┘  │
 └─────────────┘                                            │  ┌──────────────────┐  │
                                       parquet spool        │  │ main app (app/)  │  │
                     data/ai/<project>/inbox/  ◀────────────│──│ export node      │  │
                                                            │  └──────────────────┘  │
                                                            └───────────────────────┘
             DuckDB files:  data/projects/<id>.duckdb   (user workspace — main app only)
                            data/ai/<id>/workspace.duckdb (AI workspace — sub-app only)
```

Three components, one process rule:

| Component | What it is | Touches DuckDB? |
|---|---|---|
| `shori_mcp.py` | Single-file stdio MCP server, spawned and lifecycle-managed by the agent CLI. Every tool is one HTTP call to the sub-app. Contains **no** DuckDB, permission, or SQL logic. Holds exactly one piece of state: the session's pinned project (§6a). | Never |
| `ai_workspace` sub-app | Self-contained FastAPI app mounted at `/ai` on the existing backend. Owns the AI workspace DuckDB files, permissions, results, audit log, and serves the workspace SPA. | AI workspace files only |
| Main app (`backend/app/`) | Unchanged except three touchpoints (§5). | User project files only (as today) |

**Invariant (concurrency):** exactly one process — the backend — ever opens a DuckDB
file, and within it, the main app and the sub-app each own disjoint sets of files.
The MCP shim and the browser tab are HTTP clients. This is why a separate DuckDB
file per project remains a valid AI workspace store: DuckDB's
one-writer-process-or-many-readers rule is never tested.

## 2. The sandbox invariants

The AI workspace connection is opened once per project with:

```sql
SET memory_limit = '...';            -- operational guardrail
SET enable_external_access = false;  -- disables ATTACH, read_csv/read_parquet on
                                     -- arbitrary paths, COPY TO, extension loading
SET lock_configuration = true;       -- SQL can no longer change any setting
```

Consequences:

- **AI SQL cannot escape the workspace.** No `ATTACH 'data/projects/x.duckdb'`, no
  reading or writing files. Separation of impact comes from this workspace boundary,
  **not** from restricting statement types.
- Therefore **full SQL is allowed inside the workspace** — `CREATE TABLE AS`,
  `CREATE VIEW`, `INSERT`, `DROP` on AI-created objects. The agent can build
  scratch tables freely; that is the point of the sandbox.
- **Ingestion must use Arrow, not SQL file reads.** Because
  `enable_external_access` is instance-global and locked, the sub-app cannot
  `CREATE TABLE t AS FROM 'inbox/t.parquet'` on the same instance. It reads the
  parquet with pyarrow, registers the Arrow table, and CTAS-es from it.
- Queries run with a wall-clock timeout enforced via DuckDB's interrupt handle.

## 3. Data flow in: clone-by-export

Data enters the AI workspace **only** through an explicit user action in the main
app — the export node gains an "Export to AI workspace" target. The clone is the
primary permission boundary: a table the user never exported does not exist in the
AI's world, at any permission level.

Mechanics (file-drop contract, no cross-package imports):

1. Main app runs the existing `export_service.export_table_to_local(fmt="parquet")`
   into `data/ai/<project_id>/inbox/<table>.parquet`, plus a JSON sidecar
   `{source_node_id, source_table, exported_at}`.
2. The sub-app sweeps its inbox lazily (on the next list/UI request): pyarrow read →
   register → `CREATE OR REPLACE TABLE` → record `{cloned_from, cloned_at}` in its
   metadata store → delete the spool files.

Clones are snapshots. Staleness is surfaced (`cloned_at` in the UI and in
`shori_list_ai_tables`); refresh = export again. There are no live views into the
user workspace, by design.

## 4. Consent model

Three mechanisms, in increasing order of what they grant. There is no MCP tool
that reads user-workspace data, mutates any of these controls, clones data, or
deletes the workspace — controls change **only** in the workspace UI.

1. **The clone boundary (per-table, implicit).** A table exists in the AI's
   world only because the user exported it. Cloning *is* consent for the agent
   to see that table's schema (name, columns, types, `cloned_at`, provenance)
   and to draft/validate SQL against it. If even a table's column names are too
   sensitive, don't clone it.

2. **Standing workspace toggles (default off).**

   | Toggle | Meaning |
   |---|---|
   | `autonomous_execute` | The agent may execute the editor's query directly. Results still land in the UI, **not** with the agent. |
   | `auto_share_results` | Query results are visible to the agent without per-result approval. |

3. **Just-in-time approvals (per action, when the standing grant is off).** A
   gated tool call is not a dead end: it registers a **pending request** that
   surfaces in the UI and returns immediately with a structured
   `pending_approval` message telling the agent to ask the user. Execution
   requests are approved by clicking Run; result disclosure by a per-result
   **"Share with AI"** action. The user approves while looking at the concrete
   query or the concrete rows — not an abstract standing rule. (MCP calls must
   not block on human input, hence request-then-recheck rather than waiting.)

Additional rules, unchanged from v1:

- **Enforcement lives in the sub-app only.** The MCP shim performs no checks
  (it couldn't be trusted to anyway); the UI and the shim hit the same
  enforcement path.
- **Every tool call is audited**: tool name, SQL text, decision
  (allowed / pending / denied), timestamp. The activity feed is a first-class
  UI surface.

### Superseded design: the per-table toggle matrix

v1 specified per-table `schema`/`execute`/`preview` toggles with binder-based
multi-table checks and derived-object lineage inheritance. Dropped, deliberately:

- The clone boundary already provides per-table *schema* consent; a toggle was
  redundant.
- Per-table *execute* protected little: execution is blind and cannot touch the
  user workspace.
- Per-table *preview* was pre-authorization and required lineage inheritance to
  stop `CREATE VIEW v AS SELECT * FROM restricted` from laundering access.
  Just-in-time, per-result approval subsumes it: **the user is the lineage
  check**, deciding at disclosure time with the actual rows on screen. All
  lineage machinery is deleted.
- Note: embedded DuckDB has no user/role/GRANT system — per-table enforcement
  was always going to be sub-app logic, never the database's.

Trade-off accepted: standing grants are workspace-global. In a
mixed-sensitivity workspace, leave `auto_share_results` off and approve per
result. Revival path if per-result approval proves tedious in long sessions:
an escalation in the approval dialog ("always share results from table X"),
reintroducing per-table rules as opt-in convenience.

### Known, accepted leaks (decided consciously)

- `shori_execute` returns success/failure and the **DuckDB error message**.
  Error text can quote data values (e.g. a failed cast quoting the offending
  string). Accepted for MVP; row counts and result column names are deliberately
  **not** returned (may be added later).
- Schema itself (column names) is information; that is the explicit floor of
  the clone boundary — the table was user-exported to begin with.

### What this model is — and is not

This is **control and audit, not a hard security perimeter**. The agent runs as
the user; an agent session granted unrestricted Bash can bypass everything by
copying database files. The hard levers are session-level: run data-exploration
sessions with `--allowedTools "mcp__shori__*"` (no Bash) — the MCP tools make
Bash unnecessary for this workflow. The permission system's value is making the
sanctioned path the easy path, bounding prompt-injection blast radius, and
producing an audit trail.

## 5. Coupling contract with the main app

The entire feature's footprint on the existing codebase is **three touchpoints**:

1. `app/main.py`: a guarded mount —
   `try: app.mount("/ai", build_ai_app()) except Exception: log + continue`.
   If the AI package is broken or removed, the main app boots unaffected.
2. Export node: one new export target ("AI workspace") that calls the existing
   parquet export into the spool path.
3. Frontend: one button that opens `/ai/<project_id>` in a new tab.

Everything else lives in `backend/ai_workspace/` (own package; imports nothing
from `app/`), the SPA package, and `shori_mcp.py`. Besides the spool, the
sub-app has one more deliberately narrow interface to main-app data: it opens
`data/projects.sqlite3` **read-only** (SQLite URI `mode=ro`) to resolve project
identity (`id`, `name`) — a stable two-column dependency, never a write.
**Removal = delete those + revert three small diffs.** The same
contract means the sub-app could later be lifted into a separate process (its own
port) without touching the main app again, if stronger isolation is ever wanted.

## 6. MCP tool surface (MVP)

### 6a. Project binding — no per-call `project_id`

Tools do **not** take a `project_id` argument. The model never states the project;
the shim injects it into every HTTP call. This is structural drift-prevention: as
the session context grows or compacts, there is no parameter for the model to get
wrong, and no "remember the project" burden on it. The backend API itself stays
stateless — every HTTP request carries an explicit project id — so enforcement
and audit are unaffected.

Two binding modes:

1. **Session pin (default).** The shim starts unpinned. Every tool except
   `shori_list_projects` / `shori_use_project` fails closed with a structured
   error ("no project selected — call shori_list_projects, then
   shori_use_project") until `shori_use_project` pins one. Re-pinning requires
   another explicit, transcript-visible, audited `shori_use_project` call. The
   pin lives in the shim process (one shim per agent session → per-terminal
   scope; two terminals on two projects never share a pin). The pin is
   deliberately **not** persisted: a shim restart loses it and fails closed —
   a stale auto-restored pin is exactly the silent-wrong-project bug this
   design exists to prevent.
2. **Config pin (stronger).** If `SHORI_PROJECT_ID` is set on the shim (env or
   `--project` flag at registration), the session is born locked:
   `shori_list_projects` and `shori_use_project` are not registered at all, and
   the pin survives restarts by construction. The workspace UI displays a
   copyable per-project setup command (e.g. `claude mcp add shori --env
   SHORI_PROJECT_ID=<id> -- uv run shori_mcp.py`) to make this the standard
   ritual for side-by-side sessions.

Known limitation (session-pin mode): agent-CLI subagents share the parent
session's MCP servers, so a subagent calling `shori_use_project` re-pins the
whole session. Visible in the audit log; nonexistent under config pin.

### 6b. Tools

All tools are read-only from the main app's perspective; none can affect the user
workspace. Gates refer to the §4 consent model. All tools except the first two
operate on the pinned project.

| Tool | Gate | Behavior |
|---|---|---|
| `shori_list_projects()` | — (absent under config pin) | project id, name, whether an AI workspace exists |
| `shori_use_project(project_id)` | — (absent under config pin) | pins the session; echoes `{project_id, name}` |
| `shori_get_workspace_state()` | — | toggle states, editor status (clean / user-edited), latest result metadata + shared flag, pending requests. Lets the agent know its situation and what to ask the user to enable |
| `shori_list_tables()` | clone boundary | cloned tables with column count, `cloned_from`, `cloned_at` |
| `shori_get_table_schema(table)` | clone boundary | columns + types |
| `shori_validate_sql(sql)` | clone boundary | `EXPLAIN`-only: output columns/types or the bind/syntax error; never executes |
| `shori_read_editor()` | clone boundary | current editor SQL + whether the user has edited since the agent last wrote |
| `shori_write_editor(sql, note)` | clone boundary | writes the editor directly when it is clean (empty or agent-owned); **stages a draft** with a Load banner when the user has local edits — the agent never clobbers the user's typing. Return says which happened |
| `shori_execute()` | `autonomous_execute`, else JIT | executes the **editor's current content**. Granted → `{status, result_id, error?}` — no rows, no counts. Gated → registers a pending execution request (UI: Run button highlights), returns `pending_approval` |
| `shori_get_result(result_id \| "latest", limit≤100)` | `auto_share_results` or per-result share, else JIT | capped rows when the result is shared; otherwise registers a disclosure request, returns `pending_approval` |

Notes:

- **Editor-bound execution is the audit invariant:** the only SQL that can ever
  run is SQL visibly sitting in the editor. There are no hidden agent queries —
  even in autonomous mode, every step rewrites the editor and lands in the feed.
- **User-shared results are in scope.** The user can run a query themselves and
  click "Share with AI"; `shori_get_result("latest")` then returns it. This is
  the primary "use this result as context" workflow (core use case 1) and is
  safe because sharing is a deliberate per-result user action.
- Workflow ladder by trust posture: default → agent drafts into the editor,
  validates, user runs and reads (use case 2 needs nothing beyond this);
  \+ `autonomous_execute` → agent iterates blind; + sharing/auto-share → agent
  reads results and can summarize, derive insights, and build on them.

## 7. Workspace UI (separate SPA)

A small standalone SPA served by the sub-app at `/ai/<project_id>`, opened in a
new tab from the main app. Decoupled in the Claude-Design-vs-claude.ai sense:
separate package, separate build, shared backend port. Surfaces:

- **Tables**: cloned tables with provenance (`cloned_from`, `cloned_at`,
  staleness badge). No per-table controls (§4).
- **Editor pane**: the shared query surface. Shows agent drafts as a staged
  banner with Load when the user has local edits, an "edited by you" badge, the
  Run button (which doubles as the approval control for pending execution
  requests), and the `autonomous_execute` toggle.
- **Result pane**: rows of the latest execution, marked "visible to you only"
  by default, with a per-result **Share with AI** action (which doubles as the
  approval control for pending disclosure requests) and the
  `auto_share_results` toggle.
- **Activity feed**: every AI tool call with its decision
  (allowed / pending / denied), SQL text, and timing — the audit surface.

MVP transport: the UI polls the sub-app (~1s); no WebSocket needed yet.

The agent conversation itself stays in the user's terminal (Claude Code / Codex);
the UI is for observation and control, not chat.

## 8. Implementation phases (small, reviewable)

1. **Phase 0 — one tool end-to-end.** ✅ implemented. `shori_mcp.py` (FastMCP,
   stdio, PEP 723 inline deps) with `shori_list_projects` + `shori_use_project`
   (session pin, fail-closed) + a stub permissions tool; guarded mount;
   registered with `claude mcp add`. (The stub `shori_get_permissions` becomes
   `shori_get_workspace_state` in Phase 2.)
2. **Phase 1 — data in, schema out.** ✅ implemented. Spool + export-to-AI
   target (export node destination) + inbox sweep with Arrow ingestion +
   workspace DuckDB creation with §2 settings (sandbox lock verified by test) +
   `list_tables` / `get_table_schema`. Clone metadata in a sidecar
   `meta.sqlite3` (agent SQL can touch every workspace table, so provenance and
   audit records must not live in the workspace DuckDB itself).
3. **Phase 2 — the shared editor.** ✅ implemented. Editor state store (content,
   owner-of-last-edit, staged drafts that supersede each other),
   `read_editor` / `write_editor` / `validate_sql` (bind-only via DESCRIBE →
   EXPLAIN, with a multi-statement guard so validation can never execute),
   `get_workspace_state` (replaces the permissions stub); activity audit of all
   agent-originated calls (`X-Shori-Client: mcp`); workspace UI as a single
   static no-build page served at `/ai/<project_id>` (tables, editor with
   draft-Load banner and edited-by-you badge, activity feed, ~1.5s polling);
   main-app toolbar button + `/ai` dev-proxy entry (touchpoint #3). Use case 2
   is complete at the default trust posture.
4. **Phase 3 — execution.** `autonomous_execute` toggle, `shori_execute` on
   editor content with timeout, JIT execution requests wired to the Run button,
   results store, feed entries.
5. **Phase 4 — disclosure.** Result pane share action + `auto_share_results`
   toggle, `shori_get_result`, JIT disclosure requests (use case 1 complete).

Each phase is independently shippable and reviewable; consent defaults to the
safest state at every step.
