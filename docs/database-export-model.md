# Database Export Model — Canonical Domain Spec

> **Status: canonical / authoritative — implemented (see §9).** This is the single
> source of truth for how a pipeline result is written *out* to an external database:
> what an export is allowed to do, who grants that permission, which copy of the data
> it reads, and where the per-engine boundary sits. When code, UI copy, API fields, or
> conversation disagree about export semantics, **this document wins** — fix the
> divergence to match it, or change this document first.
>
> This spec builds on [node-state-model.md](node-state-model.md): the export node is
> deliberately *outside* that model (it owns no table and has no data state), but the
> data it reads obeys that model's location precedence completely — §5 is the whole
> reason this document exists. It is a sibling to
> [ai-workspace-model.md](ai-workspace-model.md), which covers the *other* non-file
> export destination.

**Audience:** humans and AI agents working on export, database connections, or the
platform settings page. **Why it exists:** every other node in Shori writes into a
DuckDB file the app owns and can freely drop and rebuild. An export writes into a
database the app does *not* own, where a mistake is not recoverable by re-running.
That asymmetry — irreversible writes to someone else's system — is what drives every
decision recorded here.

---

## 1. Shape of the system

```
   ┌──────────┐  data edge   ┌────────────┐
   │ upstream │─────────────▶│   EXPORT   │   no table of its own
   │   node   │              │    node    │   not executed by the engine
   └──────────┘              └─────┬──────┘
        │                          │
        │ its table lives in       │  destination
        │ ONE OR BOTH of:          ├──────────────▶ local file   (csv/parquet/xlsx)
        ▼                          ├──────────────▶ AI workspace (parquet spool → ai-workspace-model.md §3)
   scratch.main   (in-memory)      └──────────────▶ database     (this document)
   "project".main (materialized)                          │
                                                          ▼
                                              ┌───────────────────────┐
   read the PRECEDENCE-CHOSEN copy ──────────▶│  DuckDB query cursor  │
   (§5 — never by bare name)                  └───────────┬───────────┘
                                                          │ batches of 1000 rows
                                                          ▼
                                              ┌───────────────────────┐
                                              │ DatabaseExportWriter  │  ← per-engine boundary (§8)
                                              │   OracleTableWriter   │
                                              └───────────┬───────────┘
                                                          │ executemany, one transaction
                                                          ▼
                                                   SCHEMA.TABLE  (must already exist)
```

**Invariant (ownership):** Shori owns everything to the left of the writer and nothing
to the right of it. The target table, its grants, indexes, constraints, and its
lifecycle belong to whoever administers that database. An export **adds rows and
nothing else** (§4).

## 2. Ubiquitous Language (glossary)

| Term | Means |
|---|---|
| **Destination** | Where an export node writes: `local` \| `ai_workspace` \| `database`. One field, one dropdown; the database options are individual approved connections. |
| **Export permission** (`allow_export`) | A per-connection boolean on a *global* Oracle connection. Being able to read a database does not make it writable; this is the separate, explicit grant that does. |
| **Approved connection** | A global connection with `db_type == "oracle"` and `allow_export == true`. Only these appear as destinations, and only these are accepted by the API. |
| **Target table** | The `SCHEMA.TABLE` an export appends to. Both parts are unquoted Oracle identifiers, upper-cased on parse — what the user would get typing the same name in SQL\*Plus. |
| **Export query** | The SQL an export reads. The node's own SQL when the *SQL query* toggle is on; otherwise `SELECT * FROM <upstream_table>`. |
| **Export plan** | The matching of the query's result columns to the target's columns, plus the errors and warnings that matching produced. Computed before any row is written. |
| **Validation** | An explicit, on-demand comparison of the export plan against the *live* target table. The only preview-time operation that touches the destination database. |
| **Writer** | The per-engine half of an export (`DatabaseExportWriter`). Oracle is the only implementation today. |

## 3. The permission model

Export permission is **opt-in, Oracle-only, and revocable at any time.**

| Rule | Why |
|---|---|
| The toggle exists only on **global** connections, and only when `db_type == "oracle"` | Project-local connections live inside pipeline JSON and are not a platform-administered surface. Postgres has no export implementation yet, so offering the toggle would promise something that does not exist. |
| Default is **off** | A connection added for reading must never become writable as a side effect. |
| Switching a draft's database type **re-seeds the flag to off** | The approval was granted to a *different* database; carrying it across a type switch would silently transfer consent. |
| Revoking is **never blocked** by usage | The one operation that must always be available is withdrawal. Export nodes pointing at a revoked connection fail at export time with a clear message; they are not silently rewritten. |
| Deleting a connection **is** blocked while any node references it | Includes export nodes, which reference a connection through `destination`/`connection_source_id` and carry no `connection_mode` — the in-use guard checks both shapes. |

**Invariant (enforcement):** the destination dropdown filtering to approved connections
is *UX*. The gate is `resolve_export_connection` on the server, which re-loads the
connection from the store on every export and validate call. A node config is persisted
JSON; a stale or hand-edited one must not be able to write to a database whose approval
was never granted or has since been withdrawn.

## 4. What an export does — append only

An export issues exactly one kind of statement against the target: `INSERT`.

| Shori does | Shori never does |
|---|---|
| `INSERT` the query's rows into an existing table | `CREATE TABLE`, or any DDL |
| Leave unsupplied target columns to their defaults | `DROP`, `TRUNCATE`, `DELETE`, `MERGE`, `UPDATE` |
| Refuse before writing when the plan is invalid (§6) | Alter grants, indexes, constraints, or storage |

The target table **must already exist**. This is the deliberate consequence of §1's
ownership invariant: inferring DDL from a DuckDB result schema means guessing at
`VARCHAR2` widths, `NUMBER` precision, nullability and defaults, and then owning those
guesses in someone else's schema forever.

**Transaction semantics: one transaction, one commit at the end, rollback on any
failure or abort.** With append-only semantics a partially written table is the worst
possible outcome — the rows are indistinguishable from legitimate ones, and there is no
way to tell from the outside how many landed. All-or-nothing also makes abort correct
for free. The accepted trade-off is undo-segment growth on very large exports; if that
ever bites, batch-commit becomes a per-node option, never a silent default.

## 5. Reading the source — why bare table names are forbidden

**This is the subtlest rule in this document and the easiest to regress.**

A node's data can exist in **two places at once** under the same table name: the
RAM-only `scratch` catalog and the project's DuckDB file. The connection's search path
is pinned project-first:

```
search_path = "project".main , scratch.main        ← disk FIRST
```

Disk-first is *not* the correct read order. The real precedence lives in
`consumable_location`: **fresh beats stale, then in-memory beats materialized, then
most recently built.** So:

> Running `SELECT * FROM orders` on a plain cursor returns the **stale disk copy**
> whenever a fresher in-memory copy exists — silently, with no error.

For a local-file export that is an annoyance you notice and re-run. For a database
export it means **stale rows appended to a live table, with no undo**. That asymmetry
is why the resolution machinery is mandatory here rather than merely nice:

1. `resolve_direct_upstreams(pipeline, node_id, cache_keys, manager)` picks the
   consumable copy per upstream, and reports any upstream with no copy anywhere.
2. Missing upstreams produce the standard **409 `upstreams_unavailable`** contract, and
   the UI opens the existing load-destination prompt.
3. `install_temp_views` shadows each upstream name with a connection-local
   `CREATE TEMP VIEW` over the chosen catalog. Temp views resolve *before* the search
   path and are isolated per cursor, so no shared-catalog DDL and no lock contention
   with concurrent loads.
4. `DuckDBManager.pinned_query(sql, resolution)` bundles that into one context manager,
   which also holds an op-tracker slot so a `compact()` cannot swap the project file
   mid-export.

Steps 1–3 are shared verbatim with the transform live preview — that is the point. Two
copies of this rule would mean the more dangerous caller eventually runs the
unmaintained one.

## 6. Preview, validate, export — three operations, one of which is dangerous

| Operation | Touches Oracle? | Writes? | What it is |
|---|---|---|---|
| **Preview** | **No** | No | The export query streamed from DuckDB into the existing live-preview panel. Shows exactly what *would* be written. |
| **Preview and validate target** | Yes (reads `ALL_TAB_COLUMNS`) | No | Preview, plus the export plan compared against the live target's columns. |
| **Export** | Yes | **Yes** | The tracked run that appends rows. |

They are surfaced as a **split button** (Preview primary, validate behind the caret) and
a separate Export action. The split is deliberate: validating is the same intent one
step further, but only the plain form is safe to click without thinking.

### 6.1 The export plan

Result columns are matched to target columns **case-insensitively** — Oracle stores
unquoted identifiers upper-cased, DuckDB preserves whatever the query produced.

| Condition | Outcome |
|---|---|
| Source column with no matching target column | **Error** — refuses to export |
| Target column that is `NOT NULL` with no default and not supplied by the query | **Error** |
| Source type with no Oracle equivalent (`INTERVAL`, `TIME`, `LIST`, `STRUCT`, `MAP`, `UNION`) | **Error**, naming the column — cast it in the query |
| Duplicate column names in the result | **Error** |
| Risky-but-legal conversion (text→number, number→date, date→text, …) | **Warning** — Oracle may succeed via implicit conversion, but it depends on NLS settings or value content |
| Target column not supplied, nullable or defaulted | Fine — left to its default |

The same plan is computed by both `validate` and `export`; validation is an early,
row-free look at the identical decision the export will make. **An export never trusts
a prior validation** — it recomputes the plan against the target it is about to write
to, because the schema can change in between.

## 7. Execution model

An export is a **tracked asynchronous run**, not a blocking request:

- `POST /api/data/{project}/export-to-database` returns an `ExecutionRunStatus`
  immediately; the frontend polls the existing `GET /api/execute/runs/{id}`.
- Progress rides in `row_count`, updated per batch from the export thread. The
  execution registry is lock-guarded, so this is safe without extra machinery.
- Abort works on two levels: `connection.cancel()` interrupts the in-flight round trip,
  and the batch loop polls `controller.is_cancelled()` between batches. Both paths land
  in the same `rollback()`.
- The run survives closing the config panel, because it lives in the run registry rather
  than in component state.

A 2-million-row Oracle load must not sit on an HTTP request, and a user who started one
by mistake must be able to stop it. Both follow from the same choice.

## 8. The per-engine boundary

There is **no universal way to push rows into an arbitrary database.** What *is*
universal is the pipeline shape: stream batches out of DuckDB, bind them as arrays,
`executemany`. What differs per engine is small and well-bounded:

| Varies per engine | Oracle | Postgres (future) |
|---|---|---|
| Bind placeholder style | `:1` | `$1` |
| Unquoted identifier folding | UPPER | lower |
| Type mapping / bind sizing | `ALL_TAB_COLUMNS` | `information_schema` |

So the split is: one shared streaming driver, and a `DatabaseExportWriter` protocol
holding the ~50 engine-specific lines. `OracleTableWriter` is the only implementation.

Options considered and rejected as the general mechanism:

- **DuckDB's `postgres`/`mysql`/`sqlite` extensions** — a genuine zero-copy
  `INSERT INTO attached.tbl SELECT …`, and the right future fast path for Postgres. But
  **there is no Oracle extension**, so it cannot be the universal answer.
- **ADBC** — no Oracle driver.
- **SQLAlchemy + `to_sql`** — genuinely portable, but not a dependency here (the project
  uses raw `oracledb` + `asyncpg`) and it surrenders control of batching and bind sizing.

Adding Postgres later means either implementing the protocol or taking the DuckDB-ATTACH
fast path, **with no change to the node config, the API, or the UI**.

### 8.1 Oracle writer specifics

- Binds are **pre-sized from the target's declared types** (`setinputsizes`) rather than
  inferred from the first batch. A narrow or all-NULL first batch otherwise mis-sizes
  the binds for every batch after it.
- Batch size 1000, used for both the DuckDB `fetchmany` chunk and the `executemany`
  array. Fetch and insert interleave, so memory stays flat regardless of result size.
- Booleans convert to `1`/`0`; Oracle has no `BOOLEAN` column type before 23c.
- Connections come from the existing `ConnectionPoolRegistry`, so thick-mode init, DSN
  construction, and pooling are inherited rather than re-implemented.
- No LOB marshalling is needed: the source is DuckDB, not an Oracle cursor, so there are
  no cross-connection LOB locators.

## 9. Relationship to the node-state model

The export node is **outside** [node-state-model.md](node-state-model.md)'s data-state
axis, on the same precedent as the Excel workbook hub:

| Aspect | Export node |
|---|---|
| `table_name` | Present but meaningless — the engine never writes it |
| Data state (Axis 3) | **None** — owns no table in either location |
| In pipeline runs | **No-op stub.** Exporting is an explicit, irreversible act; it must be something a person chooses, never a side effect of "Run pipeline" |
| Run status (Axis 1) | **Yes** — a database export is a tracked run and the card shows its badge. This is the one axis an export node participates in |
| Cache status | Excluded, as before |
| Live preview | View-only. There is no table to promote it into, so the preview panel offers no *Load to memory* / *Materialize* for an export node |

## 10. Implementation status

| Concept | Status | Notes |
|---|---|---|
| `allow_export` on global Oracle connections | ✅ Implemented | Model, sqlite column + additive `PRAGMA`/`ALTER` migration, CRUD ([pipeline_store.py](../backend/app/storage/pipeline_store.py)). Postgres rows always store 0. |
| Server-side permission enforcement | ✅ Implemented | `resolve_export_connection` re-loads and re-checks on every export and validate call. |
| In-use guard recognises export nodes | ✅ Implemented | `_node_uses_global_connection` matches both the db_source (`connection_mode`) and export (`destination`) shapes. |
| Platform Settings toggle + badge | ✅ Implemented | Design-system `Switch`, Oracle-only; "Export enabled" badge on the connection card; flag re-seeded on db-type switch. |
| Destination dropdown with approved connections | ✅ Implemented | One `<select>` for all three destinations; a revoked/missing connection still renders, flagged, rather than silently vanishing. |
| Target table parsing + validation | ✅ Implemented | `parse_target_table`; client-side shape check mirrors it, server re-validates. |
| Optional SQL query behind a toggle | ✅ Implemented | Off by default (whole upstream table); seeded with `SELECT * FROM <upstream>` on first turn-on, never overwriting an existing query. |
| Shared upstream resolution + pinned views | ✅ Implemented | `resolve_direct_upstreams` ([pipeline_graph.py](../backend/app/services/pipeline_graph.py)), `install_temp_views` / `pinned_query` ([duckdb_manager.py](../backend/app/services/duckdb_manager.py)); shared with the transform live preview. |
| Export plan (column matching + errors/warnings) | ✅ Implemented | `build_export_plan` ([database_export.py](../backend/app/services/database_export.py)). |
| `OracleTableWriter` | ✅ Implemented | Streaming `executemany`, pre-sized binds, single transaction, rollback on failure/abort. |
| Tracked async run + progress + abort | ✅ Implemented | Mirrors the materialize-preview run scaffolding; polls the existing run endpoints. |
| Preview for export nodes | ✅ Implemented | Reuses the transform live-preview session end to end; never contacts the destination. |
| Preview-and-validate split button | ✅ Implemented | Report rendered above the preview; dismissible; kept separate from the node's run result. |
| Postgres export | ⬜ Not implemented | Protocol is in place; either implement the writer or take the DuckDB-ATTACH fast path. §8 is the contract. |
| Export during a full pipeline run | ⬜ Deliberately not implemented | See §9 — exporting stays an explicit action. |
| Auto-create / replace / upsert modes | ⬜ Deliberately not implemented | See §4. Would require owning DDL in someone else's schema. |

## 11. Decisions log

Locked decisions (so they don't get relitigated):

1. **Append only.** No DDL, no `DELETE`, no `TRUNCATE`, no upsert. The target table and
   everything about it belongs to its database's administrator.
2. **The target table must already exist.** Inferring Oracle DDL from a DuckDB schema
   means guessing widths, precision, and nullability, then owning those guesses.
3. **Permission is explicit, per-connection, and Oracle-only.** Readable never implies
   writable.
4. **Revocation is never blocked by usage.** Deletion is; withdrawal is not.
5. **The server re-checks permission on every call.** Client-side filtering is UX only.
6. **One transaction, one commit, rollback on failure or abort.** A partially appended
   table is worse than a failed one.
7. **Exports read the precedence-chosen copy, never a bare table name** (§5). Shared
   with the transform preview rather than duplicated.
8. **Preview never touches the destination database.** Validation is a separate,
   explicitly chosen action.
9. **An export never trusts a prior validation** — it recomputes the plan against the
   target it is about to write.
10. **Export nodes stay no-ops in full pipeline runs.** Irreversible writes are chosen,
    not triggered as a side effect.
11. **The SQL query is optional, behind a toggle, off by default.** The common case is
    "send this table"; shaping the rows is the exception.
12. **The per-engine boundary is a writer protocol, not a portability library.** There
    is no universal push; there is a universal *shape* (§8).
