# Persistent Per-Project DuckDB Storage Layer

## Context

Today the backend holds a single **in-memory** DuckDB behind one connection and a global `threading.Lock` ([duckdb_manager.py](backend/app/services/duckdb_manager.py)): nothing survives a restart, all reads/writes are serialized, and the pipeline engine runs nodes strictly one-at-a-time. The "skip if cached" check (`table_exists` + a per-request `node_results` dict) can never trigger across requests and would serve stale/partial data if it did.

This feature makes DuckDB the persistent OLAP layer of the platform:
- **One `.duckdb` file per project** — isolation, easy lifecycle, no cross-project interference.
- **Direct concurrent writes** (Design A): each extraction streams chunks into its own table via its own cursor on the project file; DuckDB's in-process MVCC handles concurrency. Atomicity via `<table>__staging` + rename-swap in one transaction.
- **Real cache keys** (Merkle-style fingerprints) + a metadata table inside each project `.duckdb` — nodes only rerun when their config or any upstream changed; cached data is valid until the user explicitly refreshes.
- **Concurrent extraction**: ready-set DAG scheduler + `oracledb.create_pool` per saved connection + Arrow zero-copy ingestion (verified: `fetch_df_batches` works in **thick mode**, per python-oracledb dataframes docs) + DuckDB `postgres` extension for Postgres materialization.
- **DBeaver-style preview**: preview a db_source query 200 rows at a time without creating a table; optionally materialize mid-preview (buffered rows stream into the table, cursor continues) or materialize directly.
- **Per-project advanced settings** with sensible defaults.

Baseline: branch off latest `main` (5b96c73). Note: main already contains the oracle fetch-config work, Excel source, thick-mode enforcement, asyncpg, and cancellation — `pr-oracle-fetch-config` is an older cut, reference only.

## Locked decisions (from user)

1. One DuckDB per project; all Decision-1 benefits implemented (lifecycle, namespacing, no cross-project interference, bloat/compaction).
2. Design A concurrent writes with staging + transactional swap.
3. Cache keys replace `table_exists`; metadata table lives **in the project `.duckdb`** (not sqlite); every table mutation keeps metadata in sync.
4. Concurrent scheduler + `oracledb.create_pool`; Arrow batches must work in **thick mode** (verified); Postgres via `ATTACH` + CTAS.
5. Per-connection cap, memory_limit, etc. in a per-project **advanced settings** section with defaults.
6. Cached extraction valid until explicit refresh; saved node changes show a **stale** UI indicator.
7. Node tables for deleted nodes dropped **eagerly**; cross-project querying deferred.
8. Preview = in-memory, 200 rows per scroll-fetch, no table created; "Materialize" from preview reuses already-fetched rows then continues; "Materialize" without preview also available.

## Architecture

```
data/
  projects.sqlite3                  # unchanged: project/pipeline JSON, global connections
  projects/<project_id>/
    project.duckdb                  # node tables + _shori_node_meta
    tmp/                            # duckdb temp_directory (spill)
backend/app/services/
  duckdb_manager.py                 # rewritten: file-backed, cursor-per-op, staging writer, metadata
  project_db_registry.py            # NEW: project_id -> DuckDBManager, LRU, lifecycle
  cache_keys.py                     # NEW: fingerprint computation
  connection_pools.py               # NEW: oracle/asyncpg pools per connection identity
  preview_sessions.py               # NEW: DBeaver-style live preview sessions
  pipeline_engine.py                # rewritten scheduler (ready-set, TaskGroup, semaphores)
```

Reserved table prefix: `_shori_` (metadata) and suffix `__staging` (in-flight loads); both excluded from user-visible listings and rejected as node `table_name`s.

### Metadata table (in each project.duckdb)

`_shori_node_meta(node_id PK, table_name, cache_key, status ('loading'|'complete'|'failed'), row_count, column_count, columns_json, error, started_at, finished_at, duration_ms)`

Kept in sync on **every** mutation: load start (loading) → swap commit (complete, inside the same transaction as the rename) → failure (failed); node deleted → row+table dropped; `table_name` renamed → `ALTER TABLE RENAME` + row update (cache preserved — table name is not part of the cache key); manual table delete endpoint → row deleted; project deleted → file deleted; compact → rewrite preserves table+meta. On opening a project file: drop leftover `*__staging` tables and flip stuck `loading` rows to `failed`.

### Cache keys

`cache_key(node) = sha256(node.type + canonical(config subset) + sorted(upstream cache_keys))`
- **db_source**: db_type, connection identity (host, port, service_name/database, user — **never password**), query text. `fetch_config` is **excluded** (changes how data is fetched, not what it is).
- **csv_source / excel_source**: file_path, original_filename, selected_sheet, normalized preprocessing config (reuse `preprocessing_fingerprint` from [csv_service.py](backend/app/services/csv_service.py)).
- **transform**: sql + upstream keys (Merkle propagation — editing a source automatically stales all descendants).
- **export**: not cached (no table).

Skip rule in engine: rerun iff key mismatch OR meta.status != complete OR table missing OR force. Skipped nodes return a `NodeExecutionResult` rebuilt from metadata with new field `cached: true`.

## Implementation phases

### Phase 1 — Storage core (backend)
- `config.py`: add `PROJECTS_DIR = DATA_DIR / "projects"`, helper for per-project paths.
- Rewrite `DuckDBManager(path, settings)`: persistent `duckdb.connect(path)`; **every operation runs on `self.conn.cursor()`** (thread-safe, GIL released) — delete the global lock; apply `memory_limit` + `temp_directory` pragmas; keep `_quote_identifier` / `_json_safe_value` / preview-fallback-to-VARCHAR as-is.
- New `StagingLoad` API: `begin_load(node_id, table_name, cache_key)` → object with `append_arrow(batch)` / `append_df(df)` (first append creates `__staging` via CTAS, marks meta `loading`), `commit()` (one transaction: drop real table, rename staging, upsert meta `complete`), `abort(error)` (drop staging, meta `failed`). All node-producing paths (db, csv, excel, transform) go through it.
- `compact()`: `ATTACH` fresh file + `COPY FROM DATABASE` + close + atomic file swap; `storage_info()` returns file size.
- New `ProjectDuckDBRegistry`: `get(project_id)` lazy-open with startup cleanup, LRU cap on open files (8), `close_and_delete(project_id)`, `close_all()`; replaces `app.state.duckdb` in [main.py](backend/app/main.py) lifespan.

### Phase 2 — Cache keys + metadata + stale status
- New `cache_keys.py` with per-type composition above; unit-tested heavily.
- Engine consults `_shori_node_meta` instead of `table_exists`/`node_results` (delete the dead dict).
- New endpoint `POST /api/execute/cache-status` (body: pipeline definition) → per node `{state: fresh|stale|missing, row_count, column_count, finished_at}`. Frontend calls it after pipeline load/save and after each run completes.
- `NodeExecutionResult.cached: bool = False` added in [models/pipeline.py](backend/app/models/pipeline.py) and TS types.

### Phase 3 — Concurrent scheduler + connection pools
- Rewrite `execute_pipeline` in [pipeline_engine.py](backend/app/services/pipeline_engine.py): keep `topological_sort` for cycle validation; execution becomes ready-set scheduling — launch all in-degree-0 nodes as tasks in an `asyncio.TaskGroup`, decrement children on completion, start them as they hit 0. Global `asyncio.Semaphore(settings.max_concurrent_nodes)`.
- Failure policy: failed/cancelled node → its descendants get status `cancelled` with error "Upstream node failed"; independent branches keep running.
- New `connection_pools.py` (app-level, created in lifespan, closed on shutdown): Oracle `oracledb.create_pool(min=0, max=settings.max_connections_per_database)` keyed by connection identity; Postgres `asyncpg.create_pool` likewise. The pool max **is** the per-connection cap; engine + preview sessions acquire/release from the same pools.
- [execution_registry.py](backend/app/services/execution_registry.py): abort callbacks become per-(execution, node) so cancelling a run aborts all in-flight queries; transforms register a DuckDB `interrupt()` callback.
- Single-node endpoint `POST /api/execute/node/start` gains `project_id` in the request body (frontend already knows `pipelineId`, which **is** the project id).

### Phase 4 — Extraction paths
- Add `pyarrow` to [requirements.txt](backend/requirements.txt).
- **Oracle**: replace pandas-chunk loading in [oracle_service.py](backend/app/services/oracle_service.py) with `connection.fetch_df_batches(sql, size=arraysize)` → each `OracleDataFrame` batch ingested zero-copy by DuckDB (register via Arrow PyCapsule, `INSERT INTO staging SELECT * FROM batch`). Works in thick mode (verified). Keep existing `fetch_config` semantics: `fetchall` → `fetch_df_all`, `fetchmany` → batched. Fallback to the current row-based path if Arrow fetch raises on exotic types.
- **Postgres direct materialization**: DuckDB `postgres` extension — `ATTACH (TYPE postgres, READ_ONLY)` + `CREATE TABLE staging AS FROM postgres_query(...)`, so the data never passes through Python. Graceful fallback to the asyncpg path if the extension can't be installed (offline).
- CSV / Excel / Transform paths rewired through `begin_load`/`commit` (transform = `INSERT INTO staging (sql)` CTAS).

### Phase 5 — Preview sessions (DBeaver-style)
- New `preview_sessions.py` + `app.state.preview_sessions`: a session holds a pooled connection + open cursor + in-memory buffer of all fetched batches.
  - `POST /api/data/preview-session/start` (project_id, node config) → executes query, returns `session_id`, columns, first `preview_chunk_rows` (200) rows, `has_more`.
  - `POST .../{id}/fetch` → next 200 rows (frontend calls on scroll-to-bottom).
  - `POST .../{id}/materialize` → registers an execution run (node badge shows running), streams buffered rows into `__staging`, continues draining the **same cursor** to completion, swap + metadata; session closes. Works for both Oracle and Postgres (Postgres sessions use an asyncpg server-side cursor inside a transaction).
  - `DELETE .../{id}` → close/release.
  - Idle TTL reaper (`preview_session_ttl_seconds`, default 600s); buffer cap `preview_max_buffer_rows` (default 10,000) — past the cap scrolling stops with "Preview limited to N rows — materialize to load everything" (materialize still works).
- "Materialize without preview" = existing single-node execution (no new path needed).

### Phase 6 — Project lifecycle & data endpoints
- `DELETE /api/pipelines/{id}` in [pipelines.py](backend/app/routers/pipelines.py) → also `registry.close_and_delete(project_id)` (removes the whole `data/projects/<id>/` dir).
- `POST`/`PUT` pipeline save → **reconcile**: drop tables+meta for nodes no longer in the definition (the eager-drop semantics — deletion takes effect when the pipeline is saved); `table_name` changes become `ALTER TABLE RENAME` preserving cache.
- [data.py](backend/app/routers/data.py) endpoints become project-scoped: `/api/data/{project_id}/preview/{table}`, `/schema/`, `/export/`, `/table/` (delete keeps meta in sync). Frontend client updated to pass `pipelineId`.
- New: `POST /api/pipelines/{id}/compact` and `GET /api/pipelines/{id}/storage` (file size) for the settings UI.
- Lifespan guard: refuse to start with `uvicorn --workers > 1` (web concurrency env check) — the design assumes a single writer process.

### Phase 7 — Per-project advanced settings
- `ProjectSettings` pydantic model on `PipelineDefinition.settings` (default factory, backward-compatible with existing saved JSON):
  - `max_concurrent_nodes: 4`, `max_connections_per_database: 2`, `duckdb_memory_limit: "2GB"`, `preview_chunk_rows: 200`, `preview_max_buffer_rows: 10000`, `preview_session_ttl_seconds: 600`.
- Applied when opening the project DB, building pools, scheduling, and previewing.

### Phase 8 — Frontend
- **Types/API** ([types/pipeline.ts](frontend/src/types/pipeline.ts), [api/client.ts](frontend/src/api/client.ts)): `ProjectSettings`, cache-status response, preview-session endpoints, `cached` flag, project-scoped data calls.
- **Store** ([pipelineStore.ts](frontend/src/store/pipelineStore.ts)): `cacheStatusByNodeId` refreshed after load/save/run; preview-session state per node tab; settings in pipeline snapshot (counts toward unsaved-changes).
- **Stale badge**: extend [NodeStatusBadge.tsx](frontend/src/components/flow/NodeStatusBadge.tsx) with `Cached` / `Stale` chip ("node changed since last run") driven by cache-status; per-node **Refresh** action = force single-node run.
- **DataPreviewPanel** ([DataPreviewPanel.tsx](frontend/src/components/panels/DataPreviewPanel.tsx)): new "live preview" tab kind with infinite scroll (scroll-sentinel fetching 200-row pages; TanStack Table is already in deps if virtualization is needed); `Materialize` button on the tab; the materialized-table preview switches from Prev/Next to the same infinite-scroll grid (same offset/limit endpoint underneath).
- **db_source node actions**: `Preview` (starts session, no table) and `Materialize` (single-node run) in node UI / [NodeEditorModal.tsx](frontend/src/components/panels/NodeEditorModal.tsx).
- **ProjectSettingsModal** (new, opened from Toolbar): advanced settings form + storage size + Compact button.

### Phase 9 — Tests
- Backend (pytest, mirror existing `tests/test_services` style): staging swap atomicity (abort mid-load leaves old table intact + meta not `complete`); metadata state transitions; cache-key unit tests (config change, upstream Merkle propagation, password/fetch_config exclusion); registry LRU + delete; scheduler (independent nodes run concurrently — assert overlap with instrumented fake services; dependents wait; failure cancels descendants only); preview session lifecycle incl. TTL + materialize-from-buffer; save-reconcile drops/renames.
- Frontend (vitest): store cache-status handling, preview-session tab reducer logic.

## Decisions I made (flagging for review)

- **Eager drop = on pipeline save** (reconcile), not on canvas delete — deleting a node then discarding changes must not destroy data.
- `fetch_config` excluded from cache keys (doesn't change result content).
- `table_name` rename preserves cached data via `ALTER TABLE RENAME`.
- Materialize-from-preview continues the open cursor for both DBs; the Postgres `ATTACH` fast path is used only for materialize-without-preview.
- Preview buffer is capped (default 10k rows) rather than spilling to disk; materialize works regardless.
- Reserved prefixes `_shori_` / `__staging` rejected as user table names.

## Verification

1. `cd backend && pytest` — all new + existing tests green.
2. Manual end-to-end (backend `uvicorn`, frontend `npm run dev`; postgres via `docker-compose.test.yml`):
   - Create project → run pipeline → `data/projects/<id>/project.duckdb` exists; restart backend → nodes show **Cached**, run completes instantly without touching sources.
   - Edit a source node's SQL, save → that node + all descendants show **Stale**; run → only stale nodes execute.
   - Two independent db_source nodes → run pipeline → wall-clock ≈ max(query times), not sum; statuses update concurrently.
   - `kill -9` backend mid-extraction → restart → node shows stale/failed (not cached), no `__staging` junk visible, old table version (if any) still queryable.
   - Preview a db_source: scroll loads 200-row pages; Materialize mid-scroll → table appears with full row count; Materialize without preview works.
   - Delete a node + save → its table gone from the file; delete project → directory gone; Compact after dropping a large table → file shrinks.
3. `cd frontend && npm test` (vitest) and `npm run build` for type safety.
