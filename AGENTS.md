# AGENTS.md

Guide for AI coding agents working in this repository. For human-oriented setup commands and a usage walkthrough, see [README.md](README.md) — this file is intentionally complementary and does not duplicate those sections.

## Purpose

Shori is a visual data pipeline builder for data wrangling. Users construct pipelines as node graphs in the browser — CSV / PostgreSQL / Oracle sources feed SQL transforms feed exports. The backend executes the graph in an in-memory DuckDB instance and streams per-node status back to the UI.

## Tech Stack

- **Backend** — Python 3.11+, FastAPI, DuckDB (in-memory), `asyncpg` (Postgres), `oracledb` Thick mode, pandas
- **Frontend** — React 19, TypeScript (strict), Vite 7, `@xyflow/react` (React Flow), Zustand, Tailwind 4, Monaco editor, TanStack Table, axios, react-router-dom v7
- **Tests** — pytest + pytest-asyncio + httpx (backend); Vitest + Testing Library + jsdom (frontend)
- **Storage** — SQLite at `data/projects.sqlite3` for pipeline definitions and global DB connections; filesystem for uploads/exports

## Environment

- Python versions are managed with **pyenv**. The repo does not pin a version via `.python-version`; the project requires 3.11+.
- Virtualenv and dependency install use **uv** (`uv venv .venv`, `uv pip install -r requirements.txt`). Exact commands are in [README.md](README.md).
- Oracle Thick mode needs `ORACLE_CLIENT_LIB_DIR` set before the backend starts; optionally `ORACLE_CLIENT_CONFIG_DIR` for `tnsnames.ora`.
- Integration tests need the Docker Postgres container from `docker-compose.test.yml` (port 5433). See [docs/setup-test-db-design.md](docs/setup-test-db-design.md).

## Repository Layout

```
shori/
├── backend/                        FastAPI app
│   ├── app/
│   │   ├── main.py                 Entry: lifespan wires services into app.state
│   │   ├── config.py               Runtime paths (DATA_DIR, UPLOAD_DIR, …)
│   │   ├── models/pipeline.py      All Pydantic models
│   │   ├── routers/                pipelines, execution, data, settings, upload
│   │   ├── services/               6 services (see Backend Architecture)
│   │   └── storage/pipeline_store.py   SQLite-backed pipeline + connection persistence
│   ├── tests/                      pytest; @pytest.mark.integration for Postgres-dependent
│   ├── requirements.txt
│   ├── requirements-test.txt
│   └── pyproject.toml              Pytest config only (asyncio_mode=auto, integration marker)
├── frontend/                       React + Vite app
│   └── src/
│       ├── main.tsx, App.tsx
│       ├── api/client.ts           All backend HTTP calls (single source of truth)
│       ├── store/                  Zustand: pipelineStore.ts, settingsStore.ts
│       ├── components/             flow/, panels/, toolbar/, projects/, settings/, connections/
│       ├── lib/                    Pure helpers: executionTiming, csvPreprocessing, dragData, …
│       └── types/pipeline.ts       TS mirrors of backend Pydantic models
├── docs/setup-test-db-design.md
├── scripts/setup_test_db.py
├── data/                           Runtime: uploads/, exports/, pipelines/, projects.sqlite3
└── docker-compose.test.yml
```

## Backend Architecture

- **Entry**: `app.main:app` (uvicorn target). The FastAPI lifespan constructs `DuckDBManager`, `CsvPreprocessArtifactStore`, `ExecutionRegistry`, and `PipelineStore`, and attaches them to `app.state`. Routers reach services via `request.app.state`, never via globals.
- **Services** ([backend/app/services/](backend/app/services/)):
  - `duckdb_manager.py` — In-memory DuckDB. Registers CSVs and DataFrames as named tables, runs SQL transforms, exports CSVs. Thread-safe via `threading.Lock`.
  - `pipeline_engine.py` — Topologically sorts a `PipelineDefinition`, executes nodes in dependency order, treats existing DuckDB tables as cache (force-refresh bypasses), emits `on_node_start` / `on_node_finish` / `on_node_update` callbacks.
  - `execution_registry.py` — Tracks active `ExecutionRun`s, holds `ExecutionController`s for cancellation, evicts entries after a retention window (~15 min).
  - `postgres_service.py` — `asyncpg` queries returning DataFrames; mocked in unit tests.
  - `oracle_service.py` — `oracledb` Thick mode; supports fetchall vs fetchmany (`arraysize`, `prefetchrows`).
  - `csv_service.py` — Upload handling plus optional bash/python preprocessing (60s timeout) with artifact fingerprinting to skip rework.
- **Routers** ([backend/app/routers/](backend/app/routers/)), all mounted under `/api`:
  - `pipelines.py` — CRUD + star
  - `execution.py` — Async run lifecycle: start, poll `runs/{id}`, abort
  - `data.py` — Table preview, CSV-source preview (raw and preprocessed), CSV export
  - `settings.py` — Global database connections CRUD
  - `upload.py` — CSV upload, Postgres/Oracle connection-test endpoints
- **Models** ([backend/app/models/pipeline.py](backend/app/models/pipeline.py)): `NodeType` (CSV_SOURCE, DB_SOURCE, TRANSFORM, EXPORT), `NodeStatus`, `NodeDefinition`, `PipelineDefinition`, `NodeExecutionResult`, `ExecutionRunStatus`, plus `SavedPostgresConnection` / `SavedOracleConnection` (discriminated by `db_type`).

## Frontend Architecture

- **Entry**: [src/main.tsx](frontend/src/main.tsx) → [App.tsx](frontend/src/App.tsx) with `react-router-dom`.
- **State** — Zustand store at [src/store/pipelineStore.ts](frontend/src/store/pipelineStore.ts): React Flow `nodes`/`edges`, pipeline metadata, `nodeResults`, `activeExecutions`, `executionClockNow` (drives live elapsed timers), preview tabs, and node-editor draft.
- **API client** — [src/api/client.ts](frontend/src/api/client.ts) is an axios instance with base URL `/api` (Vite dev server proxies to backend on port 8000). It exports `executePipeline`, `startPipelineExecution`, `getExecutionRunStatus`, `abortExecutionRun`, `previewData`, `previewCsvSource`, `uploadCsv`, plus settings/connection endpoints. Treat this file as the source of truth for backend HTTP shapes.
- **Components** ([frontend/src/components/](frontend/src/components/)):
  - `flow/` — React Flow canvas plus per-type node components (`CsvSourceNode`, `DatabaseSourceNode`, `TransformNode`, `ExportNode`) and `NodeStatusBadge`.
  - `panels/` — `NodeConfigPanel`, `DataPreviewPanel`, `ConnectionForm`, Monaco-based SQL editor.
  - `toolbar/` — Execute / Force Refresh / Save / Load.
  - `projects/`, `settings/`, `connections/` — project browser, global settings page, connection modal.
- **Types** — [src/types/pipeline.ts](frontend/src/types/pipeline.ts) holds hand-maintained TS interfaces that mirror the backend Pydantic models. Keep these in sync when backend models change.

## End-to-End Pipeline Execution Flow

1. User edits the graph → Zustand store updates `nodes` / `edges`.
2. Click Run → `client.startPipelineExecution(pipeline, force?)` → `POST /api/execute/pipeline/start`.
3. The router creates an `ExecutionRun` + `ExecutionController` and spawns an async task that calls `PipelineEngine.execute`.
4. The engine topologically sorts the nodes; for each node it emits `on_node_start`, runs the node (CSV load / DB query / DuckDB SQL / CSV export), registers the result as a DuckDB table, and emits `on_node_finish`.
5. The frontend polls `GET /api/execute/runs/{id}` and merges status into `nodeResults`.
6. Preview requests hit `GET /api/data/preview/{table_name}`, which pages rows out of DuckDB.

## Conventions

- **Backend** — `snake_case`. Pydantic `BaseModel` for every request/response. Raise `HTTPException` at API boundaries. Access services through `request.app.state`. Guard shared mutable state with `threading.Lock`. Router handlers are `async`. Don't mutate Pydantic models in place — use `model_copy(update={…})`.
- **Frontend** — `camelCase`. One component per file. TypeScript strict mode. Read from the Zustand store with selector hooks (`usePipelineStore((s) => s.nodes)`) rather than the whole store. Discriminated unions (`db_type`) handle the Postgres/Oracle variants of `DatabaseSourceConfig`.
- **Tests** — Backend uses `tmp_path` plus monkeypatching of `app.config` constants to isolate data directories per test; `@pytest.mark.integration` marks tests requiring the Docker Postgres container. Frontend uses Vitest, Testing Library, and `@testing-library/user-event`.
- **Paths** — Always reference runtime files through `app.config` constants. Never hardcode `data/` paths.

## Where to Look Next

- Setup commands and usage walkthrough → [README.md](README.md)
- Test database design (Docker setup, schema, seed data) → [docs/setup-test-db-design.md](docs/setup-test-db-design.md)
- Pipeline data shapes → [backend/app/models/pipeline.py](backend/app/models/pipeline.py)
- Full HTTP endpoint surface → [frontend/src/api/client.ts](frontend/src/api/client.ts)
