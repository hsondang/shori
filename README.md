# Shori

A visual data pipeline builder for data wrangling. Construct pipelines as node graphs, write SQL transformations, and preview results — all in the browser.

## Prerequisites

- **Python 3.11+**
- **uv**
- **Node.js 18+**
- **Docker** (optional, for the test PostgreSQL database)

## Project Structure

```
shori/
├── backend/          # FastAPI + DuckDB + asyncpg + oracledb
├── frontend/         # React + React Flow + Monaco Editor
├── scripts/          # Setup scripts (test database)
├── data/             # Runtime data (uploads, exports, pipelines)
└── docker-compose.test.yml
```

## Setup

### Backend

```bash
cd backend
uv venv .venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

Oracle connections use `python-oracledb` Thick mode. Install Oracle Instant Client and
set `ORACLE_CLIENT_LIB_DIR` before starting the backend. If you use `tnsnames.ora` or
`sqlnet.ora`, also set `ORACLE_CLIENT_CONFIG_DIR`.

```bash
export ORACLE_CLIENT_LIB_DIR=/path/to/instantclient
export ORACLE_CLIENT_CONFIG_DIR=/path/to/network/admin  # optional
```

### Frontend

```bash
cd frontend
npm install
```

### Test Database (optional)

Spins up a PostgreSQL 16 container on port **5433** with sample data (customers, products, orders):

```bash
cd backend
source .venv/bin/activate
cd ..
python scripts/setup_test_db.py
```

Connection details for the test database:

| Field    | Value        |
|----------|--------------|
| Host     | `localhost`  |
| Port     | `5433`       |
| Database | `shori_test` |
| User     | `shori_test` |
| Password | `shori_test` |

## Running

Start both servers in separate terminals:

**Terminal 1 — Backend** (runs on port 8000):

```bash
cd backend
source .venv/bin/activate
uvicorn app.main:app --reload --port 8000
```

**Terminal 2 — Frontend** (runs on port 5173, proxies API calls to backend):

```bash
cd frontend
npm run dev
```

Open **http://localhost:5173** in your browser.

## Usage

1. **Drag nodes** from the toolbar onto the canvas: CSV Source, PostgreSQL Source, Oracle Source, Transform, Export
2. **Connect nodes** by dragging from an output handle (right) to an input handle (left)
3. **Click a node** to configure it in the right panel:
   - **CSV Source** — upload a CSV file
   - **PostgreSQL / Oracle Source** — enter connection details, write a SQL query, and test the connection
   - **Transform** — write SQL referencing upstream node table names (shown as purple badges above the editor)
   - **Export** — download a node's data as CSV
4. **Execute** the pipeline with the toolbar button. Use **Force Refresh** to re-run from scratch (bypasses cache)
5. **Preview data** by clicking "Preview data" on any executed node — results appear in the bottom panel with pagination

Each node's result is registered as a named table in DuckDB. Transform nodes reference these names directly in SQL, e.g.:

```sql
SELECT c.name, COUNT(o.id) AS order_count
FROM customers c
JOIN orders o ON o.customer_id = c.id
GROUP BY c.name
```

Pipelines can be saved and loaded from the toolbar.

## Testing

### Stack

| Layer | Framework |
|-------|-----------|
| Backend unit & integration | pytest + pytest-asyncio + httpx |
| Frontend unit & component | Vitest + Testing Library |

---

### Backend tests

**Install test dependencies** (separate from production requirements):

```bash
cd backend
source .venv/bin/activate
uv pip install -r requirements-test.txt
```

**Run all unit tests:**

```bash
cd backend
python -m pytest tests/ -v
```

**Run integration tests** (requires the Docker Postgres container — start it first with `python scripts/setup_test_db.py`):

```bash
python -m pytest tests/ -v -m integration
```

#### What is tested

| File | What it covers |
|------|---------------|
| `tests/test_routers/test_pipelines.py` | Pipeline CRUD endpoints: list, create, get, update (id override), delete, 404 handling |
| `tests/test_routers/test_data.py` | Data preview (pagination, offset beyond total), schema inspection, CSV export, 404s |
| `tests/test_routers/test_upload.py` | CSV upload (success, non-CSV rejection), Postgres connection test endpoint (mocked) |
| `tests/test_routers/test_execution.py` | Single-node execution, inline pipeline, stored pipeline, transform pipelines, force-refresh, cycle detection |
| `tests/test_services/test_duckdb_manager.py` | CSV registration, DataFrame registration, SQL transforms, table existence, drop, pagination, CSV export |
| `tests/test_services/test_pipeline_engine.py` | Topological sort (linear, diamond, single node, cycle, disconnected), all node types, result caching, force-refresh, execution timing |
| `tests/test_services/test_postgres_service.py` | DataFrame output, empty results, connection cleanup on success and error (all with mocked asyncpg) |
| `tests/test_services/test_postgres_integration.py` | Real queries against the Docker test DB: full result set, connection test, invalid query |
| `tests/test_storage/test_pipeline_store.py` | Save/load roundtrip, FileNotFoundError, list all, delete, delete nonexistent |

---

### Frontend tests

**Install dependencies** (included in `npm install`):

```bash
cd frontend
npm install
```

**Run all tests:**

```bash
cd frontend
npm test
```

#### What is tested

| File | What it covers |
|------|---------------|
| `src/store/pipelineStore.test.ts` | `addNode` default configs for all 5 node types, `updateNodeData` patching, `deleteNode` with edge cascade, `onConnect` edge creation, `newPipeline` reset |
| `src/components/flow/NodeStatusBadge.test.tsx` | All 4 status colours, row count rendering, locale-formatted numbers, rounded execution time, error message display |
| `src/components/panels/DataPreviewPanel.test.tsx` | Column headers and types, row data, NULL rendering, Prev/Next pagination button states, page counter text, loadPreview called with correct offset |
| `src/components/panels/ConnectionForm.test.tsx` | Postgres/Oracle field labels, default port values, onChange propagation, test-connection success and error display, correct API function called per dbType |
| `src/components/flow/nodes/CsvSourceNode.test.tsx` | Filename display, table name, preview button visibility (success only), source handle position |
| `src/components/flow/nodes/PostgresSourceNode.test.tsx` | Connection string format (`host:port/database`), table name, source handle |
| `src/components/flow/nodes/TransformNode.test.tsx` | SQL preview, 50-char truncation with ellipsis, "No SQL defined" fallback, source and target handles |
| `src/components/flow/nodes/ExportNode.test.tsx` | No download button without connected source, source table name display, download button when connected, target handle |
