# Test Database Setup Script Design

This document explains the design and usage of [`scripts/setup_test_db.py`](../scripts/setup_test_db.py). It is the canonical documentation for how the project prepares the PostgreSQL database used by backend integration tests.

## Purpose

`setup_test_db.py` bootstraps the PostgreSQL test environment used by the backend integration tests. It is a manual helper script: it prepares the database and sample data, but it is not invoked automatically by `pytest`.

## What It Does

- Starts the Docker container defined in [`docker-compose.test.yml`](../docker-compose.test.yml) if the test database container is not already running.
- Waits until PostgreSQL on `localhost:5433` accepts connections.
- Creates the test schema if it does not already exist.
- Seeds sample data if it is missing.
- Supports a `--reset` mode that drops the seeded tables, recreates the schema, and reloads the sample data.

## What It Does Not Do

- It does not run the test suite.
- It does not stop or remove the Docker container automatically.
- It does not modify production databases or the application's normal runtime data.

## Dependencies And Inputs

- `docker compose` is used to start the container from `docker-compose.test.yml`.
- `asyncpg` is used to connect to PostgreSQL and execute schema/seed SQL.
- The script uses a fixed connection configuration:
  - host: `localhost`
  - port: `5433`
  - database: `shori_test`
  - user: `shori_test`
  - password: `shori_test`
- The expected Docker container name is `shori-test-postgres`.

## Execution Flow

The script entrypoint is `main(reset: bool = False)`.

1. `main()` checks whether `shori-test-postgres` is already running by calling `is_container_running()`.
2. If the container is not running, `run_compose_up()` executes `docker compose -f docker-compose.test.yml up -d`.
3. `wait_for_postgres()` repeatedly tries to connect with `asyncpg` until PostgreSQL is ready or the retry limit is reached.
4. `setup_database(reset=reset)` connects to the test database and prepares the schema/data.
5. If `--reset` was provided, `reset_database()` drops `order_items`, `orders`, `products`, and `customers`, then `seed_database()` recreates the schema and inserts sample data.
6. Without `--reset`, the script follows an idempotent path:
   - If the `customers` table does not exist, `seed_database()` creates all tables and inserts seed data.
   - If the schema already exists, the script checks whether `customers` already contains rows.
   - If rows exist, it skips reseeding.
   - If the schema exists but the table is empty, it inserts the sample data.
7. After setup completes, the script prints the connection details for the ready test database.

## Schema And Seed Data

The script defines both schema creation SQL and seed SQL in the file itself.

Tables created:

- `customers`
- `products`
- `orders`
- `order_items`

The seed data loads a small fixed dataset across those tables so integration tests can run against predictable PostgreSQL data.

## How It Relates To Tests

The backend integration tests depend on this database being available and seeded. In particular, [`backend/tests/test_services/test_postgres_integration.py`](../backend/tests/test_services/test_postgres_integration.py) connects to the PostgreSQL instance on port `5433` and runs real queries against the seeded tables.

Because `pytest` only marks these tests as integration tests, you must run the setup script yourself before executing:

```bash
cd backend
python -m pytest tests/ -v -m integration
```

## Usage

Start or prepare the test database:

```bash
cd backend
source .venv/bin/activate
cd ..
python scripts/setup_test_db.py
```

Reset and re-seed the test database:

```bash
cd backend
source .venv/bin/activate
cd ..
python scripts/setup_test_db.py --reset
```

Stop the test database container:

```bash
docker compose -f docker-compose.test.yml down
```

## Failure Modes

- If `docker compose` fails to start the container, the script prints the Docker error output and exits with a non-zero status.
- If PostgreSQL does not become ready before the retry limit is reached, the script exits with an error.
- If the database connection, schema creation, or seed SQL fails, the exception from `asyncpg` propagates and the script stops.
