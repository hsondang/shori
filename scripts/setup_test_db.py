#!/usr/bin/env python3
"""
Setup script for the Shori test database.

Starts a Docker Postgres container (if not already running) and creates
the test schema with sample data if it doesn't already exist.

Usage:
    python scripts/setup_test_db.py           # create schema + seed (idempotent)
    python scripts/setup_test_db.py --reset   # drop everything and re-seed from scratch

Test DB connection details:
    host:     localhost
    port:     5433
    database: shori_test
    user:     shori_test
    password: shori_test
"""

import argparse
import asyncio
import subprocess
import sys
import time

import asyncpg

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "database": "shori_test",
    "user": "shori_test",
    "password": "shori_test",
}

COMPOSE_FILE = "docker-compose.test.yml"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS customers (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    email       TEXT UNIQUE NOT NULL,
    country     TEXT NOT NULL,
    created_at  DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    category    TEXT NOT NULL,
    unit_price  NUMERIC(10, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    id           SERIAL PRIMARY KEY,
    customer_id  INT NOT NULL REFERENCES customers(id),
    order_date   DATE NOT NULL,
    status       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS order_items (
    id          SERIAL PRIMARY KEY,
    order_id    INT NOT NULL REFERENCES orders(id),
    product_id  INT NOT NULL REFERENCES products(id),
    quantity    INT NOT NULL,
    unit_price  NUMERIC(10, 2) NOT NULL
);
"""

SEED_SQL = """
INSERT INTO customers (name, email, country, created_at) VALUES
    ('Alice Tanaka',    'alice@example.com',   'Japan',         '2023-01-15'),
    ('Bob Smith',       'bob@example.com',     'USA',           '2023-02-20'),
    ('Clara Müller',    'clara@example.com',   'Germany',       '2023-03-05'),
    ('David Chen',      'david@example.com',   'China',         '2023-04-10'),
    ('Eva Rossi',       'eva@example.com',     'Italy',         '2023-05-22'),
    ('Frank Lee',       'frank@example.com',   'South Korea',   '2023-06-01'),
    ('Grace Okonkwo',   'grace@example.com',   'Nigeria',       '2023-07-14'),
    ('Hiro Yamamoto',   'hiro@example.com',    'Japan',         '2023-08-30'),
    ('Isabel Santos',   'isabel@example.com',  'Brazil',        '2023-09-09'),
    ('James Brown',     'james@example.com',   'USA',           '2023-10-18')
ON CONFLICT DO NOTHING;

INSERT INTO products (name, category, unit_price) VALUES
    ('Widget A',    'Electronics',  29.99),
    ('Gadget B',    'Electronics',  49.99),
    ('Tool C',      'Hardware',     15.00),
    ('Supply D',    'Office',        8.50),
    ('Component E', 'Electronics',  99.00)
ON CONFLICT DO NOTHING;

INSERT INTO orders (customer_id, order_date, status) VALUES
    (1, '2024-01-10', 'completed'),
    (2, '2024-01-15', 'completed'),
    (3, '2024-02-01', 'shipped'),
    (4, '2024-02-14', 'completed'),
    (5, '2024-03-05', 'pending'),
    (6, '2024-03-20', 'completed'),
    (7, '2024-04-02', 'cancelled'),
    (8, '2024-04-18', 'shipped'),
    (9, '2024-05-01', 'completed'),
    (10,'2024-05-22', 'pending'),
    (1, '2024-06-10', 'completed'),
    (2, '2024-06-25', 'shipped'),
    (3, '2024-07-04', 'completed'),
    (4, '2024-07-19', 'pending'),
    (5, '2024-08-01', 'completed');

INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
    (1,  1, 2, 29.99),
    (1,  3, 1, 15.00),
    (2,  2, 1, 49.99),
    (3,  4, 5,  8.50),
    (3,  1, 1, 29.99),
    (4,  5, 1, 99.00),
    (5,  2, 2, 49.99),
    (6,  1, 3, 29.99),
    (7,  3, 2, 15.00),
    (8,  4, 10, 8.50),
    (9,  5, 2, 99.00),
    (10, 1, 1, 29.99),
    (11, 2, 1, 49.99),
    (12, 3, 4, 15.00),
    (13, 5, 1, 99.00),
    (14, 4, 3,  8.50),
    (15, 1, 2, 29.99),
    (15, 2, 1, 49.99);
"""


def run_compose_up():
    print("Starting Docker container (shori-test-postgres)...")
    result = subprocess.run(
        ["docker", "compose", "-f", COMPOSE_FILE, "up", "-d"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"docker compose failed:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)
    print("Container started.")


def is_container_running() -> bool:
    result = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", "shori-test-postgres"],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0 and result.stdout.strip() == "true"


async def wait_for_postgres(retries: int = 20, delay: float = 2.0):
    print("Waiting for Postgres to be ready...", end="", flush=True)
    for _ in range(retries):
        try:
            conn = await asyncpg.connect(**DB_CONFIG)
            await conn.close()
            print(" ready.")
            return
        except Exception:
            print(".", end="", flush=True)
            time.sleep(delay)
    print()
    print("ERROR: Postgres did not become ready in time.", file=sys.stderr)
    sys.exit(1)


async def reset_database(conn):
    """Drop all tables and sequences, then recreate and reseed."""
    print("Resetting database...")
    await conn.execute("""
        DROP TABLE IF EXISTS order_items CASCADE;
        DROP TABLE IF EXISTS orders     CASCADE;
        DROP TABLE IF EXISTS products   CASCADE;
        DROP TABLE IF EXISTS customers  CASCADE;
    """)
    print("Tables dropped.")


async def seed_database(conn):
    print("Creating schema...")
    await conn.execute(SCHEMA_SQL)
    print("Inserting test data...")
    await conn.execute(SEED_SQL)
    customers = await conn.fetchval("SELECT COUNT(*) FROM customers")
    products  = await conn.fetchval("SELECT COUNT(*) FROM products")
    orders    = await conn.fetchval("SELECT COUNT(*) FROM orders")
    items     = await conn.fetchval("SELECT COUNT(*) FROM order_items")
    print(f"Seeded: {customers} customers, {products} products, {orders} orders, {items} order_items.")


async def setup_database(reset: bool = False):
    conn = await asyncpg.connect(**DB_CONFIG)
    try:
        if reset:
            await reset_database(conn)
            await seed_database(conn)
            return

        # Idempotent path: only create/seed if not already present
        exists = await conn.fetchval(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = 'customers'"
        )
        if exists:
            print("Schema already exists — skipping schema creation.")
        else:
            await seed_database(conn)
            return

        row_count = await conn.fetchval("SELECT COUNT(*) FROM customers")
        if row_count > 0:
            print(f"Test data already present ({row_count} customers) — skipping seed.")
        else:
            print("Inserting test data...")
            await conn.execute(SEED_SQL)
            customers = await conn.fetchval("SELECT COUNT(*) FROM customers")
            products  = await conn.fetchval("SELECT COUNT(*) FROM products")
            orders    = await conn.fetchval("SELECT COUNT(*) FROM orders")
            items     = await conn.fetchval("SELECT COUNT(*) FROM order_items")
            print(f"Seeded: {customers} customers, {products} products, {orders} orders, {items} order_items.")
    finally:
        await conn.close()


async def main(reset: bool = False):
    if not is_container_running():
        run_compose_up()
    else:
        print("Container shori-test-postgres is already running.")

    await wait_for_postgres()
    await setup_database(reset=reset)

    print("\nTest database is ready.")
    print(f"  host:     {DB_CONFIG['host']}")
    print(f"  port:     {DB_CONFIG['port']}")
    print(f"  database: {DB_CONFIG['database']}")
    print(f"  user:     {DB_CONFIG['user']}")
    print(f"  password: {DB_CONFIG['password']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Setup the Shori test database.")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop all tables and re-seed from scratch (resets sequences too).",
    )
    args = parser.parse_args()
    asyncio.run(main(reset=args.reset))
