import asyncpg
import pandas as pd


class PostgresService:
    async def connect(self, config: dict) -> asyncpg.Connection:
        conn_config = config["connection"]
        return await asyncpg.connect(
            host=conn_config["host"],
            port=conn_config["port"],
            database=conn_config["database"],
            user=conn_config["user"],
            password=conn_config["password"],
        )

    async def fetch_query(self, connection: asyncpg.Connection, query: str) -> pd.DataFrame:
        rows = await connection.fetch(query)
        if not rows:
            return pd.DataFrame()
        columns = list(rows[0].keys())
        data = [list(row.values()) for row in rows]
        return pd.DataFrame(data, columns=columns)

    async def execute_query(self, config: dict) -> pd.DataFrame:
        connection = await self.connect(config)
        try:
            return await self.fetch_query(connection, config["query"])
        finally:
            await connection.close()

    def abort_query(self, connection: asyncpg.Connection) -> None:
        if not connection.is_closed():
            connection.terminate()

    async def test_connection(self, config: dict) -> bool:
        connection = await asyncpg.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"],
        )
        await connection.close()
        return True
