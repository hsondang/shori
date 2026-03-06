import asyncpg
import pandas as pd


class PostgresService:
    async def execute_query(self, config: dict) -> pd.DataFrame:
        conn_config = config["connection"]
        connection = await asyncpg.connect(
            host=conn_config["host"],
            port=conn_config["port"],
            database=conn_config["database"],
            user=conn_config["user"],
            password=conn_config["password"],
        )
        try:
            rows = await connection.fetch(config["query"])
            if not rows:
                return pd.DataFrame()
            columns = list(rows[0].keys())
            data = [list(row.values()) for row in rows]
            return pd.DataFrame(data, columns=columns)
        finally:
            await connection.close()

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
