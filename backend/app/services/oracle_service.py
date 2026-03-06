import oracledb
import pandas as pd


class OracleService:
    async def execute_query(self, config: dict) -> pd.DataFrame:
        conn_config = config["connection"]
        dsn = oracledb.makedsn(
            conn_config["host"],
            conn_config["port"],
            service_name=conn_config["service_name"],
        )
        connection = await oracledb.connect_async(
            user=conn_config["user"],
            password=conn_config["password"],
            dsn=dsn,
        )
        try:
            cursor = connection.cursor()
            await cursor.execute(config["query"])
            columns = [col[0] for col in cursor.description]
            rows = await cursor.fetchall()
            return pd.DataFrame(rows, columns=columns)
        finally:
            await connection.close()

    async def test_connection(self, config: dict) -> bool:
        dsn = oracledb.makedsn(
            config["host"],
            config["port"],
            service_name=config["service_name"],
        )
        connection = await oracledb.connect_async(
            user=config["user"],
            password=config["password"],
            dsn=dsn,
        )
        await connection.close()
        return True
