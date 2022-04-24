import snowflake.connector

from .sql import SqlInput


class SnowflakeInput(SqlInput):
    """
    Very simple PostgreSQL input adapter
    """

    def __init__(
        self, *, username, password, account, database, schema=None, where_clause=None
    ):
        conn = snowflake.connector.connect(
            user=username,
            password=password,
            account=account,
            database=database,
            schema=schema,
        )
        super().__init__(conn, schema=None, where_clause=where_clause)
