from psycopg2 import connect

from .sql import SqlInput, SqlOutput


class PgSqlInput(SqlInput):
    def __init__(
        self,
        *,
        username,
        password,
        host,
        port,
        database,
        schema=None,
        where_clause=None
    ):
        connection = connect(
            user=username, password=password, host=host, port=port, dbname=database
        )
        super().__init__(connection, schema=schema, where_clause=where_clause)


class PgSqlOutput(SqlOutput):
    def __init__(self, *, username, password, host, port, database, schema=None):
        connection = connect(
            user=username, password=password, host=host, port=port, dbname=database
        )
        super().__init__(connection, schema=schema)
