from sqlalchemy import create_engine

from .sql import SqlInput, SqlOutput


class PgSqlInput(SqlInput):
    def __init__(self, *, username, password, host, port, database, schema=None, where_clause=None):
        conn_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        super().__init__(create_engine(conn_string), schema=schema, where_clause=where_clause)


class PgSqlOutput(SqlOutput):
    def __init__(self, *, username, password, host, port, database, schema=None):
        conn_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        super().__init__(create_engine(conn_string), schema=schema)
