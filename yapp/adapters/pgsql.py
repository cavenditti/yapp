from sqlalchemy import create_engine

from .SQL_input import SQL_input
from .SQL_output import SQL_output


class PgSqlInput(SQL_input):
    def __init__(self, *, username, password, host, port, database, schema=None, where_clause=None):
        conn_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        super().__init__(create_engine(conn_string), schema=schema, where_clause=where_clause)


class PgSqlOutput(SQL_output):
    def __init__(self, *, username, password, host, port, database, schema=None):
        conn_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        super().__init__(create_engine(conn_string), schema=schema)
