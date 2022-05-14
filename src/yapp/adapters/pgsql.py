from sqlalchemy import create_engine

from .sql import SqlInput, SqlOutput


def make_pgsql_connection(username, password, host, port, database):
    """
    Create PostgreSQL connection using SQLAlchemy `create_engine`

    Args:
        username:
        password:
        host:
        port:
        database:
    """
    return create_engine(f"postgresql://{username}:{password}@{host}:{port}/{database}")


class PgSqlInput(SqlInput):
    """
    Very simple PostgreSQL input adapter
    """

    def __init__(
        self,
        *,
        username,
        password,
        host,
        port,
        database,
        schema=None,
        where_clause=None,
    ):
        connection = make_pgsql_connection(username, password, host, port, database)
        super().__init__(connection, schema=schema, where_clause=where_clause)


class PgSqlOutput(SqlOutput):
    """
    Very simple PostgreSQL ouput adapter
    """

    def __init__(self, *, username, password, host, port, database, schema=None):
        connection = make_pgsql_connection(username, password, host, port, database)
        super().__init__(connection, schema=schema)
