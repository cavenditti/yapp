import logging

import pandas as pd

from yapp import InputAdapter, OutputAdapter


class SqlInput(InputAdapter):
    """
    SQL Input adapter

    An input adapter for SQL databases, input is read into a pandas DataFrame
    """

    def __init__(self, conn, schema=None, where_clause=None):
        self.schema = schema
        self.conn = conn
        self.where_clause = where_clause

    def get(self, table_name):
        schema = self.schema + "." if self.schema else ""
        where_clause = " where " + self.where_clause if self.where_clause else ""
        query = f"select * from {schema}{table_name}{where_clause}"
        logging.debug('Using query: "%s"', query)
        return pd.read_sql(query, self.conn)


class SqlOutput(OutputAdapter):
    """
    SQL output adapter

    Output adapter for SQL databases, a pandas DataFrame is written to a table
    """

    def __init__(self, conn, schema=None, extra_fields: dict = None):
        self.conn = conn
        self.schema = schema
        self.extra_fields = extra_fields if extra_fields else {}

    def save(self, table_name, data):
        for field, value in self.extra_fields.items():
            data[field] = value
        data.to_sql(table_name, self.conn, schema=self.schema, if_exists="append")
