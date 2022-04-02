from abc import ABC, abstractmethod
import logging
import pandas as pd


class OutputAdapter(ABC):
    """
    Abstract output Adapter

    An output adapter represents a specific output destination
    """

    @abstractmethod
    def save(self, key, data):
        pass

    def __setitem__(self, key, data):
        logging.debug(f'Saving output to {self.__class__.__name__}: "{key}"')
        return self.save(key, data)


class SQL_Output(OutputAdapter):
    """
    SQL output adapter

    Output adapter for SQL databases, a pandas DataFrame is written to a table
    """

    def __init__(self, conn, schema=None):
        self.conn   = conn
        self.schema = schema

    def save(self, table_name, data):
        pd.to_sql(table_name, self.conn, schema=self.schema, if_exists='append')
