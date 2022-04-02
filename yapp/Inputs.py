from abc import ABC, abstractmethod
import logging
import pandas as pd

from yapp.AttrDict import AttrDict


class Inputs(AttrDict):
    """
    Inputs implementation (just AttrDict with some utility methods)
    """

    def __init__(self, *args, sources=[], **kwargs):
        super().__init__(*args, **kwargs)
        self.exposed = {}  # mapping name to source
        for source in sources:
            self.register(source)

    def __repr__(self):
        return '<yapp inputs>'

    def __str__(self):
        return 'yapp Inputs object'

    def __len__(self):
        return super().__len__() + len(self.exposed) - 1  # skip exposed

    def __getitem__(self, key):
        try:
            logging.debug(f'Using input {key}')
            # if it's an exposed resource from an adapter return it
            if key in self.exposed:
                source, name = self.exposed[key]
                return self[source][name]
            else:
                return super().__getitem__(key)
        except KeyError as e:
            raise KeyError(f'Trying to load missing input "{key}", {e}')

    def __getattr__(self, key):
        return self[key]

    def __setitem__(self, key, value):
        if key in self.exposed:
            raise ValueError('Cannot assign to exposed input from adapter')
        super().__setitem__(key, value)

    def merge(self, d: dict):
        self.__dict__.update(d)
        return self

    def __or__(self, d):
        return self.merge(d)  # TODO union operator should not work in place

    def register(self, name: str, adapter):
        """
        New input adapter (just a new Item)
        """
        self[name] = adapter
        logging.info(f"Registered new input source: {name}")

    def expose(self, source, internal_name, name):
        """
        Expose input attribute using another name
        """
        self.exposed[name] = (source, internal_name)


class InputAdapter(ABC):
    """
    Abstract Input Adapter

    An input adapter represents a type of input from a specific source
    """

    @abstractmethod
    def get(self, key):
        pass

    def __getattr__(self, key):
        logging.debug(f'Loading input from {self.__class__.__name__}: "{key}"')
        return self.get(key)

    def __getitem__(self, key):
        return self.__getattr__(key)


class SQL_Input(InputAdapter):
    """
    SQL Input adapter

    An input adapter for SQL databases, input is read into a pandas DataFrame
    """

    def __init__(self, conn, schema=None, where_clause=None):
        self.schema       = schema
        self.conn         = conn
        self.where_clause = where_clause

    def get(self, table_name):
        schema       = self.schema+'.' if self.schema else ''
        where_clause = ' where '+self.where_clause if self.where_clause else ''
        query        = f"select * from {schema}{table_name}{where_clause}"
        logging.debug(f'Using query: "{query}"')
        return pd.read_sql(query, self.conn)
