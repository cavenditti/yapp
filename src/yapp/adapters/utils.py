import pandas as pd

from yapp import InputAdapter, OutputAdapter


class DummyInput(InputAdapter):
    """
    Dummy input adapter that always returns an empty DataFrame
    """

    def get(self, _):
        return pd.DataFrame()


class DummyOutput(OutputAdapter):
    """
    Dummy output adapter that prints data it should save
    """

    def save(self, name, data):
        print(name, data)
