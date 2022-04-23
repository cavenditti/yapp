from os.path import join

import pandas as pd

from yapp import InputAdapter


class CsvInput(InputAdapter):
    """
    CSV Input adapter

    An input adapter for CSV files, input is read into a pandas DataFrame
    """

    def __init__(self, directory, **other_kwargs):
        self.directory = directory
        self.other_kwargs = other_kwargs

    def get(self, filename: str):
        if not filename.endswith(".csv"):
            filename += ".csv"
        return pd.read_csv(join(self.directory, filename), **self.other_kwargs)
