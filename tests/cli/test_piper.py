import os
from yapp.cli.parsing import ConfigParser
from test_parser import make_tmp

def test_build_pipeline(tmp_path):
    python_file = """
import pandas as pd
def create(df, value=1):
    return pd.DataFrame({'a': [value, value, value]})

def multiply(df, by=2):
    return df.apply(lambda x: x*by)
"""

    pipelines_yml = """
a_pipeline:
    outputs:
        - to: utils.DummyOutput

    steps:
        - !pipe:
            run: funcs.create
        - !pipe
            run: funcs.multiply
        - !pipe
            run: funcs.multiply
        - !pipe
            run: funcs.multiply
            with:
                by: 3
"""

    make_tmp(tmp_path, "funcs.py", python_file, parent='a_pipeline')
    make_tmp(tmp_path, "pipelines.yml", pipelines_yml)
    os.chdir(tmp_path)
    pipeline = ConfigParser("a_pipeline", path=tmp_path).parse()
    pipeline()
