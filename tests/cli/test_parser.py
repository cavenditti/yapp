import graphlib
import os
import pathlib

import pytest

from yapp.cli.parsing import ConfigParser
from yapp.core.attr_dict import AttrDict
from yapp.core.errors import (ConfigurationError, EmptyConfiguration,
                              ImportedCodeFailed, MissingConfiguration,
                              MissingPipeline)
from yapp.core.inputs import Inputs


def make_tmp(tmp_path, filename, content, parent=''):
    pathlib.Path(parent).mkdir(parents=True, exist_ok=True)
    with open(os.path.join(tmp_path, filename), "w") as file:
        file.write(content)


def test_load_module(tmpdir):
    pass


def test_create_adapter():
    pass


def test_build_hooks():
    pass


def test_build_inputs():
    pass


def test_build_outputs():
    pass


def test_build_job_class():
    pass



# --------------------------------------------------------------------
# TODO the following are basically integration tests and should be kept separated (?)

# used in multiple tests
nop_py = """
def do_nothing():
   pass
"""

basic_pipelines_yml = """
a_pipeline:
    steps:
        - run: nop.do_nothing
"""

def test_build_simple_pipeline(tmp_path):

    make_tmp(tmp_path, "nop.py", nop_py, parent='a_pipeline')
    make_tmp(tmp_path, "pipelines.yml", basic_pipelines_yml)
    pipeline = ConfigParser("a_pipeline", path=tmp_path).parse()

    assert pipeline.name == 'a_pipeline'
    assert pipeline.current_job is None
    assert isinstance(pipeline.inputs, Inputs)
    assert len(pipeline.inputs) == 0
    assert isinstance(pipeline.outputs, list)
    assert len(pipeline.outputs) == 0
    assert pipeline.inputs.config == AttrDict()
    assert 'config' not in pipeline.inputs

    # try to run it
    pipeline()
    assert pipeline.completed



def test_build__circular_pipeline(tmp_path):
    pipelines_yml_circular = """
a_pipeline:
    steps:
        - run: nop.do_nothing
          after: nop.do_nothing
"""

    with pytest.raises(graphlib.CycleError):
        make_tmp(tmp_path, "nop.py", nop_py, parent='a_pipeline')
        make_tmp(tmp_path, "pipelines.yml", pipelines_yml_circular)
        pipeline = ConfigParser("a_pipeline", path=tmp_path).parse()
        pipeline()
        assert pipeline.completed



def test_bad_python_file(tmp_path):
    nop_py_bad = """
def do_nothing():
    syntax error in file
"""

    with pytest.raises(ImportedCodeFailed):
        make_tmp(tmp_path, "nop.py", nop_py_bad, parent='a_pipeline')
        make_tmp(tmp_path, "pipelines.yml", basic_pipelines_yml)
        ConfigParser("a_pipeline", path=tmp_path).parse()


def test_build_pipeline(tmp_path):
    python_file = """
def do_nothing():
    pass

def do_something():
    return {'value': 99.0}
"""

    pipelines_yml = """
a_pipeline:
    inputs:
        - from: utils.DummyInput
          expose:
            - use: whatever
              as: one
            - use: whatever
              as: two
            - use: whatever
              as: three

    outputs:
        - to: utils.DummyOutput

    steps:
        - run: just.do_nothing
        - run: just.do_something
          after: just.do_nothing
"""

    make_tmp(tmp_path, "just.py", python_file, parent='a_pipeline')
    make_tmp(tmp_path, "pipelines.yml", pipelines_yml)
    pipeline = ConfigParser("a_pipeline", path=tmp_path).parse()

    assert pipeline.name == 'a_pipeline'
    assert pipeline.current_job is None
    assert isinstance(pipeline.inputs, Inputs)
    assert len(pipeline.inputs) == 3
    assert 'one' in pipeline.inputs
    assert 'two' in pipeline.inputs
    assert 'three' in pipeline.inputs
    assert isinstance(pipeline.outputs, list)
    assert len(pipeline.outputs) == 1
    assert pipeline.inputs.config == AttrDict()
    assert 'config' not in pipeline.inputs

    # try to run it
    pipeline()
    assert pipeline.completed


def test_very_long_pipeline(tmp_path):
    num = 100

    python_file = '\n'.join([f"""
def do_nothing_{i}():
    pass

""" for i in range(num)])

    pipelines_yml = """
a_pipeline:
    inputs:
        - from: utils.DummyInput
          expose:
            - use: whatever
              as: one
            - use: whatever
              as: two
            - use: whatever
              as: three

    outputs:
        - to: utils.DummyOutput

    steps:""" + '\n'.join([f"""
        - run: just.do_nothing_{i}
        """ for i in range(num)])

    make_tmp(tmp_path, "just.py", python_file, parent='a_pipeline')
    make_tmp(tmp_path, "pipelines.yml", pipelines_yml)
    pipeline = ConfigParser("a_pipeline", path=tmp_path).parse()
    pipeline()
    assert pipeline.completed


def test_build_pipeline_nodir(tmp_path):
    make_tmp(tmp_path, "nop.py", "def do_nothing():\n    pass")
    make_tmp(tmp_path, "pipelines.yml", basic_pipelines_yml)
    ConfigParser("a_pipeline", path=tmp_path).parse()


def test_requested_missing_pipeline(tmp_path):
    with pytest.raises(MissingPipeline):
        make_tmp(tmp_path, "pipelines.yml", "a_pipeline:\nsteps:")
        ConfigParser("missing_pipeline", path=tmp_path).parse()


def test_bad_pipeline(tmp_path):
    with pytest.raises(ConfigurationError):
        make_tmp(tmp_path, "pipelines.yml", "parsing_test:\n")
        ConfigParser("parsing_test", path=tmp_path).parse()

def test_zero_steps(tmp_path):
    with pytest.raises(ConfigurationError):
        make_tmp(tmp_path, "pipelines.yml", "parsing_test:\n  steps:")
        ConfigParser("parsing_test", path=tmp_path).parse()


def test_empty_pipelines_yml(tmp_path):
    with pytest.raises(EmptyConfiguration):
        make_tmp(tmp_path, "pipelines.yml", "")
        ConfigParser("parsing_test", path=tmp_path).parse()


def test_missing_pipelines_yml():
    with pytest.raises(MissingConfiguration):
        ConfigParser("parsing_test").parse()
