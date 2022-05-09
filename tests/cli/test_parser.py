import os
import pathlib

import pytest

from yapp.cli.parsing import ConfigParser
from yapp.core.errors import (ConfigurationError, EmptyConfiguration,
                              ImportedCodeFailed, MissingConfiguration,
                              MissingPipeline)


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


def test_build_job():
    pass


basic_yml = ""


def test_bad_python_file(tmp_path):
    with pytest.raises(ImportedCodeFailed):
        make_tmp(tmp_path, "nop.py", "def do_nothing():\nwrong python file", parent='a_pipeline')
        make_tmp(tmp_path, "pipelines.yml", "a_pipeline:\n  steps:\n    - nop.do_nothing")
        ConfigParser("a_pipeline", path=tmp_path).parse()


def test_build_pipeline(tmp_path):
    make_tmp(tmp_path, "nop.py", "def do_nothing():\n    pass", parent='a_pipeline')
    make_tmp(tmp_path, "pipelines.yml", "a_pipeline:\n  steps:\n    - nop.do_nothing")
    ConfigParser("a_pipeline", path=tmp_path).parse()


def test_build_pipeline_nodir(tmp_path):
    make_tmp(tmp_path, "nop.py", "def do_nothing():\n    pass")
    make_tmp(tmp_path, "pipelines.yml", "a_pipeline:\n  steps:\n    - nop.do_nothing")
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
