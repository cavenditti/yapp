import pytest
import os

from yapp.cli.parsing import ConfigParser
from yapp.core.errors import MissingConfiguration


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


basic_yml = ''

def test_build_pipeline(tmp_path):
    with pytest.raises(MissingConfiguration):
        with open(os.path.join(tmp_path, 'pipelines.yml'), 'w') as config:
            config.write('')
        ConfigParser('parsing_test', path=tmp_path).parse()

def test_missing_pipelines_yml():
    with pytest.raises(FileNotFoundError):
        ConfigParser('parsing_test').parse()
