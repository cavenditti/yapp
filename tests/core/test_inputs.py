import pytest

from yapp import Inputs, InputAdapter

class DummyInput(InputAdapter):
    """
    Mock InputAdapter
    """
    def get(self, _):
        return "just a value"

class BadInput(InputAdapter):
    def get(self, key):
        raise KeyError(f'{key} not found')


def test_basic_inputs():
    inputs = Inputs()
    inputs['something'] = 42
    assert 'something' in inputs
    assert len(inputs) == 1
    assert str(inputs) == '<yapp inputs 1 {\'something\'}>'
    assert inputs.something == 42
    assert inputs['something'] == 42

def test_adapters_inputs():
    inputs = Inputs(sources=[DummyInput()])
    assert 'DummyInput' in inputs
    inputs.expose('DummyInput', 'anything', 'an_input')
    inputs.expose('DummyInput', 'this is just ignored by DummyInput', 'another_input')
    assert 'an_input' in inputs.exposed
    assert inputs.an_input == inputs.another_input
    assert inputs['an_input'] == inputs.another_input
    assert inputs.an_input == inputs['another_input']
    assert inputs['an_input'] == inputs['another_input']

    with pytest.raises(ValueError):
        # cannot assign to exposed value
        inputs['an_input'] = 12 # type: ignore

    with pytest.raises(ValueError):
        # cannot assign to exposed value
        inputs.an_input = 12 # type: ignore

def test_merge_inputs():
    inputs = Inputs()
    inputs['something'] = 42
    inputs2 = Inputs(sources=[DummyInput()])
    inputs = inputs | inputs2


