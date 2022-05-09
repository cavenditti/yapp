import pytest

from yapp import InputAdapter, Inputs


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
    assert str(inputs) == '<yapp inputs 1>'
    assert repr(inputs) == '<yapp inputs 1 {\'something\'}>'
    assert inputs['something'] == 42

class TestAdaptersInput():
    inputs = Inputs(sources=[DummyInput()])

    def test_adapters_inputs(self):
        assert 'DummyInput' in self.inputs.sources

        self.inputs.expose('DummyInput', 'anything', 'an_input')
        self.inputs.expose('DummyInput', 'this is just ignored by DummyInput', 'another_input')

        assert 'an_input' in self.inputs.exposed
        assert 'another_input' in self.inputs.exposed

        assert 'an_input' in self.inputs
        assert 'another_input' in self.inputs

        assert self.inputs['an_input'] == self.inputs['another_input']

    def test_exposed_assignment(self):
        with pytest.raises(ValueError):
            # cannot assign to exposed value
            self.inputs['an_input'] = 12 # type: ignore

    def test_register_input(self):
        self.inputs.register('an_adapter', DummyInput())
        self.inputs.expose('an_adapter', 'something', 'my_input')

        assert 'my_input' in self.inputs
        assert self.inputs['my_input'] == "just a value"

    def test_merge_inputs(self):
        inputs2 = Inputs(sources=[DummyInput()])
        inputs2.expose('an_adapter', 'something', 'extra')
        self.inputs.update(inputs2)
        assert len(self.inputs) == 4
        assert 'extra' in self.inputs.exposed

    def test_unuion_operator_inputs(self):
        inputs2 = Inputs(sources=[DummyInput()])
        inputs2.expose('an_adapter', 'something', 'extra2')
        with pytest.raises(NotImplementedError):
            self.inputs = self.inputs | inputs2
            assert len(self.inputs) == 5
            assert 'extra2' in self.inputs.exposed

    def test_invalid_inputs(self):
        with pytest.raises(KeyError):
            self.inputs['not there']
