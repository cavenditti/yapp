import pytest

from yapp import Pipeline, Job
from yapp.adapters.utils import DummyInput, DummyOutput
from yapp.core.inputs import Inputs


class DummyJob(Job):
    def execute(self):
        return {"a_value": -15}


class DummyJob2(Job):
    def execute(self, a_value):
        return {"a_value": a_value * 2}


def hook_factory(expected_job, expected_job_name):
    """
    Hook used to check the current job
    """
    def job_looker_hook(pipeline):
        assert isinstance(pipeline.current_job, expected_job)

    def job_name_looker_hook(pipeline):
        assert pipeline.job_name == expected_job_name

    return job_looker_hook, job_name_looker_hook


def test_empty_pipeline():
    pipeline = Pipeline([])
    pipeline()


def test_simple_pipeline():
    pipeline = Pipeline([DummyJob], name="test_pipeline")

    assert pipeline.inputs is not None
    assert pipeline.outputs is not None

    assert pipeline.current_job is None

    pipeline()

    assert pipeline.inputs is not None
    assert pipeline.outputs is not None

    assert pipeline.config is pipeline.inputs.config

    assert "a_value" in pipeline.inputs


def test_bad_inputs_pipeline():
    with pytest.raises(ValueError):
        pipeline = Pipeline([DummyJob], inputs=22, name="test_pipeline")
        pipeline()

    with pytest.raises(ValueError):
        pipeline = Pipeline([DummyJob], name="test_pipeline")
        pipeline(outputs=DummyOutput)  # not a list

    with pytest.raises(ValueError):
        pipeline = Pipeline([DummyJob], name="test_pipeline")
        pipeline(inputs=DummyOutput)  # not list

    with pytest.raises(ValueError):
        pipeline = Pipeline([DummyJob], name="test_pipeline")
        pipeline(outputs=[DummyOutput, 123])  # list items not all OutputAdapter


def test_runtime_inputs_outputs_pipeline():
    inputs = Inputs(sources=[DummyInput])
    pipeline = Pipeline([DummyJob], name="test_pipeline")
    pipeline(inputs=inputs, outputs=[DummyOutput])


def test_hooks_pipeline():
    inputs = Inputs(sources=[DummyInput])

    # used to check at pipeline start
    checker_hooks_none = hook_factory(type(None), None)
    # used to check at job start
    checker_hooks_dummy_job = hook_factory(DummyJob, "DummyJob")

    pipeline = Pipeline(
        [DummyJob],
        name="test_pipeline",
        on_pipeline_start=[*checker_hooks_none],
        on_job_start=[*checker_hooks_dummy_job],
    )
    pipeline(inputs=inputs, outputs=[DummyOutput])
