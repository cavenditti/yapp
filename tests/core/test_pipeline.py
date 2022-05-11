import pytest

from yapp import Job, Pipeline
from yapp.adapters.utils import DummyInput, DummyOutput
from yapp.core.inputs import Inputs
from yapp.core.output_adapter import OutputAdapter


class DummyJob(Job):
    def execute(self):
        return {"a_value": -15}


class DummyJob2(Job):
    def execute(self, a_value):
        return {"another_value": a_value * 2}


def hook_factory(expected_job, expected_job_name):
    """
    Hook used to check the current job
    """

    def job_looker_hook(pipeline):
        assert isinstance(pipeline.current_job, expected_job)

    def job_name_looker_hook(pipeline):
        assert pipeline.job_name == expected_job_name
        if expected_job_name:
            assert pipeline.current_job.config is pipeline.config
            assert str(pipeline.current_job) == f"<yapp job {expected_job_name}>"

    return job_looker_hook, job_name_looker_hook


def test_empty_pipeline():
    pipeline = Pipeline([])
    pipeline()
    assert pipeline.completed


def pipeline_common_asserts(pipeline, *args, **kwargs):
    assert pipeline.inputs is not None
    assert pipeline.outputs is not None

    assert pipeline.current_job is None

    pipeline(*args, **kwargs)

    assert pipeline.completed
    assert pipeline.inputs is not None
    assert pipeline.outputs is not None

    assert pipeline.config is pipeline.inputs.config

    return pipeline


def test_simple_pipeline():
    pipeline = Pipeline([DummyJob], name="test_pipeline")
    pipeline = pipeline_common_asserts(pipeline)

    assert "a_value" in pipeline.inputs

    pipeline = Pipeline([DummyJob, DummyJob2], name="test_pipeline")
    pipeline = pipeline_common_asserts(pipeline)

    assert "another_value" in pipeline.inputs


def test_outputadapter(capfd):
    pipeline = Pipeline([DummyJob], name="test_pipeline", outputs=[DummyOutput])
    pipeline()
    assert pipeline.completed

    out, _ = capfd.readouterr()
    assert out.strip() == "a_value -15"

    pipeline = Pipeline([DummyJob, DummyJob2], name="test_pipeline", outputs={DummyOutput})
    pipeline()
    assert pipeline.completed

    out, _ = capfd.readouterr()
    outs = out.rstrip().split('\n')
    assert len(outs) == 2
    assert outs[0] == "a_value -15"
    assert outs[1] == "another_value -30"


# With type hints these don't make much sense anymore
def test_bad_inputs():
    with pytest.raises(ValueError):
        pipeline = Pipeline([DummyJob], inputs=22, name="test_pipeline")  # type: ignore
        pipeline()

    # Doesn't raise error
    pipeline = Pipeline([DummyJob], name="test_pipeline", outputs=DummyOutput)
    pipeline()
    assert pipeline.completed


    with pytest.raises(ValueError):
        # not an Inputs
        pipeline = Pipeline([DummyJob], name="test_pipeline", inputs=DummyOutput)  # type: ignore
        pipeline()

    with pytest.raises(ValueError):
        pipeline = Pipeline([DummyJob], name="test_pipeline", outputs=[DummyOutput, 123])  # type: ignore
        pipeline()


def test_runtime_inputs_outputs():
    inputs = Inputs(sources=[DummyInput])
    pipeline = Pipeline(
        [DummyJob], name="test_pipeline", inputs=inputs, outputs=[DummyOutput]
    )
    pipeline()
    assert pipeline.completed


def test_hooks():
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
        inputs=inputs,
        outputs=[DummyOutput]
    )
    pipeline()
    assert pipeline.completed


class ReturnsNoneJob(Job):
    def execute(self):
        return None


class PrintEmptyOutput(OutputAdapter):
    def save(self, key, data):
        pass

    def empty(self, job_name):
        print(f"empty {job_name} output", end="")


def test_empty_output(capfd):
    pipeline = Pipeline([ReturnsNoneJob], name="test_pipeline", outputs=[PrintEmptyOutput])
    pipeline()

    out, _ = capfd.readouterr()
    assert out == "empty ReturnsNoneJob output"


def test_ignore_empty_output(capfd):
    pipeline = Pipeline([ReturnsNoneJob], name="test_pipeline", outputs=[DummyOutput])
    pipeline()
    assert pipeline.completed

    out, _ = capfd.readouterr()
    assert out == ""


class PrintFinalOutput(OutputAdapter):
    def save(self, key, data):
        pass

    def save_result(self, key, data):
        print(key, "=", data, end="")


def test_save_results(capfd):
    pipeline = Pipeline([DummyJob], outputs=PrintFinalOutput)
    pipeline(save_results="a_value")
    assert pipeline.completed

    out, _ = capfd.readouterr()
    assert out == "a_value = -15"


def test_save_results_default(capfd):
    pipeline = Pipeline([DummyJob], outputs=DummyOutput)
    pipeline(save_results="a_value")
    assert pipeline.completed

    out, _ = capfd.readouterr()
    outs = out.strip().split('\n')
    assert len(outs) == 2
    assert outs[0] == outs[1] == "a_value -15"
