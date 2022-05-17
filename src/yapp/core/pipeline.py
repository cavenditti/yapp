import inspect
import logging
from datetime import datetime
from typing import Sequence, Set

from .inputs import Inputs
from .job import Job
from .monitor import Monitor
from .output_adapter import OutputAdapter


def enforce_list(value):
    """Makes sure the argument can be treated as a list"""
    if value is None:
        return []
    if isinstance(value, set):
        return list(value)
    return value if isinstance(value, list) else [value]


class Pipeline:
    """yapp Pipeline object

    Pipeline implementation.
    Collects jobs, hooks and input and output adapter and runs the pipeline.

    Attributes:
        OK_LOGLEVEL (int):
            Loglevel to use for pipeline and jobs completed execution status messages
        VALID_HOOKS (list):
            list of valid hooks that can be used in a pipeline
        __nested_timed_calls (int):
            level of nested calls to `timed`, used to enhance logging
    """

    OK_LOGLEVEL = logging.INFO

    VALID_HOOKS = [
        "pipeline_start",
        "pipeline_finish",
        "job_start",
        "job_finish",
    ]

    started_at = None
    finished_at = None

    __nested_timed_calls = 0

    def __init__(
        self,
        job_list: Sequence[type[Job]],
        name: str = "",
        inputs: Inputs | None = None,
        outputs: Sequence[type[OutputAdapter]]
        | Set[type[OutputAdapter]]
        | type[OutputAdapter]
        | None = None,
        monitor: Monitor | None = None,
        **hooks,
    ):
        """__init__.

        Args:
            job_list:
                List of Jobs classes to run (in correct order) inside the pipeline

            name:
                Pipeline name

            inputs:
                Inputs for the pipeline

            outputs:
                Outputs for the pipeline

            monitor:
                Monitor for the pipeline

            **hooks:
                Hooks to attach to the pipeline
        """
        if name:
            self.name = name
        else:
            self.name = self.__class__.__name__
        logging.debug("Creating pipeline %s", self.name)

        self.job_list = job_list
        logging.debug(
            "Jobs for %s: %s",
            self.name,
            " -> ".join([job.__name__ for job in self.job_list]),
        )

        # inputs and outputs
        self.inputs = inputs if inputs else Inputs()
        self.outputs = enforce_list(outputs)
        self.save_results = []
        self.monitor = monitor if monitor else Monitor()
        logging.debug("Inputs for %s: %s", self.name, repr(self.inputs))

        # hooks
        # Hook names should be checked inside yapp.cli,
        # there should never be an invalid name here
        # but we're being cautious anyway
        logging.debug("Hooks for %s: %s", self.name, hooks)
        for hook_name in Pipeline.VALID_HOOKS:
            if hook_name in hooks:
                logging.debug(
                    "Adding %s hooks for %s", len(hooks[hook_name]), hook_name
                )
                new_hooks = hooks[hook_name]
            else:
                new_hooks = []
            # If monitor object has it, use the method
            if hasattr(monitor, hook_name) and callable(getattr(monitor, hook_name)):
                new_hooks.append(getattr(monitor, hook_name))
            setattr(self, hook_name, new_hooks)

        # current job if any
        self.current_job = None

    @property
    def config(self):
        """Shortcut for configuration from inputs"""
        return self.inputs.config

    @property
    def job_name(self):
        """Shortcut for self.current_job.name which handles no current_job"""
        if self.current_job:
            return self.current_job.name
        return None

    @property
    def completed(self):
        """
        True if pipeline successfully run, False otherwise
        """
        return self.finished_at is not None

    def run_hook(self, hook_name):
        """Run all hooks for current event

        A hook is just a function taking a pipeline as single argument

        Args:
            hook_name (str):
                name of the hook to run ("on_pipeline_start", "on_job_start", etc.)
        """
        hooks = getattr(self, hook_name)
        for hook in hooks:
            self.timed(f"{hook_name} hook", hook.__name__, hook, self)

    def timed(self, typename, name, func, *args, _update_object=None, **kwargs):
        """Runs a timed execution of a function, logging times

        The first two parameters are used to specify the type and name of the entity to run.

        Args:
            typename (str):
                name of the type of the component to run ("pipeline", "job", "hook", etc.)
            name (str):
                name of the component to run
            func (callable):
                function to run
            *args:
            **kwargs:

        Returns:
            (Any) The output of provided function
        """
        # Increase nesting level
        self.__nested_timed_calls += 1
        # TODO find some better idea for this
        # prefix = ">" if self.__nested_timed_calls < 3 else ""
        if typename == "pipeline":
            prefix = ">>"
        elif self.__nested_timed_calls < 3:
            prefix = ">"
        else:
            prefix = ""

        logging.info("%s Starting %s %s", prefix, typename, name)
        start = datetime.now()
        if _update_object:
            _update_object.started_at = start
        out = func(*args, **kwargs)
        end = datetime.now()
        logging.log(
            Pipeline.OK_LOGLEVEL,
            "%s Completed %s %s (elapsed: %s)",
            prefix,
            typename,
            name,
            end - start,
        )
        if _update_object:
            _update_object.finished_at = start

        # Decrease nesting level
        self.__nested_timed_calls -= 1
        return out

    def _run_job(self, job):
        """Execution of a single job"""

        # Get arguments used in the execute function
        arg_spec = inspect.getfullargspec(job.execute)
        if arg_spec.defaults:
            args = arg_spec.args[1 : -len(arg_spec.defaults)]
        else:
            args = arg_spec.args[1:]
        logging.debug("Required inputs for %s: %s", job.name, args)

        self.run_hook("job_start")

        # call execute with right inputs
        #
        # exception handling is done at cli level for now
        # maybe some specific exception handling may fit here?
        last_output = job.execute(*[self.inputs[i] for i in args], **job.params)
        logging.debug("%s run successfully", job.name)
        logging.debug(
            "%s returned %s",
            job.name,
            list(last_output.keys()) if isinstance(last_output, dict) else last_output,
        )

        self.run_hook("job_finish")

        # save output and merge into inputs for next steps
        if isinstance(last_output, dict):
            logging.debug(
                "saving last_output: %s len %s",
                type(last_output),
                len(last_output) if last_output is not None else "None",
            )
            for key in last_output:
                self.save_output(key, last_output[key])
        else:
            if last_output is None:
                logging.warning("> %s returned None", job.name)
            # save using job name
            self.save_output(job.name, last_output)
            # replace last_output with dict to merge into inputs
            last_output = {job.name: last_output}
        # merge into inputs
        try:
            self.inputs.update(last_output)
        except (TypeError, ValueError):
            logging.warning("> Cannot merge output to inputs for job %s", job.name)
        logging.info("Done saving %s outputs", job.name)

    def save_output(self, name, data, results=False):
        """Save data to each output adapter

        Args:
            name (str):
                name to pass to the output adapters when saving the data
            data (Any):
                data to save
        """

        method = "_save" if not results else "_save_result"
        for output in self.outputs:
            getattr(output, method)(name, data)
            logging.debug("saved %s output to %s", name, output)

    def _run(self):
        """Runs all Pipeline's jobs"""
        self.run_hook("pipeline_start")

        for job_class in self.job_list:
            logging.debug('Instantiating new job from "%s"', job_class)
            job_obj = job_class(self)
            self.current_job = job_obj
            self.timed(
                "job", job_obj.name, self._run_job, job_obj, _update_object=job_obj
            )

        self.run_hook("pipeline_finish")

        # should this be done here or before the hook?
        for output_name in self.save_results:
            self.save_output(output_name, self.inputs[output_name], results=True)

    def __call__(
        self,
        save_results: Sequence[str] | None = None,
    ):
        """Pipeline entrypoint

        Sets up inputs, outputs and config (if specified) and runs the pipeline
        """
        # Override inputs or outputs if specified
        if save_results:
            self.save_results = enforce_list(save_results)

        if not isinstance(self.inputs, Inputs):
            raise ValueError(f"{self.inputs} is not an Inputs object")

        for i, output in enumerate(self.outputs):
            if isinstance(output, type):
                self.outputs[i] = output = output()
            if not isinstance(output, OutputAdapter):
                raise ValueError(f"{output} is not an OutputAdapter")

        # Check if something is missing
        if not self.inputs:
            logging.warning("> Missing inputs for pipeline %s", self.name)
        if not self.outputs:
            logging.warning("> Missing outputs for pipeline %s", self.name)

        self.timed("pipeline", self.name, self._run, _update_object=self)
