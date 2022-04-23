import logging
import json
from datetime import datetime
import inspect

from .Job import Job
from .Inputs import Inputs
from .OutputAdapter import OutputAdapter
from .AttrDict import AttrDict


class Pipeline:
    """
    Pipeline
    """

    # list of valid hooks for a Pipeline
    _valid_hooks = [
        "on_pipeline_start",
        "on_pipeline_finish",
        "on_job_start",
        "on_job_finish",
    ]

    # used to keep track of nested levels in logs
    __nested_timed_calls = 0

    def __init__(self, job_list, name="", inputs=Inputs(), outputs=[], **hooks):

        if name:
            self.name = name
        else:
            self.name = self.__class__.__name__
        logging.debug(f"Creating pipeline {self.name}")

        self.job_list = job_list
        logging.debug(
            f"Jobs for {self.name}: {' -> '.join([job.__name__ for job in self.job_list])}"
        )

        # inputs and outputs
        self.inputs = inputs
        self.outputs = outputs
        logging.debug(f"Inputs for {self.name}: {self.inputs}")
        logging.debug(f"Outputs for {self.name}: {self.outputs}")

        # hooks
        # Hook names should be checked inside yapp.cli,
        # there should never be an invalid name here
        # but we're being cautious anyway
        logging.debug(f"Hooks for {self.name}: {hooks}")
        for hook_name in Pipeline._valid_hooks:
            if hook_name in hooks:
                logging.debug(f"Adding {len(hooks[hook_name])} hooks for {hook_name}")
                new_hooks = hooks[hook_name]
            else:
                new_hooks = []
            setattr(self, hook_name, new_hooks)

        # current job if any
        self.current_job = None

    @property
    def config(self):
        """
        Shortcut for configuration from inputs
        """
        return self.inputs.config

    @property
    def job_name(self):
        """
        Shortcut for self.current_job.name which handles no current_job
        """
        if self.current_job:
            return self.current_job.name
        else:
            return None

    def run_hook(self, hook_name):
        """
        Run all hooks for current event
        A hook is just a function taking a pipeline as single argument
        """
        hooks = getattr(self, hook_name)
        for hook in hooks:
            self.timed(f"{hook_name} hook", hook.__name__, hook, self)

    # TODO this could be a decorator
    def timed(self, typename, name, fn, *args, **kwargs):
        """
        Timed execution of a function, logging times
        """
        # Increase nesting level
        self.__nested_timed_calls += 1
        # TODO find some better idea for this
        prefix = ">" if self.__nested_timed_calls < 3 else ""

        logging.info(f"{prefix} Starting {typename} {name}")
        start = datetime.now()
        out = fn(*args, **kwargs)
        end = datetime.now()
        logging.ok(f"{prefix} Completed {typename} {name} (elapsed: {end-start})")

        # Decrease nesting level
        self.__nested_timed_calls -= 1
        return out

    def _run_job(self, job):
        """
        Execution of a single job
        """

        # Get arguments used in the execute function
        args = inspect.getfullargspec(job.execute).args[1:]
        logging.debug(f"Required inputs for {job.name}: {args}")

        self.run_hook("on_job_start")

        # call execute with right inputs
        # TODO exception handling
        last_output = job.execute(*[self.inputs[i] for i in args])
        logging.debug(f"{job.name} run successfully")
        logging.debug(
            f"""{job.name} returned {list(last_output.keys()) if last_output else
                last_output}"""
        )

        self.run_hook("on_job_finish")

        # save output and merge into inputs for next steps
        if last_output:
            self.save_output(job.__class__.__name__, last_output)
            self.inputs.merge(last_output)

    def save_output(self, name, data):
        """
        Save data to each output adapter
        """
        for output in self.outputs:
            output.save(name, data)
            logging.info(f"saved {name} output to {output}")

    def _run(self):
        """
        Runs all Pipeline's jobs
        """
        self.run_hook("on_pipeline_start")

        for job_class in self.job_list:
            logging.debug(f'Instantiating new job from "{job_class}"')
            job_obj = job_class(self)
            self.current_job = job_obj
            self.timed("job", job_obj.name, self._run_job, job_obj)

        self.run_hook("on_pipeline_finish")

    def __call__(self, inputs=None, outputs=None, config=None):
        """
        Pipeline entrypoint
        """
        # Override inputs or outputs if specified
        if inputs:
            self.inputs = inputs
        if outputs:
            self.outputs = outputs

        # Check if something is missing
        if not self.inputs:
            logging.warning(f"Missing inputs for pipeline {self.name}")
            # raise ValueError(f'Missing inputs for pipeline {self.name}')
        if not self.outputs:
            logging.warning(f"Missing outputs for pipeline {self.name}")
            # raise ValueError(f'Missing output for pipeline {self.name}')

        if config:  # config shorthand, just another input
            self.input.config = AttrDict(config)

        self.timed("pipeline", self.name, self._run)
