import inspect
import logging
from datetime import datetime

from .attr_dict import AttrDict
from .inputs import Inputs


class Pipeline:
    """
    Pipeline
    """

    # used for log completed status messages for jobs and pipelines
    OK_LOGLEVEL = logging.INFO

    # list of valid hooks for a Pipeline
    valid_hooks = [
        "on_pipeline_start",
        "on_pipeline_finish",
        "on_job_start",
        "on_job_finish",
    ]

    # used to keep track of nested levels in logs
    __nested_timed_calls = 0

    def __init__(self, job_list, name="", inputs=None, outputs=None, **hooks):

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
        self.outputs = outputs if outputs else []
        logging.debug("Inputs for %s: %s", self.name, self.inputs)

        # hooks
        # Hook names should be checked inside yapp.cli,
        # there should never be an invalid name here
        # but we're being cautious anyway
        logging.debug("Hooks for %s: %s", self.name, hooks)
        for hook_name in Pipeline.valid_hooks:
            if hook_name in hooks:
                logging.debug(
                    "Adding %s hooks for %s", len(hooks[hook_name]), hook_name
                )
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
    def timed(self, typename, name, func, *args, **kwargs):
        """
        Timed execution of a function, logging times
        """
        # Increase nesting level
        self.__nested_timed_calls += 1
        # TODO find some better idea for this
        prefix = ">" if self.__nested_timed_calls < 3 else ""

        logging.info("%s Starting %s %s", prefix, typename, name)
        start = datetime.now()
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

        # Decrease nesting level
        self.__nested_timed_calls -= 1
        return out

    def _run_job(self, job):
        """
        Execution of a single job
        """

        # Get arguments used in the execute function
        args = inspect.getfullargspec(job.execute).args[1:]
        logging.debug("Required inputs for %s: %s", job.name, args)

        self.run_hook("on_job_start")

        # call execute with right inputs
        #
        # TODO exception handling is done at cli level for now
        # but here would definetly be a better choice
        last_output = job.execute(*[self.inputs[i] for i in args])
        logging.debug("%s run successfully", job.name)
        logging.debug(
            "%s returned %s",
            job.name,
            list(last_output.keys()) if last_output else last_output,
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
            logging.info("saved %s output to %s", name, output)

    def _run(self):
        """
        Runs all Pipeline's jobs
        """
        self.run_hook("on_pipeline_start")

        for job_class in self.job_list:
            logging.debug('Instantiating new job from "%s"', job_class)
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
            logging.warning("Missing inputs for pipeline %s", self.name)
        if not self.outputs:
            logging.warning("Missing outputs for pipeline %s", self.name)

        if config:  # config shorthand, just another input
            self.inputs.config = AttrDict(config)

        self.timed("pipeline", self.name, self._run)
