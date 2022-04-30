import inspect
import logging
from datetime import datetime

from .attr_dict import AttrDict
from .inputs import Inputs
from .output_adapter import OutputAdapter


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
        "on_pipeline_start",
        "on_pipeline_finish",
        "on_job_start",
        "on_job_finish",
    ]

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
            setattr(self, hook_name, new_hooks)

        # current job if any
        self.current_job = None

    @property
    def config(self):
        """ Shortcut for configuration from inputs
        """
        return self.inputs.config

    @property
    def job_name(self):
        """ Shortcut for self.current_job.name which handles no current_job
        """
        if self.current_job:
            return self.current_job.name
        return None

    def run_hook(self, hook_name):
        """ Run all hooks for current event

        A hook is just a function taking a pipeline as single argument

        Args:
            hook_name (str):
                name of the hook to run ("on_pipeline_start", "on_job_start", etc.)
        """
        hooks = getattr(self, hook_name)
        for hook in hooks:
            self.timed(f"{hook_name} hook", hook.__name__, hook, self)

    def timed(self, typename, name, func, *args, **kwargs):
        """ Runs a timed execution of a function, logging times

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
        """ Execution of a single job
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
            logging.debug('saving last_output: %s len %s', type(last_output),
                    len(last_output))
            self.save_output(job.__class__.__name__, last_output)
            self.inputs.update(last_output)

    def save_output(self, name, data):
        """ Save data to each output adapter

        Args:
            name:
                name to pass to the output adapters when saving the data
            data:
                data to save
        """
        for output in self.outputs:
            output[name] = data
            logging.info("saved %s output to %s", name, output)

    def _run(self):
        """ Runs all Pipeline's jobs
        """
        self.run_hook("on_pipeline_start")

        for job_class in self.job_list:
            logging.debug('Instantiating new job from "%s"', job_class)
            job_obj = job_class(self)
            self.current_job = job_obj
            self.timed("job", job_obj.name, self._run_job, job_obj)

        self.run_hook("on_pipeline_finish")

    def __call__(self, inputs=None, outputs=None, config=None):
        """ Pipeline entrypoint

        Sets up inputs, outputs and config (if specified) and runs the pipeline
        """
        # Override inputs or outputs if specified
        if inputs:
            self.inputs = inputs
        if outputs:
            self.outputs = outputs

        # FIXME define one type for outputs and just enforce it
        # Eventually creating an Outputs class
        if isinstance(self.outputs, set):
            self.outputs = list(self.outputs)

        if not isinstance(self.inputs, Inputs):
            raise ValueError(f'{self.inputs} is not an Inputs object')
        if not isinstance(self.outputs, list):
            raise ValueError(f'{self.outputs} is not a list')

        for i,output in enumerate(self.outputs):
            if isinstance(output, type):
                self.outputs[i] = output = output()
            if not isinstance(output, OutputAdapter):
                raise ValueError(f'{output} is not an OutputAdapter')

        # Check if something is missing
        if not self.inputs:
            logging.warning("Missing inputs for pipeline %s", self.name)
        if not self.outputs:
            logging.warning("Missing outputs for pipeline %s", self.name)

        if not config:
            config = {}

        # config shorthand, just another input
        self.inputs.config = AttrDict(config)

        self.timed("pipeline", self.name, self._run)
