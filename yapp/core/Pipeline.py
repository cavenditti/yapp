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

    def __init__(
        self,
        job_list,
        name="",
        inputs=Inputs(),
        outputs=[],
        on_job_start=None,
        on_job_finish=None,
        on_pipeline_start=None,
        on_pipeline_finish=None,
    ):

        if name:
            self.name = name
        else:
            self.name = self.__class__.__name__


        self.job_list = job_list

        # inputs and outputs
        self.inputs = inputs
        self.outputs = outputs

        # hooks
        self.on_job_start = on_job_start
        self.on_job_finish = on_job_finish
        self.on_pipeline_start = on_pipeline_start
        self.on_pipeline_finish = on_pipeline_finish

        # current job if any
        self.current_job = None

    @property
    def config(self):
        """
        Shortcut for configuration from inputs
        """
        return self.inputs.config

    def run_hook(self, hook):
        """
        Check and run hook
        """
        if hook is not None:
            logging.info(f"Running hook {hook.__name__}")
            return hook(self)
            logging.info(f"Done running hook {hook.__name__}")
        return None

    def timed(self, typename, name, fn, *args, **kwargs):
        """
        Timed execution of a function, logging times
        """
        logging.info(f">> Starting {typename} {name}")
        start = datetime.now()
        out = fn(*args, **kwargs)
        end = datetime.now()
        logging.info(f">> Completed {typename} {name} (elapsed: {end-start})")
        return out

    def _run_job(self, job):
        """
        Execution of a single job
        """
        # Create new job
        job_obj = job(self)

        # Get arguments used in the execute function
        args = inspect.getfullargspec(job_obj.execute).args[1:]
        logging.debug(f'Required inputs for {job.__name__}: {args}')

        self.run_hook(self.on_job_start)

        # call execute with right inputs
        # TODO exception handling
        last_output = job_obj.execute(*[self.inputs[i] for i in args])
        logging.debug(f'{job.__name__} run successfully')
        logging.debug(f'''{job.__name__} returned {list(last_output.keys()) if last_output else
                last_output}''')

        self.run_hook(self.on_job_finish)

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
        self.run_hook(self.on_pipeline_start)

        for job in self.job_list:
            self.current_job = job
            self.timed("job", job.__name__, self._run_job, job)

        self.run_hook(self.on_pipeline_finish)

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
            logging.warning(f'Missing inputs for pipeline {self.name}')
            #raise ValueError(f'Missing inputs for pipeline {self.name}')
        if not self.outputs:
            logging.warning(f'Missing outputs for pipeline {self.name}')
            #raise ValueError(f'Missing output for pipeline {self.name}')

        if config:  # config shorthand, just another input
            self.input.config = AttrDict(config)

        self.timed("pipeline", self.name, self._run)
