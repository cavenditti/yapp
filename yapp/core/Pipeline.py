import logging
import json
from datetime import datetime
import inspect

from yapp.core import Job, Inputs, OutputAdapter, AttrDict


class Pipeline:
    """
    Pipeline
    """

    def __init__(
        self,
        job_list,
        inputs=None,
        outputs=None,
        on_job_start=None,
        on_job_finish=None,
        on_pipeline_start=None,
        on_pipeline_finish=None,
    ):

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
        logging.info(f"Starting {typename} {name}")
        start = datetime.now()
        out = fn(*args, **kwargs)
        end = datetime.now()
        logging.info(f"Completed {typename} {name} (elapsed: {end-start})")
        return out

    def _run_job(self, job):
        """
        Execution of a single job
        """
        # Create new job
        job_obj = job(self)

        # Get arguments used in the execute function
        args = inspect.getfullargspec(job_obj.execute).args[1:]

        self.run_hook(self.on_job_start)

        # call execute with right inputs
        # TODO exception handling
        last_output = job_obj.execute(*[self.inputs[i] for i in args])

        self.run_hook(self.on_job_finish)

        # save output and merge into inputs for next steps
        self.save_output(job.__class__.__name__, last_output)
        logging.info(f"saved {self.name} pipeline output")

        self.inputs.merge(last_output)

    def save_output(self, name, data):
        """
        Save data to each output adapter
        """
        for output in self.outputs:
            output.save(name, data)

    def _run(self):
        """
        Runs all Pipeline's jobs
        """
        self.run_hook(self.on_pipeline_start)

        for job in self.job_list:
            self.current_job = job
            self.timed("job", job.__name__, self._run_job, job)

        self.run_hook(self.on_pipeline_finish)

    def __call__(self, inputs, outputs=None, config=None):
        """
        Pipeline entrypoint
        """
        self.name = self.__class__.__name__
        # Override inputs or outputs if specified
        if inputs:
            self.inputs = inputs
        if outputs:
            self.outputs = outputs

        # Check if something is missing
        if not inputs:
            raise ValueError('Missing inputs')
        if not outputs:
            raise ValueError('Missing outputs')

        if config:  # config shorthand, just another input
            self.input.config = AttrDict(config)

        self.timed("pipeline", self.name, self._run)


"""
def compose_pipeline(sources: dict, output: OutputAdapter, config, *pipeline_list):
    '''
        Composes multiple pipelines into one

        :param sources: input data sources
        :param outputs: output data destinations
        :param config: The data types you want to apply to the dataframe
        :*pipeline_list: The list of pipelines to compose
        :return: Output of the last pipeline
    '''

    inputs = Inputs(sources=sources)
    # TODO allow multiple output adapters
    outputs = [output]

    last_output = {}
    for pipeline in pipeline_list:
        last_output = pipeline.run(inputs | last_output, outputs=outputs, config=config)()
    return last_output

"""
