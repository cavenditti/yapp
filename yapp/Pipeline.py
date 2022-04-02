import logging
import json
from datetime import datetime
import inspect

from yapp import Job, Inputs, OutputAdapter, AttrDict


class Pipeline:
    """
    Pipeline
    """

    def __init__(self, job_list):
        self.job_list = job_list

    @property
    def config(self):
        return self.inputs.config

    def run(self, job):
        # Create new job
        job_obj = job(self)

        # Get arguments used in the execute function
        args = inspect.getfullargspec(job_obj.execute).args[1:]

        # call execute with right inputs
        last_output = job_obj.execute(*[self.inputs[i] for i in args])

        # save output and merge into inputs for next steps
        self.save_output(job.__class__.__name__, last_output)
        logging.info(f'saved {self.name} pipeline output')

        self.inputs.merge(last_output)

    def save_output(self, name, data):
        """
        Save data to each output adapter
        """
        for output in self.outputs:
            output.save(name, data)

    def __call__(self, inputs, outputs=None, config=None):
        self.name    = self.__class__.__name__
        self.inputs  = inputs
        self.outputs = outputs
        if config:  # config shorthand, just another input
            self.input.config = AttrDict(config)

        logging.info(f'Starting pipeline {self.name}')
        start = datetime.now()

        for job in self.job_list:
            self.run(job)

        end = datetime.now()
        logging.info(f'Completed pipeline {self.name} (elapsed: {end-start})')

        return self.last_output


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
