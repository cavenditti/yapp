from abc import ABC, abstractmethod


class Job(ABC):
    """
    Job represents a step in our pipeline
    """

    def __init__(self, pipeline):
        self.pipeline = pipeline

    @property
    def name(self):
        return self.__class__.__name__

    def __str__(self):
        return self.name

    @abstractmethod
    def execute(self, *inputs):
        pass

    @property
    def config(self):
        return self.pipeline.config
