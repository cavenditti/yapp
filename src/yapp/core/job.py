from abc import ABC, abstractmethod


class Job(ABC):
    """
    Job represents a step in our pipeline
    """

    def __init__(self, pipeline):
        self.pipeline = pipeline

    @property
    def name(self):
        """
        Helper to return the name of the class
        """
        return self.__class__.__name__

    def __str__(self):
        return self.name

    @abstractmethod
    def execute(self, *inputs):
        """
        Job entrypoint
        """

    @property
    def config(self):
        """
        Shortcut for self.pipeline.config
        """
        return self.pipeline.config
