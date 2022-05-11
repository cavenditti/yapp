from abc import ABC, abstractmethod


class Job(ABC):
    """
    Job represents a step in our pipeline
    """

    started_at = None
    finished_at = None

    def __init__(self, pipeline):
        self.pipeline = pipeline

    @property
    def name(self):
        """
        Helper to return the name of the class
        """
        return self.__class__.__name__

    @property
    def completed(self):
        """
        True if job successfully run, False otherwise
        """
        return self.finished_at is not None

    def __str__(self):
        return f"<yapp job {self.name}>"

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
