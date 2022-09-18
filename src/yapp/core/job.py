from abc import ABC, abstractmethod
from typing import final
from graphlib import TopologicalSorter


class Job(ABC):
    """
    Job represents a step in our pipeline
    """

    started_at = None
    finished_at = None
    params = {}

    @final
    def __init__(self, pipeline, name=""):
        self.__name = name if name else self.__class__.__name__
        #self.pipeline = pipeline

    @property
    def name(self):
        """
        Helper to return the name of the class
        """
        return self.__name

    @final
    @property
    def completed(self):
        """
        True if job successfully run, False otherwise
        """
        return self.finished_at is not None

    def __str__(self):
        return f"<yapp job {self.name}>"

    @abstractmethod
    def execute(self, *inputs, **params):
        """
        Job entrypoint
        """

    '''
    @final
    @property
    def config(self):
        """
        Shortcut for self.pipeline.config
        """
        return self.pipeline.config
    '''


class Jobs(TopologicalSorter):
    def __init__(self, dag: dict, mapping: dict):
        """
        Create a new Jobs dag

        Params:
            dag: Directed acyclic graph of the jobs
            mapping: dictionary mapping job names in dag to actual jobs
        """
        super().__init__(dag)
        self.__mapping = mapping

    def get_ready(self):
        return ((name, self.__mapping[name]) for name in super().get_ready())

    def static_order(self):
        raise NotImplementedError

    def names(self):
        return [job.name for job in self.__mapping.values()]
