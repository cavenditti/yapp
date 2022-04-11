from abc import ABC, abstractmethod


class Job(ABC):
    def __init__(self, pipeline):
        self.pipeline = pipeline

    @abstractmethod
    def execute(self, *inputs):
        pass

    @property
    def config(self):
        return self.pipeline.config
