from abc import ABC, abstractmethod
from typing import final


class Job(ABC):
    """
    Job represents a step in our pipeline
    """

    @final
    def __init__(self, name="", params=None, aliases=None):
        if not name:
            name = self.__class__.__name__
        self.name = name

        self.started_at = None
        self.finished_at = None

        self.aliases = aliases if aliases else {}
        self.params = params if params else {}

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

    @final
    def alias(self, original, aliased):
        """
        Alias an input
        """
        self.aliases[aliased] = original
