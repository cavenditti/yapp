import logging
from abc import ABC, abstractmethod


class InputAdapter(ABC):
    """
    Abstract Input Adapter

    An input adapter represents a type of input from a specific source
    """

    @abstractmethod
    def get(self, key):
        """
        Returns the requested input
        """

    def __getattr__(self, key):
        logging.debug('Loading input from %s: "%s"', self.__class__.__name__, key)
        return self.get(key)

    def __getitem__(self, key):
        return self.__getattr__(key)
