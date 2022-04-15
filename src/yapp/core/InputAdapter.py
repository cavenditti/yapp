from abc import ABC, abstractmethod
import logging


class InputAdapter(ABC):
    """
    Abstract Input Adapter

    An input adapter represents a type of input from a specific source
    """

    @abstractmethod
    def get(self, key):
        pass

    def __getattr__(self, key):
        logging.debug(f'Loading input from {self.__class__.__name__}: "{key}"')
        return self.get(key)

    def __getitem__(self, key):
        return self.__getattr__(key)
