from abc import ABC, abstractmethod
import logging


class OutputAdapter(ABC):
    """
    Abstract output Adapter

    An output adapter represents a specific output destination
    """

    @abstractmethod
    def save(self, key, data):
        pass

    def __setitem__(self, key, data):
        logging.debug(f'Saving output to {self.__class__.__name__}: "{key}"')
        return self.save(key, data)
