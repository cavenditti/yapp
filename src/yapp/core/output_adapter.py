import logging
from abc import ABC, abstractmethod


class OutputAdapter(ABC):
    """
    Abstract output Adapter

    An output adapter represents a specific output destination
    """

    @abstractmethod
    def save(self, key, data):
        """
        Save data here
        """

    def __setitem__(self, key, data):
        logging.debug('Saving output to %s: "%s"', self.__class__.__name__, key)
        return self.save(key, data)
