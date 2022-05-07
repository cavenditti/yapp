import logging
from abc import ABC, abstractmethod


class OutputAdapter(ABC):
    """
    Abstract output Adapter

    An output adapter represents a specific output destination
    """

    @property
    def name(self):
        """
        Helper to return the name of the class
        """
        return self.__class__.__name__

    @abstractmethod
    def save(self, key, data):
        """
        Save intermediate data here
        Args:
            key (str):
                Key is the name used as key in the returned dict from the Job, or if it didn't return a
                dictionary, the Job's name.
            data (dict | Any):
                Data returned from Job execution
        """

    def empty(self, job_name):  # type: ignore
        """
        Override this if you wish to save something when a Job returns nothing,
        Leave as it is you prefer ignoring it.

        Args:
            job_name (str):
                Name of the job returning None
        """

    def save_result(self, key, data):
        """
        Save final result here
        Leave it as it is you just use save
        """
        self.save(key, data)

    def _save(self, key, data):
        if data is None:
            logging.debug('Empty output to %s for job "%s"', self.name, key)
            return self.empty(key)

        logging.debug('Saving output to %s: "%s"', self.name, key)
        return self.save(key, data)

    def _save_result(self, key, data):
        logging.debug('Saving final output to %s: "%s"', self.name, key)
        return self.save_result(key, data)
