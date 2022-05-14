import logging
import sys
from abc import ABC, abstractmethod


class YappFatalError(RuntimeError, ABC):
    """
    Generic fatal exception
    """

    exit_code = 1

    @abstractmethod
    def log(self):
        """
        Used to log specific error messages
        """

    def log_and_exit(self):
        """
        Calls log and exit with the relevan error exit_code
        """
        self.log()
        sys.exit(self.exit_code)


class MissingConfiguration(YappFatalError):
    """
    Exception raised when no configuration file is found
    """

    exit_code = 2

    def log(self):
        logging.error("Missing configuration (is pipelines.yml empty?)")


class EmptyConfiguration(YappFatalError):
    """
    Exception raised when an empty configuration file is found
    """

    exit_code = 3

    def log(self):
        logging.error("pipelines.yml file not found")


class MissingPipeline(YappFatalError):
    """
    Exception raised when users requests a pipeline name not in pipelines.yml
    """

    exit_code = 4

    def log(self):
        logging.error('Pipeline "%s" not found in pipelines.yml', self.args[0])


class MissingEnv(YappFatalError):
    """
    Exception raised when an environment variable requested in config is not defined
    """

    exit_code = 5

    def log(self):
        logging.error("Enviroment variable %s undefined", self.args[0])


class ImportedCodeFailed(YappFatalError):
    """
    Exception raised when a module is found bu importing it fails
    """

    exit_code = 6

    def log(self):
        logging.error("Error while importing %s %s", self.args[0], self.args[1:])


class ConfigurationError(YappFatalError):
    """
    Exception raised when an invalid configuration file is found
    """

    exit_code = 7

    def __init__(self, config_errors, relevant_field=None):

        if not relevant_field:
            self.msg = f"Configuration errors: {config_errors}"
        else:
            self.msg = f"Configuration errors for {relevant_field}"
        super().__init__()

    def log(self):
        logging.error(self.msg)
