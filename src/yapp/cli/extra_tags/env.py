import os

from yapp.cli.tag_parser import TagParser
from yapp.core.errors import MissingEnv


class Env(TagParser):
    """
    Tag to automatically use for env variables
    """

    tag = "env"

    def constructor(self, loader, node):
        """
        Conctructor for env variables
        """
        try:
            value = loader.construct_scalar(node)
            return os.environ[value]
        except KeyError as error:
            # logging.exception(error)
            raise MissingEnv(error.args[0]) from None
