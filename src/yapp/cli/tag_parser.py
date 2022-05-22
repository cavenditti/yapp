from abc import ABC, abstractmethod
from typing import final

import yaml


class TagParser(ABC):
    """
    TagParser subclasses are used to define new special tags used inside "pypelines.yml"
    """

    tag = "mytagname"

    def __new__(cls, *_, **__):
        if cls.tag is TagParser.tag:
            raise NotImplementedError('TagParser subclasses must define a "tag"')
        return object.__new__(cls)

    @final
    def register(self, normalize_name=True):
        """
        Register the constructor for yaml parsing
        """
        tag = self.tag
        if normalize_name:
            tag.lstrip("!")
            tag.rstrip(":")
            tag = "!" + tag
            yaml.add_constructor(tag, self.constructor)
            # if we are normalizing the name, also add a variant ending in ":"
            yaml.add_constructor(tag + ":", self.constructor)
        else:
            yaml.add_constructor(tag, self.constructor)

    @abstractmethod
    def constructor(self, loader, node):
        """
        Define here the constructors for yaml tags
        """
