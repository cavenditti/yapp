import json
import logging


class AttrDict(dict):
    """
    Extends dict so that elements can be accessed as attributes
    """

    @staticmethod
    def recursive_convert(obj):
        if isinstance(obj, dict):
            obj = AttrDict(obj)
        elif isinstance(obj, (list, set)):
            obj = type(obj)(map(AttrDict.recursive_convert, obj))
        return obj

    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self
        for k in self:
            self[k] = AttrDict.recursive_convert(self[k])

    def __getattr__(self, attr):
        logging.error(f"no attr named {attr}")
        logging.error(json.dumps(self, indent=4))
