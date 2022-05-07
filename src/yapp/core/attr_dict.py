class AttrDict(dict):
    """
    Extends dict so that elements can be accessed as attributes
    """

    @staticmethod
    def recursive_convert(obj):
        """
        Makes all dict inside obj AttrDict
        """
        if isinstance(obj, dict):
            obj = AttrDict(obj)
        elif isinstance(obj, (list, set)):
            obj = type(obj)(map(AttrDict.recursive_convert, obj))
        return obj

    def __init__(self, other=None, **kwargs):
        """Create a new AttrDict
        Args:
            self (None, mapping, iterable):
        """
        if other:
            super().__init__(other, **kwargs)
        else:
            super().__init__(**kwargs)
        self.__dict__ = self
        for k in self:
            self[k] = AttrDict.recursive_convert(self[k])
