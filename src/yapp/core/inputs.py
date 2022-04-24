import logging

from .attr_dict import AttrDict


class Inputs(AttrDict):
    """
    Inputs implementation (just AttrDict with some utility methods)
    """

    def __init__(self, *args, sources=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.exposed = {}  # mapping name to source
        if not sources:
            return
        for source in sources:
            self.register(source.__class__.__name__, source)

    def __str__(self):
        keys = set(self.keys()) - {"exposed"}
        return f"<yapp inputs {len(self)} {keys}>"

    def __len__(self):
        return super().__len__() + len(self.exposed) - 1  # skip exposed

    def __getitem__(self, key):
        try:
            logging.debug('Using input "%s"', key)
            # if it's an exposed resource from an adapter return it
            if key in self.exposed:
                source, name = self.exposed[key]
                return self[source].get(name)
            return super().__getitem__(key)
        except KeyError as error:
            logging.debug('%s Trying to load missing input "%s"', self.__repr__(), key)
            if key in self.exposed:
                source, name = self.exposed[key]
                logging.debug('"%s" exposed by "%s" as "%s"', name, source, key)
            raise KeyError(f'Trying to load missing input "{key}"') from error

    def __getattr__(self, key):
        return self[key]

    def __setitem__(self, key, value):
        if key in self.exposed:
            raise ValueError("Cannot assign to exposed input from adapter")
        super().__setitem__(key, value)

    def merge(self, other: dict):
        """
        Merges new input into current object
        """
        logging.debug("Merging %s into inputs", list(other.keys()))
        self.__dict__.update(other)
        return self

    def __or__(self, other):
        return self.merge(other)  # TODO union operator should not work in place

    def register(self, name: str, adapter):
        """
        New input adapter (just a new Item)
        """
        self[name] = adapter
        logging.info('Registered new input source: "%s"', name)

    def expose(self, source, internal_name, name):
        """
        Expose input attribute using another name
        """
        self.exposed[name] = (source, internal_name)
        logging.info('Exposed "%s" from "%s" as "%s"', internal_name, source, name)
