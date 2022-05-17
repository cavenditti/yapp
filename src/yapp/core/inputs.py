import logging

from .attr_dict import AttrDict


class Inputs(dict):
    """
    Inputs implementation (just dict with some utility methods)
    """

    def __init__(self, *args, sources=None, config=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.exposed = {}  # mapping name to source
        self.sources = {}
        self.config = AttrDict(config)
        if not sources:
            return
        for source in sources:
            self.register(source.__class__.__name__, source)

    def __str__(self):
        return f"<yapp inputs {len(self)}>"

    def __repr__(self):
        keys = set(self.keys()) | set(self.keys())
        return f"<yapp inputs {len(self)} {keys}>"

    def __getitem__(self, key):
        try:
            logging.debug('Using input "%s"', key)
            # if it's an exposed resource from an adapter return it
            if key in self.exposed:
                source, name = self.exposed[key]
                return self.sources[source][name]
            return super().__getitem__(key)
        except KeyError as error:
            # allow accessing config from jobs
            # not sure if this will remain or not (for sure not here)
            # I didn't add self['config'] in __init__ to keep the right length
            if key == "config":
                return self.config

            logging.debug('%s Trying to load missing input "%s"', self.__repr__(), key)
            raise KeyError(f'Trying to load missing input "{key}"') from error

    def __setitem__(self, key, value):
        if key in self.exposed:
            raise ValueError("Cannot assign to exposed input from adapter")
        super().__setitem__(key, value)

    def update(self, other, **kwargs):
        if isinstance(other, Inputs):
            self.sources.update(other.sources)
            self.exposed.update(other.exposed)
        super().update(other, **kwargs)

    def __or__(self, _):
        raise NotImplementedError

    def register(self, name: str, adapter):
        """
        New input adapter (just a new Item)
        """
        self.sources[name] = adapter
        logging.info('Registered new input source: "%s"', name)

    def expose(self, source, internal_name, name):
        """
        Expose input attribute using another name
        """
        # add an empty value instead of overriding all special methods like __len__, __contains__
        self[name] = None
        self.exposed[name] = (source, internal_name)
        logging.info('Exposed "%s" from "%s" as "%s"', internal_name, source, name)
