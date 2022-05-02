import logging

class Inputs(dict):
    """
    Inputs implementation (just dict with some utility methods)
    """

    def __init__(self, *args, sources=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.exposed = {}  # mapping name to source
        self.sources = {}
        if not sources:
            return
        for source in sources:
            self.register(source.__class__.__name__, source)

    def __str__(self):
        keys = set(self.keys())
        return f"<yapp inputs {len(self)} {keys}>"

    def __getitem__(self, key):
        try:
            logging.debug('Using input "%s"', key)
            # if it's an exposed resource from an adapter return it
            if key in self.exposed:
                source, name = self.exposed[key]
                return self.sources[source].get(name)
            return super().__getitem__(key)
        except KeyError as error:
            logging.debug('%s Trying to load missing input "%s"', self.__repr__(), key)
            raise KeyError(f'Trying to load missing input "{key}"') from error

    def __setitem__(self, key, value):
        if key in self.exposed:
            raise ValueError("Cannot assign to exposed input from adapter")
        super().__setitem__(key, value)

    def __contains__(self, key):
        return super().__contains__(key) or self.exposed.__contains__(key)

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
        self.exposed[name] = (source, internal_name)
        logging.info('Exposed "%s" from "%s" as "%s"', internal_name, source, name)
