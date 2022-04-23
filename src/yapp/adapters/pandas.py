import sys

import pandas as pd

from yapp.core import InputAdapter

module = sys.modules[__name__]


class FunctionWrapperInputAdapter(InputAdapter):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    # FIXME endless recursion because of redefinition of __getattr__ in InputAdapter
    def get(self, name):
        return self.fn(name, *self.args, **self.kwargs)


for fn_name in [a for a in dir(pd) if a.startswith("read_")]:
    fn = pd.__getattribute__(fn_name)

    Adapter = type(fn_name, (FunctionWrapperInputAdapter), {"fn": fn})

    setattr(module, fn_name, Adapter)
