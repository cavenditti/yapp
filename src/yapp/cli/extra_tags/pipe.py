import logging
import os

from yapp.cli.tag_parser import TagParser
from yapp.core import Job
from yapp.core.utils import load_module


class Piper(TagParser):
    """
    Adds a tag used to define steps as simple functions that only pipes last output DataFrame to a
    functions and returns a new DataFrame
    """

    tag = "pipe"

    uid = 0

    def __init__(self, pipeline_name):
        # FIXME doesn't work if current workdir is not a parent
        self.base_paths = [os.path.join("./", pipeline_name), "./"]

    def constructor(self, loader, node: dict) -> dict:
        node = loader.construct_mapping(node)
        Piper.uid += 1
        new_node = {
            "run": "yapp.cli.extra_tags.pipe.Pipe",
            "name": f"_{node['run']}_{Piper.uid}",
            "with": {
                "fn": node["run"],
                "base_paths": self.base_paths,
            },
        }

        if "with" in node:
            new_node["with"]["params"] = node["with"]
        if "module" in node:
            new_node["with"]["module"] = node["module"]
        if "after" in node:
            new_node["after"] = node["after"]

        # TODO early check module import and if function exists
        return new_node


class Pipe(Job):
    """
    Pipes last output to the function defined in the arguments, passing paras to is as kwargs
    """

    def execute(self, _last_output, *, fn, base_paths, params=None, module=None):
        input_df = None

        logging.debug(fn)

        # get function
        if not module:
            try:
                func = getattr(load_module(fn, base_paths), fn)
            except FileNotFoundError:
                module_name, func_name = fn.rsplit(".", 1)
                func = getattr(load_module(module_name, base_paths), func_name)
        else:
            func = getattr(load_module(module, base_paths), fn)
            # func = getattr(importlib.import_module(module), fn)

        # Try using first of the last values
        try:
            input_df = next(iter(_last_output.values()))
        except (AttributeError, KeyError):
            pass

        # get args to pass to function
        inner_params = {}
        if params:
            inner_params.update(params)

        return func(input_df, **inner_params)
