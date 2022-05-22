import importlib.util
import logging
import os
import re
from copy import copy
from importlib import import_module
from types import ModuleType
from typing import Sequence

from yapp.core.errors import ImportedCodeFailed


def camel_to_snake(name: str) -> str:
    """Returns snake_case version of a CamelCase string"""
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


def load_module_from_path(paths: Sequence[str]) -> ModuleType:
    """
    Tries loading a python module from a .py file in one of base_paths
    """
    # try to load from all possible paths
    for path in paths:
        # logging.debug("Trying path %s for module %s", path, module_name)
        try:
            # Module name may differ from the original name (camel_to_snake)
            name = os.path.split(path)[-1]
            spec = importlib.util.spec_from_file_location(name, path + ".py")
        except FileNotFoundError:
            continue

        if not spec:
            continue

        module = importlib.util.module_from_spec(spec)

        try:
            # Not sure why the following line breaks everything
            # sys.modules[module_name] = module
            spec.loader.exec_module(module)  # type: ignore
        except FileNotFoundError:
            # always continue on FileNotFoundError
            continue
        except Exception as error:
            raise ImportedCodeFailed(module, *error.args) from None

        # if everything goes well stop here and don't try next possibilities
        # logging.debug("Found at %s", path)
        return module

    # if we exit loop without returning means we didn't found anything
    raise FileNotFoundError


def load_module_from_import(module_name: str, fallback_module: str) -> ModuleType:
    """
    Tries importing a python module using its name
    """
    try:
        # logging.debug("Trying to load module %s.%s", fallback_module, module_name)
        module = import_module(f"{fallback_module}.{module_name}")
        # logging.debug("Found at %s.%s", fallback_module, module_name)
    except ModuleNotFoundError:
        # logging.debug("Trying to load module %s", module_name)
        module = import_module(f"{module_name}")
        # logging.debug("Found at %s", module_name)
    return module


def load_module(
    module_name: str, base_paths: Sequence[str], fallback_module: str = "yapp.adapters"
) -> ModuleType:
    """
    Loads a python module from a .py file in one of base_paths or from a fallback module if not
    found in base_paths
    """
    logging.debug('Requested module to load "%s"', module_name)
    # Remove eventual trailing ".py" and split at dots
    ref = re.sub(r"\.py$", "", module_name).split(".")
    # same but snake_case
    ref_snake = copy(ref)
    ref_snake[-1] = camel_to_snake(ref_snake[-1])
    # return a possible path for each base_path for both possible names
    paths = [os.path.join(*[base_path] + ref) for base_path in base_paths]
    paths += [os.path.join(*[base_path] + ref_snake) for base_path in base_paths]

    try:
        module = load_module_from_path(paths)
        # if didn't find it, try from yapp
        # and if still cannot find it, raise an error
    except FileNotFoundError:
        try:
            module = load_module_from_import(module_name, fallback_module)
        except ModuleNotFoundError:
            tried_imports = [f"{fallback_module}.{module_name}", f"{module_name}"]
            raise FileNotFoundError(
                f"Cannot locate module {module_name} in {paths} or {tried_imports}"
            ) from None

    logging.debug("Found %s", module)
    return module
