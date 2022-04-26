"""
yapp — Yet Another Python (data) Pipeline
"""
from .core import InputAdapter, Inputs, Job, OutputAdapter, Pipeline
from ._version import version

__all__ = [
    "core",
    "adapters",
    "cli",
    "Pipeline",
    "Job",
    "Inputs",
    "InputAdapter",
    "OutputAdapter",
]

__version__ = version
