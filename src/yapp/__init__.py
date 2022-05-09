"""
yapp — Yet Another Python (data) Pipeline
"""
from .core import InputAdapter, Inputs, Job, OutputAdapter, Pipeline, Monitor
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
    "Monitor",
]

__version__ = version
