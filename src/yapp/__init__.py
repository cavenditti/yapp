"""
yapp — Yet Another Python (data) Pipeline
"""
from ._version import version
from .core import InputAdapter, Inputs, Job, Monitor, OutputAdapter, Pipeline

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
