"""
Provider abstractions for DocWrangler data ingress/egress.

This package exposes pluggable dataset providers (local filesystem vs RT
storage) and pipeline sinks (local vs RT specs bucket). Routers select the
implementation based on environment flags.
"""

from .datasets import (
    DatasetLoadResult,
    DatasetProvider,
    LocalDatasetProvider,
    RTDatasetProvider,
)
from .pipelines import (
    PipelineSaveResult,
    PipelineSink,
    LocalPipelineSink,
    RTPipelineSink,
)

__all__ = [
    "DatasetLoadResult",
    "DatasetProvider",
    "LocalDatasetProvider",
    "RTDatasetProvider",
    "PipelineSaveResult",
    "PipelineSink",
    "LocalPipelineSink",
    "RTPipelineSink",
]
