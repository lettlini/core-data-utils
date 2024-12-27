from __future__ import annotations

from .base_dataset import BaseDataSet

try:
    import networkx as nx
except ModuleNotFoundError as mnferr:
    raise ModuleNotFoundError(
        "'networkx' is a required dependecy for using GraphDataSet"
    ) from mnferr


class GraphDataSet(BaseDataSet):
    pass
