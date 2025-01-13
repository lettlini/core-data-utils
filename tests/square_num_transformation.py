from typing import Any

from core_data_utils.datasets import BaseDataSet, BaseDataSetEntry
from core_data_utils.transformations import (
    BaseDataSetTransformation,
    BaseMultiDataSetTransformation,
)


class SquareNumTransformation(BaseDataSetTransformation):
    def _transform_single_entry(
        self, entry: BaseDataSetEntry, dataset_properties: dict
    ) -> BaseDataSetEntry:
        num = entry.data

        return BaseDataSetEntry(entry.identifier, data=num**2, metadata=entry.metadata)
