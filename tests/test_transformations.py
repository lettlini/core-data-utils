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


def test_serial_parallel():
    st = SquareNumTransformation()

    example_data = {str(i): 2 * i for i in range(9)}

    ods = BaseDataSet.from_flat_dicts(example_data)

    serial_ds = st(dataset=ods, parallel=False)
    parallel_ds = st(dataset=ods, parallel=True, cpus=2)

    assert serial_ds._data_identifiers == parallel_ds._data_identifiers

    for index in range(len(serial_ds)):
        assert serial_ds[index].identifier == parallel_ds[index].identifier
        assert serial_ds[index].data == parallel_ds[index].data
