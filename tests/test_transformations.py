import multiprocessing as mp
from typing import Any

from core_data_utils.datasets import BaseDataSet, BaseDataSetEntry
from core_data_utils.transformations import (
    BaseDataSetTransformation,
    BaseMultiDataSetTransformation,
)

from .square_num_transformation import SquareNumTransformation

mp.set_start_method("spawn", force=True)


def test_serial_parallel():
    st = SquareNumTransformation()

    example_data = {i: 2 * i for i in range(9)}

    ods = BaseDataSet.from_flat_dicts(example_data)

    serial_ds = st(dataset=ods)
    parallel_ds = st(dataset=ods, cpus=2)

    assert serial_ds._data_identifiers == parallel_ds._data_identifiers

    for index in range(len(serial_ds)):
        assert serial_ds[index].identifier == parallel_ds[index].identifier
        assert serial_ds[index].data == parallel_ds[index].data
