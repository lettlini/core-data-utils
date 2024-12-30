import pytest

from core_data_utils.datasets import BaseDataSet


def test_empty_dataset():
    bds = BaseDataSet()

    assert len(bds) == 0
    assert not bds._data_identifiers
    assert len(bds._data) == 0


def test_simple_dataset():
    example_data = {i: 2 * i for i in range(9)}

    sds = BaseDataSet.from_flat_dicts(
        example_data,
        None,
    )

    print(sds._data_identifiers)
    assert len(sds) == 9

    for idx, entry in enumerate(sds):
        assert entry.identifier == idx
        assert entry.data == 2 * idx


def test_data_independence():
    example_data = {i: 2 * i for i in range(500)}

    sds = BaseDataSet.from_flat_dicts(
        example_data,
        None,
    )

    example_data[0] = -1

    assert sds[0].data == 0


def test_saving_loading():
    example_data = {i: 2 * i for i in range(9)}

    sds = BaseDataSet.from_flat_dicts(
        example_data,
        None,
    )

    sds.to_pickle("/tmp/pytest/test.pickle", mkdir=True)
    lds = sds.from_pickle("/tmp/pytest/test.pickle")

    assert len(lds) == 9

    for idx, entry in enumerate(lds):
        assert entry.identifier == sds[idx].identifier
        assert entry.data == sds[idx].data


def test_dataset_copying():
    example_data = {i: 2 * i for i in range(9)}

    sds = BaseDataSet.from_flat_dicts(
        example_data,
        None,
    )

    copied_ds = sds.copy()

    _ = sds._data.pop(1)

    assert 1 not in sds._data
    assert 1 in copied_ds._data


def test_indexing_with_str():
    example_data = {str(i): 2 * i for i in range(9)}

    sds = BaseDataSet.from_flat_dicts(
        example_data,
        None,
    )

    with pytest.raises(ValueError):
        _ = sds["1"]


def test_indexing_with_int():
    example_data = {str(i): 2 * i for i in range(9)}

    sds = BaseDataSet.from_flat_dicts(
        example_data,
        None,
    )

    assert sds[0].data == 0
    assert sds[1].data == 2
