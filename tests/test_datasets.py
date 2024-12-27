from core_data_utils.datasets import BaseDataSet


def test_empty_dataset():
    bds = BaseDataSet()

    assert bds._num_entries == 0
    assert not bds._identifiers
    assert len(bds._data) == 0


def test_simple_dataset():
    example_data = {i: 2 * i for i in range(500)}

    sds = BaseDataSet(example_data)

    assert len(sds) == 500

    for idx, entry in enumerate(sds):
        assert entry.identifier == idx
        assert entry.data == 2 * idx
