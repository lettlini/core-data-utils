from core_data_utils.datasets import BaseDataSet


def test_empty_dataset():
    bds = BaseDataSet()

    assert len(bds) == 0
    assert not bds._data_identifiers
    assert len(bds._data) == 0


def test_simple_dataset():
    example_data = {str(i): 2 * i for i in range(9)}

    sds = BaseDataSet(None, example_data, None)

    print(sds._data_identifiers)
    assert len(sds) == 9

    for idx, entry in enumerate(sds):
        assert entry.identifier == str(idx)
        assert entry.data == 2 * idx


def test_data_independence():
    example_data = {i: 2 * i for i in range(500)}

    sds = BaseDataSet(None, example_data, None)

    example_data[0] = -1

    assert sds[0].data == 0
