import pytest

from core_data_utils.datasets import BaseDataSetEntry


def test_setting_data():
    example_entry = BaseDataSetEntry(identifier="test", data=100)

    # try to change the data of the dataset entry:
    with pytest.raises(AttributeError):
        example_entry.data *= 2


def test_setting_identifier():
    example_entry = BaseDataSetEntry(identifier="test", data=100)

    # try to change the data of the dataset entry:
    with pytest.raises(AttributeError):
        example_entry.data *= 2


def test_setting_metadata():
    example_entry = BaseDataSetEntry(identifier="test", data=100, metadata={})

    # try to change the meta-data of the dataset entry:
    with pytest.raises(AttributeError):
        example_entry.metadata |= {"metadata": "changed"}
