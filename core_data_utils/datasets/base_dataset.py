from __future__ import annotations

import os
import pickle
from collections import namedtuple
from copy import deepcopy
from typing import Any, Optional

BaseDataSetEntry = namedtuple("BaseDataSetEntry", ["identifier", "data", "metadata"])


class BaseDataSet:
    """
    Class for storing datasets.

    Args:
        ds_metadata (dict): Dataset-level metadata.
        data (dict): Data to store in the dataset.
        data_metadata (dict): dataset entry-level metadata.
    """

    def __init__(
        self,
        ds_metadata: Optional[dict[str, Any]] = None,
        dataset_entries: Optional[
            list[BaseDataSetEntry] | dict[str, BaseDataSetEntry]
        ] = None,
    ) -> None:

        # initialize to empty dataset
        self._metadata: dict[str, Any] = (
            deepcopy(ds_metadata) if ds_metadata is not None else {}
        )
        self._data_identifiers: list[str] = []
        self._data: dict[str, BaseDataSetEntry] = {}

        if dataset_entries is not None:
            if isinstance(dataset_entries, list):
                for entry in dataset_entries:
                    self._data[entry.identifier] = entry

            elif isinstance(dataset_entries, dict):
                for identifier, entry in dataset_entries.items():
                    assert identifier == entry.identifier
                    self._data[identifier] = entry

            self._data_identifiers = list(self._data.keys())

        # process supplied input data records
        self._sort_identifiers()

    def _sort_identifiers(self) -> None:
        self._data_identifiers.sort()

    def __len__(self):
        return len(self._data_identifiers)

    def __getitem__(self, index: int | str) -> Any:

        if isinstance(index, int):
            if (index >= len(self)) or (index < 0):
                raise IndexError(
                    f"Index '{index}' out of bounds for '{self.__class__}' of length '{len(self)}'."
                )
            return deepcopy(self._data[self._data_identifiers[index]])

        if isinstance(index, str):
            if index not in self._data_identifiers:
                raise ValueError(f"Unknown key '{index}'.")
            return deepcopy(self._data[index])

        raise ValueError(f"Indexing with index of type '{type(index)}' unsupported.")

    def keys(self) -> list[str]:
        """
        Return list of data identifiers.

        Returns:
            (list[str]): list containing all data identifiers present in
                the dataset.
        """
        return deepcopy(self._data_identifiers)

    @property
    def metadata(self) -> dict:
        """
        Return dataset-level metadata that can be edited.

        Returns:
            (dict): dataset-level metadata
        """
        return self._metadata

    def to_dict(self) -> dict:
        """
        Return dataset in form of a nested dictionary.

        Returns:
            (dict): Dataset data in the form of a nested dictionary with the structure:
                {metadata, {data, metadata}}
        """
        return {
            "metadata": self._metadata,
            "data": self._data,
        }

    @classmethod
    def from_flat_dicts(
        cls, data_dict: dict[str, Any], metadata: Optional[dict] = None
    ) -> BaseDataSet:
        ds_entries: list[BaseDataSetEntry] = [
            BaseDataSetEntry(identifier=k, data=v, metadata={})
            for k, v in data_dict.items()
        ]
        return cls(ds_metadata=metadata, dataset_entries=ds_entries)

    def to_pickle(self, fpath: str, mkdir: bool = False) -> None:
        """
        Save instance data by serializing data dictionary to a pickle file.
        Args:
            fpath (str): File path of pickle file to which data dictionary
                should be serialized.
        """
        if mkdir:
            os.makedirs(os.path.dirname(fpath), exist_ok=True)

        with open(fpath, "wb") as save_file:
            pickle.dump(self.to_dict(), save_file)

    @classmethod
    def from_pickle(cls, fpath: str) -> BaseDataSet:
        """
        Load data into new instance of 'BaseDataSet'.
        Args:
            fpath (str): File path of pickle file to which data dictionary
                was serialzed.
        Returns:
            (BaseDataSet): New 'BaseDataSet' instance containing loaded data.
        """
        with open(fpath, "rb") as read_file:
            ds_dict = pickle.load(read_file)

        return cls(
            ds_metadata=ds_dict["metadata"],
            dataset_entries=ds_dict["data"],
        )

    def __repr__(self) -> str:
        reprstr: str = f"{self.__class__} with {len(self)} entries: \n"
        if self._metadata:
            reprstr += f"\t {self._metadata} \n"

        maxidx = min(len(self), 7)
        for i in range(maxidx):
            entry = self[i]
            if i == maxidx - 1:
                reprstr += (
                    f"\t └─── ({i}) {entry.identifier}: {entry.data.__class__} \n"
                )
            else:
                reprstr += (
                    f"\t ├─── ({i}) {entry.identifier}: {entry.data.__class__} \n"
                )
        return reprstr

    def copy(self) -> BaseDataSet:
        """
        Create a (deep) copy of the dataset
        Returns:
            (BaseDataSet): a fully independent copy of the dataset.
        """
        independent_ds_dict = deepcopy(self.to_dict())

        return BaseDataSet(
            ds_metadata=independent_ds_dict["metadata"],
            dataset_entries=independent_ds_dict["data"],
        )
