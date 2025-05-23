from __future__ import annotations

import os
import pickle
from collections.abc import Hashable
from copy import deepcopy
from typing import Any, Optional


class BaseDataSetEntry:

    def __init__(
        self, identifier: Hashable, data: Any, metadata: Optional[dict] = None
    ) -> None:
        self._identifier = identifier
        self._data = data
        self._metadata = metadata if metadata is not None else {}

    @property
    def identifier(self) -> Hashable:
        return self._identifier

    @property
    def data(self) -> str:
        return self._data

    @data.getter
    def data(self) -> str:
        return self._data

    @property
    def metadata(self) -> dict:
        return self._metadata

    def __repr__(self) -> str:
        reprstr = f"BaseDataSetEntry \n\t identifier:\t '{self._identifier}'"
        reprstr += f"\n\t data: \t\t {type(self._data).__name__}"

        if not self._metadata:
            reprstr += "\n\t metadata: \t <empty>"

        else:
            reprstr += f"\n\t metadata: \t{str(self._metadata)}"

        return reprstr


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
            list[BaseDataSetEntry] | dict[Hashable, BaseDataSetEntry]
        ] = None,
    ) -> None:

        # initialize to empty dataset
        self._metadata: dict[str, Any] = (
            deepcopy(ds_metadata) if ds_metadata is not None else {}
        )
        self._data_identifiers: list[Hashable] = []
        self._data: dict[Hashable, BaseDataSetEntry] = {}

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

    def __getitem__(self, index: int) -> Any:

        if isinstance(index, int):
            if (index >= len(self)) or (index < 0):
                raise IndexError(
                    f"Index '{index}' out of bounds for '{self.__class__}' of length '{len(self)}'."
                )
            return self._data[self._data_identifiers[index]]

        if isinstance(index, slice):
            # Handle slice notation (e.g., obj[1:10])
            start = index.start
            stop = index.stop
            step = index.step if index.step is not None else 1

            if step < 1:
                raise ValueError(
                    f"Step of slice has to be a positive integer >=1, got '{step}'"
                )
            if start >= stop:
                raise ValueError(
                    f"'start' of slice has to be strictly less than 'stop' of slice, got start='{start}', stop='{stop}'"
                )

            entries_to_return = []

            cidx = start
            while cidx < stop:
                entries_to_return.append(self[cidx])
                cidx += step

            return BaseDataSet(
                ds_metadata=self._metadata, dataset_entries=entries_to_return
            )

        raise ValueError(
            f"Indices have to be integers, got index of type {type(index)}"
        )

    def keys(self) -> list[Hashable]:
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
        cls, data_dict: dict[Hashable, Any], metadata: Optional[dict] = None
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

    def get_with_identifier(self, identifier: Hashable) -> BaseDataSetEntry:
        if identifier not in self._data_identifiers:
            raise ValueError(f"Identifier {identifier} is not a valid key.")
        return self._data[identifier]
