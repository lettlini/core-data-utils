from __future__ import annotations

import os
import pickle
from copy import deepcopy
from collections import namedtuple

BaseDataSetEntry = namedtuple("BaseDataSetEntry", ["identifier", "data"])


class BaseDataSet:
    _num_entries: int
    _data: dict
    _identifiers: list[str]

    def __init__(self, data: dict = None):
        if data is None:
            self._data = {}
            self._num_entries = 0
            self._identifiers = []
        else:
            self._data = data
            self._num_entries = len(data)
            self._identifiers = list(self._data.keys())
            self._identifier_ordering()

    def __len__(self) -> int:
        return self._num_entries

    def _identifier_ordering(self) -> None:
        self._identifiers.sort()

    def __repr__(self) -> str:
        repr_str: str = f"DataSet with {self._num_entries} entries:"
        for idx in range(min(5, len(self))):
            tid, dat = self[idx]
            repr_str += f"\n\t{tid}: {type(dat)}"

        return repr_str

    def __getitem__(self, identifier: str | int) -> tuple:
        if isinstance(identifier, str):
            if identifier not in self._identifiers:
                raise IndexError(f"'{identifier}' is not a valid identifier.")
            return BaseDataSetEntry(identifier=identifier, data=self._data[identifier])

        if isinstance(identifier, int):
            if identifier >= self._num_entries:
                raise IndexError(
                    f"Index {identifier} out of bounds for BaseDataSet with {self._num_entries} entries."
                )
            return BaseDataSetEntry(
                identifier=self._identifiers[identifier],
                data=self._data[self._identifiers[identifier]],
            )

        raise ValueError(
            f"BaseDataSet.__getitem__ has not been implemented for argument type {type(identifier)}"
        )

    def copy(self) -> BaseDataSet:
        return self.__class__(deepcopy(self._data))

    def __copy__(self) -> BaseDataSet:
        return self.copy()

    def to_pickle(self, fpath: str, mkdir: bool = False) -> None:
        if mkdir:
            os.makedirs(os.path.dirname(fpath), exist_ok=True)

        with open(fpath, "wb") as save_file:
            pickle.dump(self, save_file)

    def keys(self) -> list[str]:
        return self._identifiers
