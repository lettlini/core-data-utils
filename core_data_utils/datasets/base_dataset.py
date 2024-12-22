from __future__ import annotations

import os
import pickle
from copy import deepcopy


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
            self._identifiers.sort()

    def __len__(self) -> int:
        return self._num_entries

    def __repr__(self) -> str:
        repr_str: str = f"DataSet with {self._num_entries} entries:"
        for idx in range(min(5, len(self))):
            tid, dat = self[idx]
            repr_str += f"\n\t{tid}: {type(dat)}"

        return repr_str

    def __getitem__(self, identifier: str | int) -> tuple:
        if isinstance(identifier, str):
            if identifier not in self._identifiers:
                raise IndexError(f"Identifier {identifier} does not exist.")
            return self._data[identifier]

        if isinstance(identifier, int):
            if identifier >= self._num_entries:
                raise IndexError("Index out of Bounds")
            return (
                self._identifiers[identifier],
                self._data[self._identifiers[identifier]],
            )

    def copy(self) -> BaseDataSet:
        return self.__class__(deepcopy(self._data))

    def __copy__(self) -> BaseDataSet:
        return self.copy()

    def to_pickle(self, fpath: str, mkdir: bool = True) -> None:
        if mkdir:
            os.makedirs(os.path.dirname(fpath), exist_ok=True)

        with open(fpath, "wb") as save_file:
            pickle.dump(self, save_file)

    def keys(self) -> list[str]:
        return self._identifiers
