import os
from typing import Any, Iterable

from .datasets import BaseDataSet


class BaseFilter:
    def __init__(self, name: str, out_dir: str | None = None) -> None:
        super().__init__(name, out_dir)

    def __call__(self, dataset: BaseDataSet) -> BaseDataSet:
        new_data: dict = {}

        dataset = dataset.copy()

        for idx, entry_id in enumerate(dataset.keys()):

            if self._filter_decision_single_entry(idx, dataset[entry_id]):
                new_data[entry_id] = dataset[entry_id]

        new_ds = self._post_processing(new_data)

        return new_ds

    def _filter_decision_single_entry(
        self, index: int, in_tuple: tuple[str, Any]
    ) -> bool:
        raise NotImplementedError(
            "method '_filter_decision_single_entry' has not yet been implemented"
        )

    def _post_processing(self, data_dict: dict):
        return BaseDataSet(data_dict)


class BaseMultiDataSetTransformation:

    def __init__(
        self,
    ) -> None:
        self._setup()

    def _setup(self, *args, **kwargs) -> None:
        pass

    def _assert_compatability(self, datasets: Iterable[BaseDataSet]) -> bool:
        raise NotImplementedError(
            "'_assert_compatability' has not been implemented yet."
        )

    def __call__(self, datasets: Iterable[BaseDataSet]) -> Any:

        if len(datasets) == 0:
            raise ValueError("Length of supplied 'datasets' iterable was 0.")

        if not self._assert_compatability(datasets):
            raise RuntimeError("Supplied DataSets are not compatible.")

        datasets = [ds.copy() for ds in datasets]

        new_data_dict: dict = {}

        for identifier in datasets[0].keys():
            new_data_dict[identifier] = self._transform_single_entry(
                self._pack_entry(
                    identifier=identifier, *(d[identifier] for d in datasets)
                )
            )

        new_ds = self._post_processing(new_data_dict)

        return new_ds

    def _pack_entry(self, identifier: str, *args) -> tuple[str, tuple]:
        return tuple([identifier, args])

    def _transform_single_entry(
        self, in_tuple: tuple[str, tuple[Any, ...]]
    ) -> tuple[str, Any]:
        raise NotImplementedError(
            "'_transform_single_entry' has not been implemented yet."
        )

    def _post_processing(self, data_dict: dict[str, Any]) -> Any:
        return BaseDataSet(data_dict)


class BaseDataSetTransformation(BaseMultiDataSetTransformation):

    def _assert_compatability(self, datasets: Iterable[BaseDataSet]) -> bool:
        return True

    def __call__(self, dataset: BaseDataSet) -> Any:
        return super().__call__([dataset])

    def _pack_entry(self, identifier: str, *args) -> tuple[str, tuple]:
        assert len(args) == 1
        return tuple(identifier, args[0])

    def _transform_single_entry(self, in_tuple: tuple[str, Any]) -> tuple[str, Any]:
        return super()._transform_single_entry(in_tuple)
