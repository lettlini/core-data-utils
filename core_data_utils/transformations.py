from typing import Any, Iterable

from .datasets import BaseDataSet, BaseDataSetEntry


class BaseFilter:

    def __init__(self) -> None:
        self._setup()

    def _setup(self) -> None:
        pass

    def __call__(self, dataset: BaseDataSet) -> BaseDataSet:
        new_data: dict = {}

        dataset = dataset.copy()

        for idx, c_ds_entry in enumerate(dataset):

            c_ds_entry: BaseDataSetEntry = dataset[idx]

            if self._filter_decision_single_entry(idx, c_ds_entry):
                new_data[c_ds_entry.identifier] = c_ds_entry.data

        new_ds = self._post_processing(new_data)

        return new_ds

    def _filter_decision_single_entry(
        self, index: int, ds_entry: BaseDataSetEntry
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

    def _setup(self) -> None:
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
            new_ds_entry: BaseDataSetEntry = self._transform_single_entry(
                self._merge_entries([d[identifier] for d in datasets])
            )
            new_data_dict[new_ds_entry.identifier] = new_ds_entry.data

        new_ds = self._post_processing(new_data_dict)

        return new_ds

    def _merge_entries(self, entries: Iterable[BaseDataSetEntry]) -> BaseDataSetEntry:
        return BaseDataSetEntry(
            identifier=entries[0].identifier, data=tuple((e.data for e in entries))
        )

    def _transform_single_entry(self, entry: BaseDataSetEntry) -> tuple[str, Any]:
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

    def _merge_entries(self, entries: Iterable[BaseDataSetEntry]) -> BaseDataSetEntry:
        assert len(entries) == 1
        return entries[0]

    def _transform_single_entry(self, entry: BaseDataSetEntry) -> BaseDataSetEntry:
        return super()._transform_single_entry(entry)
