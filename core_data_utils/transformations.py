from multiprocessing import Pool
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

            if self._filter_decision_single_entry(
                idx, c_ds_entry, **self._global_dataset_properties(dataset)
            ):
                new_data[c_ds_entry.identifier] = c_ds_entry.data

        new_ds = self._post_processing(new_data)

        return new_ds

    def _global_dataset_properties(self, _: BaseDataSet) -> dict:
        return {}

    def _filter_decision_single_entry(
        self, index: int, ds_entry: BaseDataSetEntry, **kwargs
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

    def __call__(
        self, datasets: Iterable[BaseDataSet], parallel: bool = False, cpus: int = 6
    ) -> Any:
        """
        Args:
            datasets (Iterable[BaseDataSet]): Iterable of DataSets acting as
                input data for carrying out the transformation
            parallel (bool): If 'True', the transformation will be exectued in
                parallel. Default is 'False'.
            cpus (int): How many processes should be spawned when executing
                transformation in parallel. Default is '6'.
        Returns:
            (Any): Result of DataSet transformation
        """

        if len(datasets) == 0:
            raise ValueError("Length of supplied 'datasets' iterable was 0.")

        if not self._assert_compatability(datasets):
            raise RuntimeError("Supplied DataSets are not compatible.")

        datasets = [ds.copy() for ds in datasets]

        new_data_list: list[BaseDataSetEntry] = []

        if not parallel:
            for identifier in datasets[0].keys():
                new_ds_entry: BaseDataSetEntry = self._transform_single_entry(
                    self._merge_entries([d[identifier] for d in datasets])
                )
                new_data_list.append(new_ds_entry)
        else:
            # create Iterable of "entries" that can be passed to Pool.map
            entries_iterable: list[BaseDataSet] = [
                self._merge_entries([d[identifier] for d in datasets])
                for identifier in datasets[0].keys()
            ]

            with Pool(cpus) as parpool:
                new_data_list: list[BaseDataSet] = parpool.map(
                    self._transform_single_entry, entries_iterable
                )

        new_data_dict: dict = {
            nentry.identifier: nentry.data for nentry in new_data_list
        }

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

    def __call__(
        self, dataset: BaseDataSet, parallel: bool = False, cpus: int = 6
    ) -> Any:
        return super().__call__([dataset], parallel=parallel, cpus=cpus)

    def _merge_entries(self, entries: Iterable[BaseDataSetEntry]) -> BaseDataSetEntry:
        assert len(entries) == 1
        return entries[0]

    def _transform_single_entry(self, entry: BaseDataSetEntry) -> BaseDataSetEntry:
        return super()._transform_single_entry(entry)
