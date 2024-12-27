import copy
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

    def _transform(
        self,
        parallel: bool = False,
        cpus: int = 6,
        copy_datasets: bool = True,
        **kwargs: dict[str, Any],
    ) -> Any:
        """
        Args:
            parallel (bool): If 'True', the transformation will be exectued in
                parallel. Default is 'False'.
            cpus (int): How many processes should be spawned when executing
                transformation in parallel. Default is '6'.
            copy_datasets (bool): Whether to create a (deep) copy of the
                input datasets. Default is 'True'
            **kwargs (dict[str, BaseDataSet]): Iterable of DataSets acting as
                input data for carrying out the transformation
        Returns:
            (Any): Result of DataSet transformation
        """

        if len(kwargs) == 0:
            raise ValueError("Length of supplied 'datasets' iterable was 0.")

        if not self._assert_compatability(kwargs):
            raise RuntimeError("Supplied DataSets are not compatible.")

        if copy_datasets:
            kwargs = copy.deepcopy(kwargs)

        new_data_list: list[BaseDataSetEntry] = []

        # prepare list of identifiers
        identifiers: list[str] = next(iter(kwargs.values())).keys()

        if not parallel:
            for identifier in identifiers:
                new_ds_entry: BaseDataSetEntry = self._transform_single_entry(
                    self._merge_entries(
                        identifier=identifier,
                        **{
                            dsname: ds[identifier].data for dsname, ds in kwargs.items()
                        },
                    )
                )
                new_data_list.append(new_ds_entry)
        else:
            # create Iterable of "entries" that can be passed to Pool.map
            entries_iterable: list[BaseDataSet] = [
                self._merge_entries(
                    identifier=identifier,
                    **{dsname: ds[identifier].data for dsname, ds in kwargs.items()},
                )
                for identifier in identifiers
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

    def _merge_entries(
        self, identifier: str, **kwargs: dict[str, BaseDataSetEntry]
    ) -> BaseDataSetEntry:

        return BaseDataSetEntry(identifier=identifier, data=kwargs)

    def _transform_single_entry(self, entry: BaseDataSetEntry) -> tuple[str, Any]:
        raise NotImplementedError(
            "'_transform_single_entry' has not been implemented yet."
        )

    def _post_processing(self, data_dict: dict[str, Any]) -> Any:
        return BaseDataSet(data_dict)


class BaseDataSetTransformation(BaseMultiDataSetTransformation):

    def _assert_compatability(self, datasets: Iterable[BaseDataSet]) -> bool:
        return True

    def _merge_entries(
        self, identifier: str, **kwargs: dict[str, BaseDataSetEntry]
    ) -> BaseDataSetEntry:
        if len(kwargs) != 1:
            raise ValueError(
                f"Expected exactly 1 data entry to merge, got {len(kwargs)}."
            )
        return BaseDataSetEntry(identifier=identifier, data=next(iter(kwargs.values())))

    def _transform_single_entry(self, entry: BaseDataSetEntry) -> BaseDataSetEntry:
        return super()._transform_single_entry(entry)

    def __call__(
        self,
        dataset: BaseDataSet,
        parallel: bool = False,
        cpus: int = 6,
        copy_datasets: bool = True,
    ) -> Any:
        return super()._transform(
            parallel=parallel, cpus=cpus, copy_datasets=copy_datasets, x=dataset
        )
