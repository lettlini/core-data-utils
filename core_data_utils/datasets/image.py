from __future__ import annotations

import os

try:
    import cv2
except ModuleNotFoundError as mnferr:
    raise ModuleNotFoundError(
        "'opencv' is a required dependency for using 'ImageDataSet'"
    ) from mnferr

from .base_dataset import BaseDataSet, BaseDataSetEntry


class ImageDataset(BaseDataSet):

    @classmethod
    def from_directory(
        cls,
        directory: str,
    ) -> ImageDataset:

        if not os.path.isdir(directory):
            raise ValueError(f"{directory} is not a valid directory path.")

        data: dict = {}

        filenames: list[str] = [
            f
            for f in os.listdir(directory)
            if os.path.isfile(os.path.join(directory, f))
        ]
        filenames.sort()

        for filename in filenames:
            image = cv2.cvtColor(
                cv2.imread(os.path.join(directory, filename), cv2.IMREAD_COLOR),
                cv2.COLOR_BGR2RGB,
            )
            data[filename] = BaseDataSetEntry(
                identifier=filename, data=image, metadata={}
            )

        return cls(ds_metadata=None, dataset_entries=data)
