import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator

from ldb.dataset import iter_datasets
from ldb.exceptions import DatasetNotFoundError
from ldb.path import InstanceDir
from ldb.utils import format_dataset_identifier, parse_dataset_identifier


@dataclass
class DSListing:
    name: str
    latest_version: int


def ds(
    ldb_dir: Path,
) -> Iterator[DSListing]:
    for dataset in iter_datasets(ldb_dir):
        yield DSListing(
            name=dataset.name,
            latest_version=len(dataset.versions),
        )


def print_ds_listings(ds_listings: Iterable[DSListing]) -> int:
    num_listings = 0
    for item in ds_listings:
        print(format_dataset_identifier(item.name, item.latest_version))
        num_listings += 1
    return num_listings


def delete_datasets(ldb_dir: Path, ds_identifiers: Iterable[str]) -> None:
    ds_dir = os.path.join(ldb_dir, InstanceDir.DATASETS)
    ds_info = []
    for ds_ident in ds_identifiers:
        name, version = parse_dataset_identifier(ds_ident)
        if version is not None:
            raise ValueError(
                f"expected dataset identifier without version, got {ds_ident}",
            )
        path = os.path.join(ds_dir, name)
        ds_info.append((ds_ident, path))

    # fail at the beginning if any datasets are missing
    for ds_ident, path in ds_info:
        if not os.path.isfile(path):
            raise DatasetNotFoundError(
                f"Dataset not found: {ds_ident}",
            )

    for ds_ident, path in ds_info:
        try:
            os.unlink(path)
        except FileNotFoundError as exc:
            raise DatasetNotFoundError(
                f"Dataset not found: {ds_ident}",
            ) from exc
        print(f"Deleted {ds_ident}")
