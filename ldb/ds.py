from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator

from ldb.dataset import iter_datasets
from ldb.utils import format_dataset_identifier


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


def print_ds_listings(ds_listings: Iterable[DSListing]) -> None:
    for item in ds_listings:
        print(format_dataset_identifier(item.name, item.latest_version))
