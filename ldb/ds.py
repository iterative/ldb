import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator

from ldb.dataset import (
    Dataset,
    DatasetVersion,
    get_all_dataset_version_identifiers,
    iter_datasets,
)
from ldb.exceptions import DatasetNotFoundError
from ldb.path import InstanceDir
from ldb.utils import (
    format_dataset_identifier,
    get_hash_path,
    load_data_file,
    parse_dataset_identifier,
)


@dataclass
class DSListing:
    name: str
    version_num: int
    id: str


def ds(
    ldb_dir: Path,
    all_versions: bool = False,
) -> Iterator[DSListing]:
    for dataset in sorted(iter_datasets(ldb_dir), key=lambda d: d.name):
        versions = list(dataset.numbered_versions().items())
        if not all_versions:
            versions = versions[-1:]
        for i, version_id in versions:
            if version_id is not None:
                yield DSListing(
                    name=dataset.name,
                    version_num=i,
                    id=version_id,
                )


def print_ds_listings(
    ds_listings: Iterable[DSListing],
) -> int:
    num_listings = 0
    for item in ds_listings:
        entry = format_dataset_identifier(item.name, item.version_num)
        print(entry)
        num_listings += 1
    return num_listings


def delete_datasets(ldb_dir: Path, ds_identifiers: Iterable[str]) -> None:
    ds_dir = os.path.join(ldb_dir, InstanceDir.DATASETS)
    ds_version_dir = os.path.join(ldb_dir, InstanceDir.DATASET_VERSIONS)
    collection_dir = os.path.join(ldb_dir, InstanceDir.COLLECTIONS)
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

    collection_identifiers = get_all_dataset_version_identifiers(ldb_dir)
    for ds_ident, path in ds_info:
        try:
            dataset = Dataset.parse(load_data_file(Path(path)))
        except FileNotFoundError as exc:
            raise DatasetNotFoundError(
                f"Dataset not found: {ds_ident}",
            ) from exc
        for i, ds_version in dataset.numbered_versions().items():
            version_path = get_hash_path(
                Path(ds_version_dir),
                ds_version,
            )
            version_obj = DatasetVersion.parse(load_data_file(version_path))
            ds_version_ident = format_dataset_identifier(dataset.name, i)
            refs = collection_identifiers[version_obj.collection]
            refs.remove(ds_version_ident)
            if not refs:
                collection_path = str(
                    get_hash_path(
                        Path(collection_dir),
                        version_obj.collection,
                    ),
                )
                os.unlink(collection_path)
                try:
                    os.rmdir(
                        os.path.split(collection_path.rstrip(os.path.sep))[0],
                    )
                except OSError:
                    pass
        os.unlink(path)
        print(f"Deleted {ds_ident}")
