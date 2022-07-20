import os
from contextlib import contextmanager
from copy import copy
from dataclasses import asdict, dataclass, field
from json import dump, load
from pathlib import Path
from typing import Collection, Dict, Generator, Iterable, List, Optional

import fsspec
from fsspec.spec import AbstractFileSystem
from fsspec.utils import get_protocol

from ldb.exceptions import LDBException, StorageConfigurationError
from ldb.fs import posix_path as fsp
from ldb.fs.utils import has_protocol
from ldb.path import Filename
from ldb.typing import JSONDecoded

FSOptions = Dict[str, JSONDecoded]


@dataclass
class StorageLocation:
    path: str = ""
    protocol: str = "file"
    fs_id: str = ""
    read_access_verified: bool = False
    write_access_verified: bool = False
    read_and_add: bool = False
    options: FSOptions = field(default_factory=dict)


@dataclass
class StorageConfig:
    locations: List[StorageLocation] = field(default_factory=list)


def get_storage_locations(ldb_dir: Path) -> List[StorageLocation]:
    storage_path = ldb_dir / Filename.STORAGE
    if storage_path.is_file():
        return load_from_path(storage_path).locations
    return []


def get_containing_storage_location(
    path: str,
    protocol: str,
    storage_locations: Collection[StorageLocation],
) -> Optional[StorageLocation]:
    for loc in storage_locations:
        if has_protocol(loc.protocol, protocol) and fsp.isin(path, loc.path):
            return loc
    return None


def get_filesystem(
    path: str,
    protocol: str,
    storage_locations: Collection[StorageLocation],
) -> AbstractFileSystem:
    storage_location = get_containing_storage_location(
        path,
        protocol,
        storage_locations,
    )
    fs_options = {} if storage_location is None else storage_location.options
    return fsspec.filesystem(protocol, **fs_options)


def load_from_path(path: Path) -> StorageConfig:
    with path.open() as file:
        config_dict = load(file)
    return StorageConfig(
        locations=[StorageLocation(**loc) for loc in config_dict["locations"]],
    )


def save_to_path(storage_config: StorageConfig, path: Path) -> None:
    config_dict = asdict(storage_config)
    with path.open("w") as file:
        dump(config_dict, file)


def create_storage_location(
    path: str = "",
    protocol: str = "",
    read_and_add: bool = False,
    options: Optional[FSOptions] = None,
) -> StorageLocation:
    if not protocol:
        protocol = get_protocol(path)
    fs_cls = fsspec.get_filesystem_class(protocol)
    if protocol == "file":
        path = os.path.abspath(path)
    path = fs_cls._strip_protocol(path)  # pylint: disable=protected-access
    return StorageLocation(
        path=path,
        protocol=protocol,
        read_access_verified=os.access(path, os.R_OK),
        write_access_verified=os.access(path, os.W_OK),
        read_and_add=read_and_add,
        options=options if options is not None else {},
    )


@contextmanager
def edit(path: Path) -> Generator[StorageConfig, None, None]:
    try:
        storage_config: StorageConfig = load_from_path(path)
    except FileNotFoundError:
        storage_config = StorageConfig()
    yield storage_config
    save_to_path(storage_config, path)


def add_storage(
    storage_config_filepath: Path,
    storage_location: StorageLocation,
    force: bool = False,
) -> None:
    storage_location = copy(storage_location)
    with edit(storage_config_filepath) as storage_config:
        new_locations = []
        children = []
        to_replace = None
        for loc in storage_config.locations:
            keep = True
            if loc.protocol == storage_location.protocol:
                old_path = loc.path
                new_path = storage_location.path
                if old_path == new_path:
                    if (
                        storage_location.read_and_add,
                        storage_location.options,
                    ) != (loc.read_and_add, loc.options):
                        to_replace = loc
                        keep = False
                    else:
                        raise LDBException(
                            "The storage location "
                            f"{repr(storage_location.path)} already exists "
                            "with the given options",
                        )
                elif fsp.isin(new_path, old_path):
                    raise LDBException(
                        f"{repr(storage_location.path)} is inside existing "
                        f"storage location {repr(loc.path)}",
                    )
                elif fsp.isin(old_path, new_path):
                    children.append(loc)
                    keep = False
            if keep:
                new_locations.append(loc)

        if to_replace is not None:
            output = get_update_output(to_replace, storage_location)
        elif children:
            children_str = "\n".join(
                [f"  {repr(loc.path)}" for loc in children],
            )
            if not force:
                raise LDBException(
                    f"{repr(storage_location.path)} is a parent of "
                    f"existing storage locations:\n"
                    f"{children_str}\n"
                    "Use the --force option to replace them",
                )
            output = (
                "Added storage location {repr(storage_location.path)}\n"
                "Removed its children:\n"
                f"{children_str}"
            )
        else:
            output = f"Added storage location {repr(storage_location.path)}"

        new_locations.append(storage_location)
        validate_storage_locations(new_locations)
        storage_config.locations = new_locations
    print(output)


def get_update_output(old: StorageLocation, new: StorageLocation) -> str:
    old_dict = asdict(old)
    new_dict = asdict(new)
    updates = [(k, old_dict[k], new_dict[k]) for k in new_dict]
    update_str = "\n".join(
        [f"  {k}: {repr(o)} -> {repr(n)}" for k, o, n in updates if o != n],
    )
    return "Updated storage location " f"{repr(new.path)}:\n" f"{update_str}\n"


def validate_storage_locations(locations: Iterable[StorageLocation]) -> None:
    if sum(loc.read_and_add for loc in locations) > 1:
        raise StorageConfigurationError(
            "Only one storage location may be set as read-add",
        )
