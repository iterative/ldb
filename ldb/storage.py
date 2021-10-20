import os
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from json import dump, load
from pathlib import Path, PurePath
from typing import Generator, List

from ldb.exceptions import LDBException


@dataclass
class StorageLocation:
    path: str = ""
    protocol: str = ""
    fs_id: str = ""
    read_access_verified: bool = False
    write_access_verified: bool = False
    read_and_add: bool = True


@dataclass
class StorageConfig:
    locations: List[StorageLocation] = field(default_factory=list)


def load_from_path(path: Path) -> StorageConfig:
    with path.open() as file:
        config_dict = load(file)
    config_dict["locations"] = [
        StorageLocation(**loc) for loc in config_dict["locations"]
    ]
    return StorageConfig(**config_dict)


def save_to_path(storage_config: StorageConfig, path: Path) -> None:
    config_dict = asdict(storage_config)
    with path.open("w") as file:
        dump(config_dict, file)


def create_storage_location(
    path: str = "",
    protocol: str = "",
    add: bool = False,
):
    return StorageLocation(
        path=os.fspath(Path(path).absolute()),
        protocol=protocol,
        read_access_verified=os.access(path, os.R_OK),
        write_access_verified=os.access(path, os.W_OK),
        read_and_add=add,
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
):
    with edit(storage_config_filepath) as storage_config:
        new_locations = []
        children = []
        for loc in storage_config.locations:
            new_is_parent = False
            if loc.fs_id == storage_location.fs_id:
                old_path = PurePath(loc.path)
                new_path = PurePath(storage_location.path)
                if old_path == new_path:
                    raise LDBException(
                        f"The storage location {repr(storage_location.path)} "
                        "already exists",
                    )
                if old_path in new_path.parents:
                    raise LDBException(
                        f"{repr(storage_location.path)} is inside existing "
                        f"storage location {repr(loc.path)}",
                    )
                if new_path in old_path.parents:
                    new_is_parent = True
            if new_is_parent:
                children.append(loc)
            else:
                new_locations.append(loc)
        if children:
            children_str = "\n".join(
                ["  " + repr(loc.path) for loc in children],
            )
            if force:
                print(
                    "Removing children of parent storage location "
                    f"{repr(storage_location.path)}:\n"
                    f"{children_str}\n",
                )
            else:
                raise LDBException(
                    f"{repr(storage_location.path)} is a parent of "
                    f"existing storage locations:\n"
                    f"{children_str}\n"
                    "Use the --force option to replace them",
                )
        new_locations.append(storage_location)
        storage_config.locations = new_locations
