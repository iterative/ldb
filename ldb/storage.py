import os
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from json import dump, load
from pathlib import Path, PurePath
from typing import Generator, List

from ldb.exceptions import LDBException
from ldb.path import ConfigType, Filename, get_config_dir


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
        path=path,
        protocol=protocol,
        read_access_verified=os.access(path, os.R_OK),
        write_access_verified=os.access(path, os.W_OK),
        read_and_add=add,
    )


@contextmanager
def edit_path(path: Path) -> Generator[StorageConfig, None, None]:
    try:
        storage_config: StorageConfig = load_from_path(path)
    except FileNotFoundError:
        storage_config = StorageConfig()
    yield storage_config
    save_to_path(storage_config, path)


@contextmanager
def edit(
    config_type=ConfigType.INSTANCE,
) -> Generator[StorageConfig, None, None]:
    with edit_path(
        get_config_dir(config_type) / Filename.STORAGE,
    ) as storage_config:
        yield storage_config


def add_storage(
    storage_config_filepath: Path,
    storage_location: StorageLocation,
):
    with edit_path(storage_config_filepath) as storage_config:
        for loc in storage_config.locations:
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
                    raise LDBException(
                        f"{repr(storage_location.path)} is a parent of "
                        f"existing storage location {repr(loc.path)}",
                    )
        storage_config.locations.append(storage_location)
