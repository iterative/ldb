import os
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from json import dump, load
from pathlib import Path, PurePath
from typing import Generator, Iterable, List

from fsspec.implementations.local import make_path_posix

from ldb.exceptions import LDBException, StorageConfigurationError
from ldb.path import Filename


@dataclass
class StorageLocation:
    path: str = ""
    protocol: str = "file"
    fs_id: str = ""
    read_access_verified: bool = False
    write_access_verified: bool = False
    read_and_add: bool = False


@dataclass
class StorageConfig:
    locations: List[StorageLocation] = field(default_factory=list)


def get_storage_locations(ldb_dir: Path) -> List[StorageLocation]:
    storage_path = ldb_dir / Filename.STORAGE
    if storage_path.is_file():
        return load_from_path(storage_path).locations
    return []


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
    protocol: str = "file",
    read_and_add: bool = False,
) -> StorageLocation:
    if protocol == "file":
        path = os.path.abspath(path)
    return StorageLocation(
        path=make_path_posix(path),
        protocol=protocol,
        read_access_verified=os.access(path, os.R_OK),
        write_access_verified=os.access(path, os.W_OK),
        read_and_add=read_and_add,
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
    with edit(storage_config_filepath) as storage_config:
        new_locations = []
        children = []
        to_replace = None
        for loc in storage_config.locations:
            keep = True
            if loc.fs_id == storage_location.fs_id:
                old_path = PurePath(loc.path)
                new_path = PurePath(storage_location.path)
                if old_path == new_path:
                    if storage_location.read_and_add and not loc.read_and_add:
                        to_replace = loc
                        keep = False
                    else:
                        raise LDBException(
                            "The storage location "
                            f"{repr(storage_location.path)} already exists",
                        )
                elif old_path in new_path.parents:
                    raise LDBException(
                        f"{repr(storage_location.path)} is inside existing "
                        f"storage location {repr(loc.path)}",
                    )
                elif new_path in old_path.parents:
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
