import os
import re
from enum import Enum, unique
from itertools import tee
from pathlib import Path
from typing import (
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Sequence,
    Set,
    Tuple,
)

from fsspec.core import get_fs_token_paths
from fsspec.spec import AbstractFileSystem
from fsspec.utils import get_protocol
from tomlkit import document
from tomlkit.toml_document import TOMLDocument

from ldb import config
from ldb.config import ConfigType
from ldb.core import get_ldb_instance
from ldb.dataset import (
    OpDef,
    apply_queries,
    combine_collections,
    get_collection_dir_items,
    get_collection_dir_keys,
    get_collection_from_dataset_identifier,
    iter_collection_dir,
)
from ldb.exceptions import DataObjectNotFoundError, LDBException
from ldb.index import index
from ldb.index.utils import expand_indexing_paths
from ldb.path import InstanceDir, WorkspacePath
from ldb.storage import StorageLocation, get_storage_locations
from ldb.utils import (
    DATA_OBJ_ID_PATTERN,
    DATA_OBJ_ID_PREFIX,
    DATASET_PREFIX,
    ROOT,
    WORKSPACE_DATASET_PREFIX,
    format_dataset_identifier,
    get_file_hash,
    get_hash_path,
    parse_data_object_hash_identifier,
    parse_dataset_identifier,
)
from ldb.workspace import collection_dir_to_object, load_workspace_dataset


@unique
class ArgType(Enum):
    ROOT_DATASET = "root dataset"
    DATASET = "dataset"
    WORKSPACE_DATASET = "workspace_dataset"
    DATA_OBJECT = "data object"
    PATH = "path"


class AddInput(NamedTuple):
    data_object_hashes: Iterable[str]
    annotation_hashes: Iterable[str]
    message: str


def get_arg_type(paths: Sequence[str]) -> ArgType:
    if any(p == f"{DATASET_PREFIX}{ROOT}" for p in paths):
        return ArgType.ROOT_DATASET
    if any(p.startswith(DATASET_PREFIX) for p in paths):
        return ArgType.DATASET
    if any(p.startswith(WORKSPACE_DATASET_PREFIX) for p in paths):
        return ArgType.WORKSPACE_DATASET
    if any(re.search(DATA_OBJ_ID_PATTERN, p) for p in paths):
        return ArgType.DATA_OBJECT
    return ArgType.PATH


def expands_to_workspace(urlpath: str) -> bool:
    if get_protocol(urlpath) != "file":
        return False
    fs, _, paths = get_fs_token_paths(urlpath)
    for path in paths:
        if (
            fs.isdir(fs.sep.join([path, ".ldb_workspace"]))
            and os.path.abspath(path) != os.getcwd()
        ):
            return True
    return False


def process_args_for_add(
    ldb_dir: Path,
    paths: Sequence[str],
) -> AddInput:
    if not paths:
        raise LDBException("Must supply path")
    return ADD_FUNCTIONS[get_arg_type(paths)](ldb_dir, paths)


def root_dataset_for_add(
    ldb_dir: Path,
    paths: Sequence[str],  # pylint: disable=unused-argument
) -> AddInput:
    data_object_hashes = []
    annotation_hashes = []
    for data_object_hash, annotation_hash in get_collection_dir_items(
        ldb_dir / InstanceDir.DATA_OBJECT_INFO,
        is_workspace=False,
    ):
        data_object_hashes.append(data_object_hash)
        annotation_hashes.append(annotation_hash or "")
    return AddInput(data_object_hashes, annotation_hashes, "")


def dataset_for_add(ldb_dir: Path, paths: Sequence[str]) -> AddInput:
    try:
        dataset_identifiers = [parse_dataset_identifier(p) for p in paths]
    except LDBException as exc:
        raise LDBException(
            "All paths must be the same type. "
            "Found path starting with 'ds', but unable "
            "parse all paths as a dataset identifier",
        ) from exc
    collections = [
        get_collection_from_dataset_identifier(
            ldb_dir,
            ds_name,
            ds_version,
        )
        for ds_name, ds_version in dataset_identifiers
    ]
    combined_collection = combine_collections(ldb_dir, collections)
    return AddInput(
        combined_collection.keys(),
        combined_collection.values(),
        "",
    )


def workspace_dataset_for_add(ldb_dir: Path, paths: Sequence[str]) -> AddInput:
    paths = [re.sub(r"^ws:", "", p) for p in paths]
    collections = [
        collection_dir_to_object(
            Path(path) / WorkspacePath.COLLECTION,
        )
        for path in paths
    ]
    combined_collection = combine_collections(ldb_dir, collections)
    return AddInput(
        combined_collection.keys(),
        combined_collection.values(),
        "",
    )


def data_object_for_add(
    ldb_dir: Path,  # pylint: disable=unused-argument
    paths: Sequence[str],
) -> AddInput:
    try:
        data_object_hashes = sorted(
            parse_data_object_hash_identifier(p) for p in paths
        )
    except ValueError as exc:
        raise LDBException(
            "All paths must be the same type. "
            f"Found path starting with '{DATA_OBJ_ID_PREFIX}', but unable "
            "parse all paths as a data object identifier",
        ) from exc
    return AddInput(
        data_object_hashes,
        get_current_annotation_hashes(ldb_dir, data_object_hashes),
        "",
    )


def path_for_add(ldb_dir: Path, paths: Sequence[str]) -> AddInput:
    data_object_hash_iter, data_object_hash_iter2 = tee(
        data_object_hashes_from_path(paths, get_storage_locations(ldb_dir)),
    )
    try:
        annotation_hashes = get_current_annotation_hashes(
            ldb_dir,
            data_object_hash_iter,
        )
    except DataObjectNotFoundError as exc:
        cfg: TOMLDocument = (
            config.load_first([ConfigType.INSTANCE]) or document()
        )
        auto_index: bool = cfg.get("core", {}).get("auto_index", False)
        if not auto_index:
            raise DataObjectNotFoundError(
                f"{exc!s}\n"
                f"Will not index new data: auto_index = {auto_index}",
            ) from exc

        indexing_result = index(
            ldb_dir,
            paths,
            read_any_cloud_location=(
                cfg.get("core", {}).get("read_any_cloud_location", False)
            ),
        )
        data_object_hashes = indexing_result.data_object_hashes
        annotation_hashes = get_current_annotation_hashes(
            ldb_dir,
            data_object_hashes,
        )
        message = indexing_result.summary()
    else:
        data_object_hashes = list(data_object_hash_iter2)
        message = ""

    return AddInput(
        data_object_hashes,
        annotation_hashes,
        message,
    )


ADD_FUNCTIONS: Dict[ArgType, Callable[[Path, Sequence[str]], AddInput]] = {
    ArgType.ROOT_DATASET: root_dataset_for_add,
    ArgType.DATASET: dataset_for_add,
    ArgType.WORKSPACE_DATASET: workspace_dataset_for_add,
    ArgType.DATA_OBJECT: data_object_for_add,
    ArgType.PATH: path_for_add,
}


def add(
    workspace_path: Path,
    paths: Sequence[str],
    query_args: Iterable[OpDef],
) -> None:
    if not paths:
        if not query_args:
            raise LDBException(
                "Must provide either a query or at least one path",
            )
        paths = [f"{DATASET_PREFIX}{ROOT}"]
    ldb_dir = get_ldb_instance()

    workspace_path = Path(os.path.normpath(workspace_path))
    ds_name = load_workspace_dataset(workspace_path).dataset_name
    collection_dir_path = workspace_path / WorkspacePath.COLLECTION
    collection_dir_path.mkdir(exist_ok=True)

    data_object_hashes, annotation_hashes, message = process_args_for_add(
        ldb_dir,
        paths,
    )
    if message:
        print(message)
        print()

    collection = apply_queries(
        ldb_dir,
        data_object_hashes,
        annotation_hashes,
        query_args,
        warn=False,
    )
    print("Adding to working dataset...")

    num_data_objects = add_to_collection_dir(
        collection_dir_path,
        collection,
    )

    ds_ident = format_dataset_identifier(ds_name)
    print(f"Added {num_data_objects} data objects to {ds_ident}")


def add_to_collection_dir(
    collection_dir_path: Path,
    collection: Iterable[Tuple[str, str]],
) -> int:
    to_write = []
    for data_object_hash, annotation_hash in collection:
        to_write.append(
            (
                get_hash_path(collection_dir_path, data_object_hash),
                annotation_hash,
            ),
        )

    num_data_objects = 0
    for collection_member_path, annotation_hash in to_write:
        if collection_member_path.exists():
            with collection_member_path.open("r+") as file:
                existing_annotation_hash = file.read()
                if annotation_hash != existing_annotation_hash:
                    file.seek(0)
                    file.write(annotation_hash)
                    file.truncate()
                    num_data_objects += 1
        else:
            collection_member_path.parent.mkdir(exist_ok=True)
            with collection_member_path.open("w") as file:
                file.write(annotation_hash)
                num_data_objects += 1
    return num_data_objects


def process_args_for_delete(
    ldb_dir: Path,
    paths: Sequence[str],
) -> List[str]:
    if not paths:
        paths = ["."]
        arg_type = ArgType.WORKSPACE_DATASET
    else:
        arg_type = get_arg_type(paths)
    return DELETE_FUNCTIONS[arg_type](ldb_dir, paths)


def root_dataset_for_delete(
    ldb_dir: Path,
    paths: Sequence[str],  # pylint: disable=unused-argument
) -> List[str]:
    return sorted(
        get_collection_dir_keys(
            ldb_dir / InstanceDir.DATA_OBJECT_INFO,
        ),
    )


def dataset_for_delete(ldb_dir: Path, paths: Sequence[str]) -> List[str]:
    try:
        dataset_identifiers = [parse_dataset_identifier(p) for p in paths]
    except LDBException as exc:
        raise LDBException(
            "All paths must be the same type. "
            "Found path starting with 'ds', but unable "
            "parse all paths as a dataset identifier",
        ) from exc
    collections = [
        get_collection_from_dataset_identifier(
            ldb_dir,
            ds_name,
            ds_version,
        )
        for ds_name, ds_version in dataset_identifiers
    ]
    data_objects: Set[str] = set()
    for collection in collections:
        data_objects.update(collection.keys())
    return list(data_objects)


def workspace_dataset_for_delete(
    ldb_dir: Path,
    paths: Sequence[str],
) -> List[str]:
    return list(workspace_dataset_for_add(ldb_dir, paths)[0])


def data_object_for_delete(
    ldb_dir: Path,  # pylint: disable=unused-argument
    paths: Sequence[str],
) -> List[str]:
    try:
        return [parse_data_object_hash_identifier(p) for p in paths]
    except ValueError as exc:
        raise LDBException(
            "All paths must be the same type. "
            f"Found path starting with '{DATA_OBJ_ID_PREFIX}', but unable "
            "parse all paths as a data object identifier",
        ) from exc


def path_for_delete(
    ldb_dir: Path,
    paths: Sequence[str],
) -> List[str]:
    return list(
        data_object_hashes_from_path(paths, get_storage_locations(ldb_dir)),
    )


def get_data_object_storage_files(
    paths: Iterable[str],
    storage_locations: Iterable[StorageLocation],
) -> Iterator[Tuple[AbstractFileSystem, str]]:
    for fs, fs_paths in expand_indexing_paths(
        paths,
        storage_locations,
        default_format=False,
    ).items():
        for path in fs_paths:
            if not path.endswith(".json"):
                yield fs, path


def data_object_hashes_from_path(
    paths: Iterable[str],
    storage_locations: Iterable[StorageLocation],
) -> Iterator[str]:
    for fs, path in get_data_object_storage_files(paths, storage_locations):
        yield get_file_hash(fs, path)


DELETE_FUNCTIONS: Dict[ArgType, Callable[[Path, Sequence[str]], List[str]]] = {
    ArgType.ROOT_DATASET: root_dataset_for_delete,
    ArgType.DATASET: dataset_for_delete,
    ArgType.WORKSPACE_DATASET: workspace_dataset_for_delete,
    ArgType.DATA_OBJECT: data_object_for_delete,
    ArgType.PATH: path_for_delete,
}


def delete(
    workspace_path: Path,
    paths: Sequence[str],
    query_args: Iterable[OpDef],
) -> None:
    if not paths and not query_args:
        raise LDBException(
            "Must provide either a query or at least one path",
        )

    workspace_path = Path(os.path.normpath(workspace_path))
    ds_name = load_workspace_dataset(workspace_path).dataset_name
    ds_ident = format_dataset_identifier(ds_name)
    collection_dir_path = workspace_path / WorkspacePath.COLLECTION

    data_object_hashes = select_data_object_hashes(
        get_ldb_instance(),
        paths,
        query_args,
        warn=False,
    )
    num_deleted = delete_from_collection_dir(
        collection_dir_path,
        data_object_hashes,
    )
    print(f"Deleted {num_deleted} data objects from {ds_ident}")


def delete_from_collection_dir(
    collection_dir_path: Path,
    data_object_hashes: Iterable[str],
) -> int:
    if not collection_dir_path.exists():
        return 0

    num_deleted = 0
    for data_object_hash in data_object_hashes:
        collection_member_path = get_hash_path(
            collection_dir_path,
            data_object_hash,
        )
        if collection_member_path.exists():
            collection_member_path.unlink()
            try:
                collection_member_path.parent.rmdir()
            except OSError:
                pass
            num_deleted += 1
    return num_deleted


def delete_missing_from_collection_dir(
    collection_dir_path: Path,
    data_object_hashes: Iterable[str],
) -> int:
    data_object_hash_set = set(data_object_hashes)
    num_deleted = 0
    for path in iter_collection_dir(collection_dir_path):
        parent, name = os.path.split(path)
        data_object_hash = os.path.basename(parent) + name
        if data_object_hash not in data_object_hash_set:
            collection_member_path = Path(path)  # TODO: don't use Path obj
            collection_member_path.unlink()
            try:
                collection_member_path.parent.rmdir()
            except OSError:
                pass
            num_deleted += 1
    return num_deleted


def get_current_annotation_hash(ldb_dir: Path, data_object_hash: str) -> str:
    data_object_dir = get_hash_path(
        ldb_dir / InstanceDir.DATA_OBJECT_INFO,
        data_object_hash,
    )
    if not data_object_dir.is_dir():
        raise DataObjectNotFoundError(
            f"Data object not found: {DATA_OBJ_ID_PREFIX}{data_object_hash}",
        )
    try:
        return (data_object_dir / "current").read_text()
    except FileNotFoundError:
        return ""


def get_current_annotation_hashes(
    ldb_dir: Path,
    data_object_hashes: Iterable[str],
) -> List[str]:
    return [
        get_current_annotation_hash(ldb_dir, d) for d in data_object_hashes
    ]


def process_args_for_ls(
    ldb_dir: Path,
    paths: Sequence[str],
) -> AddInput:
    if not paths:
        paths = ["."]
        arg_type = ArgType.WORKSPACE_DATASET
    else:
        arg_type = get_arg_type(paths)
    return LS_FUNCTIONS[arg_type](ldb_dir, paths)


def path_for_ls(ldb_dir: Path, paths: Sequence[str]) -> AddInput:
    hashes = []
    for fs, path in get_data_object_storage_files(
        paths,
        get_storage_locations(ldb_dir),
    ):
        data_object_hash = get_file_hash(fs, path)
        try:
            annotation_hash = get_current_annotation_hash(
                ldb_dir,
                data_object_hash,
            )
        except DataObjectNotFoundError as exc:
            raise DataObjectNotFoundError(
                "Data object not found: "
                f"{DATA_OBJ_ID_PREFIX}{data_object_hash} "
                f"(path={path!r})",
            ) from exc
        hashes.append((data_object_hash, annotation_hash))
    if hashes:
        data_object_hashes, annotation_hashes = (
            list(x) for x in zip(*sorted(hashes))
        )
    else:
        data_object_hashes, annotation_hashes = [], []
    return AddInput(
        data_object_hashes,
        annotation_hashes,
        "",
    )


LS_FUNCTIONS: Dict[ArgType, Callable[[Path, Sequence[str]], AddInput]] = {
    ArgType.ROOT_DATASET: root_dataset_for_add,
    ArgType.DATASET: dataset_for_add,
    ArgType.WORKSPACE_DATASET: workspace_dataset_for_add,
    ArgType.DATA_OBJECT: data_object_for_add,
    ArgType.PATH: path_for_ls,
}


def select_data_object_hashes(
    ldb_dir: Path,
    paths: Sequence[str],
    query_args: Iterable[OpDef],
    warn: bool = True,
) -> Iterable[str]:
    if not query_args:
        data_object_hashes: Iterable[str] = process_args_for_delete(
            ldb_dir,
            paths,
        )
    else:
        data_object_hashes, annotation_hashes, _ = process_args_for_ls(
            ldb_dir,
            paths,
        )
        collection = apply_queries(
            ldb_dir,
            data_object_hashes,
            annotation_hashes,
            query_args,
            warn=warn,
        )
        data_object_hashes = (d for d, _ in collection)
    return data_object_hashes


def sync(
    workspace_path: Path,
    paths: Sequence[str],
    query_args: Iterable[OpDef],
) -> None:
    if not paths:
        paths = ["."]

    ldb_dir = get_ldb_instance()
    workspace_path = Path(os.path.normpath(workspace_path))
    ds_name = load_workspace_dataset(workspace_path).dataset_name
    ds_ident = format_dataset_identifier(ds_name)
    collection_dir_path = workspace_path / WorkspacePath.COLLECTION
    collection_dir_path.mkdir(exist_ok=True)

    data_object_hashes, annotation_hashes, message = process_args_for_add(
        ldb_dir,
        paths,
    )
    if message:
        print(message)
        print()

    collection = list(
        apply_queries(
            ldb_dir,
            data_object_hashes,
            annotation_hashes,
            query_args,
            warn=False,
        ),
    )

    data_object_hashes = {d for d, _ in collection}
    print("Syncing working dataset...")
    num_data_objects = add_to_collection_dir(
        collection_dir_path,
        collection,
    )
    print(f"Added {num_data_objects} data objects to {ds_ident}")
    num_deleted = delete_missing_from_collection_dir(
        collection_dir_path,
        data_object_hashes,
    )
    print(f"Deleted {num_deleted} data objects from {ds_ident}")
