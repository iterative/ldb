import os
import re
import shutil
from collections import defaultdict
from enum import Enum, unique
from itertools import tee
from pathlib import Path
from typing import (
    Callable,
    DefaultDict,
    Dict,
    FrozenSet,
    Iterable,
    Iterator,
    List,
    Mapping,
    NamedTuple,
    Optional,
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
from ldb.core import LDBClient, get_ldb_instance
from ldb.data_formats import Format
from ldb.dataset import (
    OpDef,
    apply_queries,
    check_datasets_for_data_objects,
    combine_collections,
    get_collection_from_dataset_identifier,
    iter_collection_dir,
)
from ldb.exceptions import DataObjectNotFoundError, LDBException
from ldb.fs.utils import get_file_hash
from ldb.index import index
from ldb.index.utils import FileSystemPath, expand_indexing_paths
from ldb.path import InstanceDir, WorkspacePath
from ldb.storage import StorageLocation, get_storage_locations
from ldb.transform import (
    TransformInfo,
    dataset_identifier_to_transform_ids,
    get_transform_infos_from_items,
    get_transform_mapping_dir_items,
)
from ldb.utils import (
    DATA_OBJ_ID_PATTERN,
    DATA_OBJ_ID_PREFIX,
    DATASET_PREFIX,
    ROOT,
    WORKSPACE_DATASET_PREFIX,
    format_dataset_identifier,
    get_hash_path,
    json_dumps,
    parse_data_object_hash_identifier,
    parse_dataset_identifier,
)
from ldb.workspace import collection_dir_to_object, load_workspace_dataset

TransformInfoMapping = Dict[str, FrozenSet[TransformInfo]]


@unique
class ArgType(Enum):
    DATASET = "dataset"
    WORKSPACE_DATASET = "workspace_dataset"
    DATA_OBJECT = "data object"
    PATH = "path"


class AddInput(NamedTuple):
    data_object_hashes: Iterable[str]
    annotation_hashes: Iterable[str]
    message: str
    transforms: Optional[Mapping[str, Sequence[str]]] = None


class AddResult(NamedTuple):
    collection: List[Tuple[str, str]]
    num_data_objects: int
    num_transforms: int
    dataset_name: str
    workspace_path: str
    physical_workflow: bool
    num_inst_data_objects: int
    num_inst_annotations: int

    def summary(self) -> str:
        ds_ident = format_dataset_identifier(self.dataset_name)
        return (
            f"Added to {ds_ident} at ws:{self.workspace_path}\n"
            f"  Selected data objects: {len(self.collection):9d}\n"
            f"  New data objects:      {self.num_data_objects:9d}\n"
            f"  Updated transforms:    {self.num_transforms:9d}"
            + (
                f"\n  Copied data objects:   {self.num_inst_data_objects:9d}\n"
                f"  Copied annotations:    {self.num_inst_annotations:9d}"
                if self.physical_workflow
                else ""
            )
        )


def get_arg_type(paths: Sequence[str]) -> ArgType:
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


ArgProcessingFunc = Callable[["LDBClient", Sequence[str]], AddInput]


def paths_to_dataset(
    client: LDBClient,
    paths: Sequence[str],
    collection_ops: Iterable[OpDef],
    warn: bool = True,
    include_transforms: bool = True,
    arg_processing_func: Optional[ArgProcessingFunc] = None,
) -> Tuple[Iterator[Tuple[str, str]], Optional[TransformInfoMapping]]:
    if arg_processing_func is None:
        arg_processing_func = process_args_for_ls
    if not paths:
        paths = ["ws:."]
    if include_transforms:
        transform_infos = paths_to_transforms(client, paths)
    else:
        transform_infos = None
    data_object_hashes, annotation_hashes, message, _ = arg_processing_func(
        client,
        paths,
    )
    if message:
        print(message)
        print()
    collection = apply_queries(
        client,
        data_object_hashes,
        annotation_hashes,
        collection_ops,
        warn=warn,
    )
    return collection, transform_infos


def paths_to_transforms(
    client: "LDBClient",
    paths: Sequence[str],
) -> Dict[str, FrozenSet[TransformInfo]]:
    transform_infos = _paths_to_transform_id_sets(client, paths)
    return get_transform_infos_from_items(client, transform_infos.items())


def paths_to_transform_ids(
    client: "LDBClient",
    paths: Sequence[str],
    workspace_path: Optional[Path] = None,
) -> Dict[str, Tuple[str, ...]]:
    transforms_id_sets = _paths_to_transform_id_sets(
        client,
        paths,
    )
    if workspace_path is not None:
        for data_obj_id, transforms_id_set in _paths_to_transform_id_sets(
            client,
            paths,
        ).items():
            try:
                transforms_id_sets[data_obj_id] |= transforms_id_set
            except KeyError:
                transforms_id_sets[data_obj_id] = transforms_id_set
    return {
        data_obj_id: tuple(transform_id_set)
        for data_obj_id, transform_id_set in transforms_id_sets.items()
    }


def _paths_to_transform_id_sets(
    client: "LDBClient",
    paths: Sequence[str],
) -> DefaultDict[str, Set[str]]:
    arg_type = get_arg_type(paths)
    transform_infos: DefaultDict[str, Set[str]] = defaultdict(set)
    separate_infos: List[Iterable[Tuple[str, List[str]]]] = []
    if arg_type == ArgType.DATASET:
        for dataset_identifier in paths:
            transform_obj = dataset_identifier_to_transform_ids(
                client,
                dataset_identifier,
            )
            separate_infos.append(transform_obj.items())
    elif arg_type == ArgType.WORKSPACE_DATASET:
        paths = [re.sub(r"^ws:", "", p) for p in paths]
        for ws_path in paths:
            info_items = get_transform_mapping_dir_items(
                Path(ws_path) / WorkspacePath.TRANSFORM_MAPPING,
            )
            separate_infos.append(list(info_items))
    for infos in separate_infos:
        for data_object_id, transform_ids in infos:
            transform_infos[data_object_id].update(transform_ids)
    return transform_infos


def process_args_for_add(
    client: LDBClient,
    paths: Sequence[str],
) -> AddInput:
    if not paths:
        raise LDBException("Must supply path")
    return ADD_FUNCTIONS[get_arg_type(paths)](client, paths)


def parse_dataset_paths(
    paths: Sequence[str],
) -> List[Tuple[str, Optional[int]]]:
    result = []
    for path in paths:
        try:
            name, version = parse_dataset_identifier(path)
        except ValueError as exc:
            raise ValueError(
                "All paths must be the same type. "
                f"Found dataset identifier, but unable to "
                f"parse all arguments as datasets: {path}",
            ) from exc
        result.append((name, version))
    return result


def dataset_for_add(client: LDBClient, paths: Sequence[str]) -> AddInput:
    dataset_identifiers = parse_dataset_paths(paths)
    collections = [
        get_collection_from_dataset_identifier(
            client,
            ds_name,
            ds_version,
        )
        for ds_name, ds_version in dataset_identifiers
    ]
    combined_collection = combine_collections(client.ldb_dir, collections)
    return AddInput(
        combined_collection.keys(),
        combined_collection.values(),
        "",
    )


def workspace_dataset_for_add(client: LDBClient, paths: Sequence[str]) -> AddInput:
    paths = [re.sub(r"^ws:", "", p) for p in paths]
    for path in paths:
        load_workspace_dataset(Path(path))
    collections = [
        collection_dir_to_object(
            Path(path) / WorkspacePath.COLLECTION,
        )
        for path in paths
    ]
    combined_collection = combine_collections(client.ldb_dir, collections)
    return AddInput(
        combined_collection.keys(),
        combined_collection.values(),
        "",
    )


def data_object_for_add(
    client: LDBClient,  # pylint: disable=unused-argument
    paths: Sequence[str],
) -> AddInput:
    try:
        data_object_hashes = sorted(parse_data_object_hash_identifier(p) for p in paths)
    except ValueError as exc:
        raise LDBException(
            "All paths must be the same type. "
            f"Found path starting with '{DATA_OBJ_ID_PREFIX}', but unable "
            "parse all paths as a data object identifier",
        ) from exc
    return AddInput(
        data_object_hashes,
        list(client.db.get_current_annotation_hashes(data_object_hashes)),
        "",
    )


def path_for_add(client: LDBClient, paths: Sequence[str]) -> AddInput:
    data_object_hash_iter, data_object_hash_iter2 = tee(
        data_object_hashes_from_path(paths, get_storage_locations(client.ldb_dir)),
    )
    transforms: Optional[Dict[str, Sequence[str]]]
    try:
        # TODO include filepath info in error message
        annotation_hashes = list(
            client.db.get_current_annotation_hashes(h.value for h in data_object_hash_iter)
        )
    except DataObjectNotFoundError as exc:
        cfg: TOMLDocument = config.load_first() or document()
        auto_index: bool = cfg.get("core", {}).get("auto_index", False)
        if not auto_index:
            raise DataObjectNotFoundError(
                f"{exc!s}\n\n"
                "Encountered data object not previously indexed by LDB. "
                "Please index first or enable auto-indexing by setting:\n\n"
                "\tcore.auto_index = true\n",
            ) from exc

        indexing_result = index(
            Path(client.ldb_dir),
            paths,
            read_any_cloud_location=cfg.get("core", {}).get("read_any_cloud_location", False),
        )
        data_object_hashes = list(indexing_result.collection.keys())
        annotation_hashes = list(
            indexing_result.collection.values(),
        )
        message = indexing_result.summary()
        transforms = indexing_result.transforms
    else:
        data_object_hashes = [h.value for h in data_object_hash_iter2]
        message = ""
        transforms = None

    return AddInput(
        data_object_hashes,
        annotation_hashes,
        message,
        transforms=transforms,
    )


ADD_FUNCTIONS: Dict[ArgType, Callable[["LDBClient", Sequence[str]], AddInput]] = {
    ArgType.DATASET: dataset_for_add,
    ArgType.WORKSPACE_DATASET: workspace_dataset_for_add,
    ArgType.DATA_OBJECT: data_object_for_add,
    ArgType.PATH: path_for_add,
}


def add(
    workspace_path: Path,
    paths: Sequence[str],
    query_args: Iterable[OpDef],
    ldb_dir: Optional[Path] = None,
    warn: bool = False,
    physical_workflow: bool = False,
    fmt: str = Format.BARE,
    params: Optional[Mapping[str, str]] = None,
) -> AddResult:
    if not paths:
        if not query_args:
            raise LDBException(
                "Must provide either a query or at least one path",
            )
        paths = [f"{DATASET_PREFIX}{ROOT}"]
    if ldb_dir is None:
        ldb_dir = get_ldb_instance()

    client = LDBClient(ldb_dir)

    workspace_path = Path(os.path.normpath(workspace_path))
    ds_name = load_workspace_dataset(workspace_path).dataset_name
    collection_dir_path = workspace_path / WorkspacePath.COLLECTION
    transform_dir_path = workspace_path / WorkspacePath.TRANSFORM_MAPPING
    collection_dir_path.mkdir(exist_ok=True)
    transform_dir_path.mkdir(exist_ok=True)

    data_object_hashes, annotation_hashes, message, transform_obj = process_args_for_add(
        client,
        paths,
    )
    if message:
        print(message)
        print()

    collection = apply_queries(
        client,
        data_object_hashes,
        annotation_hashes,
        query_args,
        warn=warn,
    )

    print("Adding to working dataset...")

    collection_list = list(collection)
    num_data_objects = add_to_collection_dir(
        collection_dir_path,
        collection_list,
    )
    if transform_obj is None:
        transform_obj = paths_to_transform_ids(
            client,
            paths,
            workspace_path=workspace_path,
        )
    transform_data = [
        (data_obj_id, json_dumps(transforms))
        for data_obj_id, transforms in transform_obj.items()
    ]
    num_transforms = add_to_collection_dir(
        transform_dir_path,
        transform_data,
    )
    num_inst_data_objects = 0
    num_inst_annotations = 0
    if physical_workflow:
        from ldb.instantiate import (
            get_processed_params,
            instantiate_collection,
        )

        processed_params = get_processed_params(params, fmt)

        # TODO: Handle other formats and transformations
        collection_dict = dict(collection_list)
        i_result = instantiate_collection(
            ldb_dir,
            collection_dict,
            workspace_path,
            fmt=fmt,
            clean=False,
            params=processed_params,
            add_path=workspace_path,
        )
        num_inst_data_objects = i_result.num_data_objects
        num_inst_annotations = i_result.num_annotations
    return AddResult(
        collection_list,
        num_data_objects,
        num_transforms,
        ds_name,
        str(workspace_path),
        physical_workflow,
        num_inst_data_objects,
        num_inst_annotations,
    )


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
    client: LDBClient,
    paths: Sequence[str],
) -> List[str]:
    if not paths:
        paths = ["."]
        arg_type = ArgType.WORKSPACE_DATASET
    else:
        arg_type = get_arg_type(paths)
    return DELETE_FUNCTIONS[arg_type](client, paths)


def dataset_for_delete(client: LDBClient, paths: Sequence[str]) -> List[str]:
    dataset_identifiers = parse_dataset_paths(paths)
    collections = [
        get_collection_from_dataset_identifier(
            client,
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
    client: LDBClient,
    paths: Sequence[str],
) -> List[str]:
    return list(workspace_dataset_for_add(client, paths)[0])


def data_object_for_delete(
    client: LDBClient,  # pylint: disable=unused-argument
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
    client: LDBClient,
    paths: Sequence[str],
) -> List[str]:
    return [
        f.value
        for f in data_object_hashes_from_path(
            paths,
            get_storage_locations(client.ldb_dir),
        )
    ]


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


class FileHash(NamedTuple):
    fs_path: FileSystemPath
    value: str


def data_object_hashes_from_path(
    paths: Iterable[str],
    storage_locations: Iterable[StorageLocation],
) -> Iterator[FileHash]:
    for fs, path in get_data_object_storage_files(paths, storage_locations):
        yield FileHash(FileSystemPath(fs, path), get_file_hash(fs, path))


DELETE_FUNCTIONS: Dict[ArgType, Callable[["LDBClient", Sequence[str]], List[str]]] = {
    ArgType.DATASET: dataset_for_delete,
    ArgType.WORKSPACE_DATASET: workspace_dataset_for_delete,
    ArgType.DATA_OBJECT: data_object_for_delete,
    ArgType.PATH: path_for_delete,
}


def delete(
    workspace_path: Path,
    paths: Sequence[str],
    query_args: Iterable[OpDef],
    physical_workflow: bool = False,
    fmt: str = Format.BARE,
    params: Optional[Mapping[str, str]] = None,
) -> None:
    if not paths and not query_args:
        raise LDBException(
            "Must provide either a query or at least one path",
        )
    ldb_dir = get_ldb_instance()
    client = LDBClient(ldb_dir)

    workspace_path = Path(os.path.normpath(workspace_path))
    ds_name = load_workspace_dataset(workspace_path).dataset_name
    ds_ident = format_dataset_identifier(ds_name)
    collection_dir_path = workspace_path / WorkspacePath.COLLECTION

    data_object_hashes = select_data_object_hashes(
        client,
        paths,
        query_args,
        warn=False,
    )
    data_object_hashes = list(data_object_hashes)

    print(f"Removing from working dataset {ds_ident} at ws:{workspace_path}")

    num_deleted = delete_from_collection_dir(
        collection_dir_path,
        data_object_hashes,
        root=False,
    )
    delete_from_collection_dir(
        workspace_path / WorkspacePath.TRANSFORM_MAPPING,
        data_object_hashes,
    )
    print(f"  Removed data objects:  {num_deleted:9d}")
    if not physical_workflow:
        return

    from ldb.instantiate import deinstantiate_collection, get_processed_params

    processed_params = get_processed_params(params, fmt)

    # TODO: Handle transformations
    collection_dict = dict(
        zip(
            data_object_hashes,
            client.db.get_current_annotation_hashes(data_object_hashes),
        )
    )
    i_result = deinstantiate_collection(
        ldb_dir, collection_dict, workspace_path, fmt=fmt, params=processed_params
    )
    num_deinst_data_objects = i_result.num_data_objects_succeeded()
    num_deinst_annotations = i_result.num_annotations_succeeded()
    print(f"  Deleted data objects:  {num_deinst_data_objects:9d}")
    print(f"  Deleted annotations:   {num_deinst_annotations:9d}")


def delete_from_index(
    paths: Sequence[str],
    query_args: Iterable[OpDef],
) -> None:
    if not paths and not query_args:
        raise LDBException(
            "Must provide either a query or at least one path",
        )
    ldb_dir = get_ldb_instance()
    client = LDBClient(ldb_dir)

    ds_ident = f"{DATASET_PREFIX}{ROOT}"
    collection_dir_path = ldb_dir / InstanceDir.DATA_OBJECT_INFO
    if not paths:
        paths = [f"{DATASET_PREFIX}{ROOT}"]

    data_object_hashes = select_data_object_hashes(
        client,
        paths,
        query_args,
        warn=False,
    )
    data_object_hashes = list(data_object_hashes)
    list(
        check_datasets_for_data_objects(
            client,
            data_object_hashes,
            error=True,
        ),
    )
    num_deleted = delete_from_collection_dir(
        collection_dir_path,
        data_object_hashes,
        root=True,
    )
    print(f"Deleted {num_deleted} data objects from {ds_ident}")


def delete_from_collection_dir(
    collection_dir_path: Path,
    data_object_hashes: Iterable[str],
    root: bool = False,
) -> int:
    if not collection_dir_path.exists():
        return 0

    if root:

        def del_func(path: Path) -> None:
            shutil.rmtree(path)

    else:

        def del_func(path: Path) -> None:
            path.unlink()

    num_deleted = 0
    for data_object_hash in data_object_hashes:
        collection_member_path = get_hash_path(
            collection_dir_path,
            data_object_hash,
        )
        if collection_member_path.exists():
            del_func(collection_member_path)
            try:
                collection_member_path.parent.rmdir()
            except OSError:
                pass
            num_deleted += 1
    return num_deleted


def delete_missing_from_collection_dir(
    collection_dir_path: Path,
    data_object_hashes: Iterable[str],
) -> Tuple[int, List[str]]:
    data_object_hash_set = set(data_object_hashes)
    deleted_data_object_hashes = []
    num_deleted = 0
    for path in iter_collection_dir(collection_dir_path):
        parent, name = os.path.split(path)
        data_object_hash = os.path.basename(parent) + name
        if data_object_hash not in data_object_hash_set:
            deleted_data_object_hashes.append(data_object_hash)
            collection_member_path = Path(path)  # TODO: don't use Path obj
            collection_member_path.unlink()
            try:
                collection_member_path.parent.rmdir()
            except OSError:
                pass
            num_deleted += 1
    return num_deleted, deleted_data_object_hashes


def process_args_for_ls(
    client: LDBClient,
    paths: Sequence[str],
) -> AddInput:
    if not paths:
        paths = ["."]
        arg_type = ArgType.WORKSPACE_DATASET
    else:
        arg_type = get_arg_type(paths)
    return LS_FUNCTIONS[arg_type](client, paths)


def path_for_ls(client: LDBClient, paths: Sequence[str]) -> AddInput:
    hashes = []
    for fs, path in get_data_object_storage_files(
        paths,
        get_storage_locations(client.ldb_dir),
    ):
        data_object_hash = get_file_hash(fs, path)
        try:
            annotation_hash: str = list(
                client.db.get_current_annotation_hashes([data_object_hash])
            )[0]
        except DataObjectNotFoundError as exc:
            raise DataObjectNotFoundError(
                "Data object not found: "
                f"{DATA_OBJ_ID_PREFIX}{data_object_hash} "
                f"(path={path!r})",
            ) from exc
        hashes.append((data_object_hash, annotation_hash))
    if hashes:
        data_object_hashes, annotation_hashes = (list(x) for x in zip(*sorted(hashes)))
    else:
        data_object_hashes, annotation_hashes = [], []
    return AddInput(
        data_object_hashes,
        annotation_hashes,
        "",
    )


LS_FUNCTIONS: Dict[ArgType, Callable[["LDBClient", Sequence[str]], AddInput]] = {
    ArgType.DATASET: dataset_for_add,
    ArgType.WORKSPACE_DATASET: workspace_dataset_for_add,
    ArgType.DATA_OBJECT: data_object_for_add,
    ArgType.PATH: path_for_ls,
}


def select_data_object_hashes(
    client: LDBClient,
    paths: Sequence[str],
    query_args: Iterable[OpDef],
    warn: bool = True,
) -> Iterable[str]:
    if not query_args:
        data_object_hashes: Iterable[str] = process_args_for_delete(
            client,
            paths,
        )
    else:
        data_object_hashes, annotation_hashes, _, _ = process_args_for_ls(
            client,
            paths,
        )
        collection = apply_queries(
            client,
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
    physical_workflow: bool = False,
    fmt: str = Format.BARE,
    params: Optional[Mapping[str, str]] = None,
) -> None:
    if not paths:
        paths = ["."]

    ldb_dir = get_ldb_instance()
    client = LDBClient(ldb_dir)
    workspace_path = Path(os.path.normpath(workspace_path))
    ds_name = load_workspace_dataset(workspace_path).dataset_name
    ds_ident = format_dataset_identifier(ds_name)
    collection_dir_path = workspace_path / WorkspacePath.COLLECTION
    collection_dir_path.mkdir(exist_ok=True)

    data_object_hashes, annotation_hashes, message, _ = process_args_for_add(
        client,
        paths,
    )
    if message:
        print(message)
        print()

    collection = list(
        apply_queries(
            client,
            data_object_hashes,
            annotation_hashes,
            query_args,
            warn=False,
        ),
    )

    data_object_hashes = {d for d, _ in collection}
    print(f"Syncing working dataset {ds_ident} at ws:{workspace_path}")
    print(f"  Selected data objects: {len(collection):9d}")
    num_data_objects = add_to_collection_dir(
        collection_dir_path,
        collection,
    )
    print(f"  Added data objects:    {num_data_objects:9d}")
    num_deleted, deleted_data_object_hashes = delete_missing_from_collection_dir(
        collection_dir_path,
        data_object_hashes,
    )
    print(f"  Removed data objects:  {num_deleted:9d}")

    if not physical_workflow:
        return

    from ldb.instantiate import (
        deinstantiate_collection,
        get_processed_params,
        instantiate_collection,
    )

    processed_params = get_processed_params(params, fmt)

    # TODO: Handle transformations
    collection_dict = dict(collection)
    i_result = instantiate_collection(
        ldb_dir,
        collection_dict,
        workspace_path,
        fmt=fmt,
        clean=False,
        params=processed_params,
        add_path=workspace_path,
    )
    num_inst_data_objects = i_result.num_data_objects
    num_inst_annotations = i_result.num_annotations
    print(f"  Copied data objects:   {num_inst_data_objects:9d}")
    print(f"  Copied annotations:    {num_inst_annotations:9d}")

    # TODO: Handle transformations
    collection_dict = dict(
        zip(
            deleted_data_object_hashes,
            client.db.get_current_annotation_hashes(deleted_data_object_hashes),
        )
    )
    i_result = deinstantiate_collection(
        ldb_dir, collection_dict, workspace_path, fmt=fmt, params=processed_params
    )
    num_deinst_data_objects = i_result.num_data_objects_succeeded()
    num_deinst_annotations = i_result.num_annotations_succeeded()
    print(f"  Deleted data objects:  {num_deinst_data_objects:9d}")
    print(f"  Deleted annotations:   {num_deinst_annotations:9d}")
