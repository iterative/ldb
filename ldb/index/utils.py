import getpass
import json
import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import (
    Any,
    Collection,
    Dict,
    Iterable,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

import fsspec
from fsspec.spec import AbstractFileSystem
from fsspec.utils import get_protocol

from ldb.data_formats import Format
from ldb.exceptions import IndexingException, NotAStorageLocationError
from ldb.fs import posix_path as fsp
from ldb.fs.utils import (
    cp_file_any_fs,
    get_file_hash,
    get_modified_time,
    has_protocol,
    unstrip_protocol,
)
from ldb.func_utils import apply_optional
from ldb.storage import StorageLocation, get_filesystem
from ldb.typing import JSONDecoded
from ldb.utils import (
    StrEnum,
    format_datetime,
    get_filetype,
    get_first,
    get_fsspec_path_suffix,
    parse_datetime,
    timestamp_to_datetime,
    unique_id,
)

ENDING_DOUBLE_STAR_RE = r"(?:/+\*\*)+/*$"

AnnotationMeta = Dict[str, Union[str, int, None]]
DataObjectMeta = Dict[
    str,
    Union[str, None, Dict[str, Union[str, List[str], int, None]]],
]
DataToWrite = Tuple[Path, bytes, bool]
IndexingJob = Tuple["IndexingConfig", List[str]]
IndexingJobMapping = Dict[AbstractFileSystem, List[IndexingJob]]
FSPathsMapping = Dict[AbstractFileSystem, List[str]]


class FileSystemPath(NamedTuple):
    fs: AbstractFileSystem
    path: str


class AnnotMergeStrategy(StrEnum):
    MERGE = "merge"
    REPLACE = "replace"


@dataclass
class IndexingConfig:
    save_data_object_path_info: bool


DEFAULT_CONFIG = IndexingConfig(save_data_object_path_info=True)
INDEXED_EPHEMERAL_CONFIG = IndexingConfig(save_data_object_path_info=False)


def expand_indexing_paths(
    paths: Iterable[str],
    storage_locations: Iterable[StorageLocation],
    default_format: bool = False,
) -> Dict[AbstractFileSystem, List[str]]:
    storage_locations = list(storage_locations)
    path_collections: Dict[AbstractFileSystem, Tuple[List[str], Set[str]]] = {}
    for indexing_path in paths:
        fs, paths_found = expand_single_indexing_path(
            indexing_path,
            storage_locations,
            default_format=default_format,
        )
        try:
            fs_paths, seen = path_collections[fs]
        except KeyError:
            fs_paths, seen = [], set()
            path_collections[fs] = fs_paths, seen
        for path in paths_found:
            if path not in seen:
                fs_paths.append(path)
                seen.add(path)
    return {fs: fs_paths for fs, (fs_paths, _) in path_collections.items()}


def expand_single_indexing_path(
    path: str,
    storage_locations: Collection[StorageLocation],
    default_format: bool = False,
) -> Tuple[AbstractFileSystem, List[str]]:
    """
    Get storage paths for indexing that match the glob, `path`.

    Because every file path under `path` is returned, any final "/**" does not
    change the result. First, `path` is expanded with any final "/**" removed
    and any matching files are included in the result. Corresponding data
    object file paths are added for matched annotation file paths and vice
    versa. Then everything under matched directories will be added to the
    result.

    The current implementation may result in some directory paths and some
    duplicate paths being included.
    """
    # TODO: make sure path is in a storage location if necessary
    protocol = get_protocol(path)
    fs_cls = fsspec.get_filesystem_class(protocol)
    path = fs_cls._strip_protocol(path)  # pylint: disable=protected-access
    fs = get_filesystem(path, protocol, storage_locations)
    if protocol == "file":
        path = os.path.abspath(path)
    if is_hidden_fsspec_path(path):
        return fs, []
    # "path/**", "path/**/**" or "path/**/" are treated the same as just "path"
    # so we strip off any "/**" or "/**/" at the end
    path = re.sub(ENDING_DOUBLE_STAR_RE, "", path)
    # find corresponding data object for annotation match and vice versa
    # for any files the expanded `path` glob matches
    path_match_globs = []
    for epath in fs.expand_path(path):
        if not is_hidden_fsspec_path(epath) and fs.isfile(epath):
            path_match_globs.append(epath)
            if default_format:
                # TODO: Check all extension levels (i.e. for abc.tar.gz use
                # .tar.gz and .gz)
                p_without_ext, ext = os.path.splitext(epath)
                if ext == ".json":
                    path_match_globs.append(p_without_ext)
                    path_match_globs.append(p_without_ext + ".*")
                else:
                    path_match_globs.append(p_without_ext + ".json")
    paths = (
        [i for p in path_match_globs for i in fs.glob(p)]
        if path_match_globs
        else []
    )
    if protocol not in ("http", "https"):
        # capture everything under any directories the `path` glob matches
        for epath in fs.expand_path(path, recursive=True):
            if not is_hidden_fsspec_path(epath) and fs.isfile(epath):
                paths.append(epath)
    return fs, paths


def is_hidden_fsspec_path(path: str) -> bool:
    return re.search(r"(?:/|^)\.(?!/|$)", path) is not None


def group_indexing_paths_by_type(
    fs: AbstractFileSystem,
    storage_paths: Iterable[str],
) -> Tuple[List[str], List[str]]:
    data_object_paths = []
    annotation_paths = []
    seen = set()
    for storage_path in storage_paths:
        if storage_path not in seen:
            seen.add(storage_path)
            if fs.isfile(storage_path):
                if storage_path.endswith(".json"):
                    annotation_paths.append(storage_path)
                else:
                    data_object_paths.append(storage_path)
    return data_object_paths, annotation_paths


def expand_dir_paths(
    paths: Iterable[str],
    storage_locations: Sequence[StorageLocation],
) -> Dict[AbstractFileSystem, List[str]]:
    for path in paths:
        path, num = re.subn(ENDING_DOUBLE_STAR_RE, "", path)
        if num:
            raise IndexingException(
                f"Paths passed with the {Format.INFER} format should only "
                "match directories, so globs with a final /** should not be "
                "used",
            )

    path_sets: Dict[AbstractFileSystem, Set[str]] = {}
    for path in paths:
        if not is_hidden_fsspec_path(path):
            protocol = get_protocol(path)
            fs_cls = fsspec.get_filesystem_class(protocol)
            path = fs_cls._strip_protocol(  # pylint: disable=protected-access
                path,
            )
            fs = get_filesystem(path, protocol, storage_locations)
            fs_paths = path_sets.setdefault(fs, set())
            for p in fs.expand_path(paths):
                if not is_hidden_fsspec_path(p) and fs.isdir(p):
                    fs_paths.add(p)
    path_lists: Dict[AbstractFileSystem, List[str]] = {}
    for fs, fs_path_set in path_sets.items():
        paths = sorted(fs_path_set)
        for i in range(len(paths) - 1):
            if (paths[i + 1].rstrip("/") + "/").startswith(
                paths[i].rstrip("/") + "/",
            ):
                raise IndexingException(
                    f"Paths passed with the {Format.INFER} format should "
                    "match non-overlapping directories. Found overlapping "
                    "directories:\n"
                    f"{paths[i]!r}\n"
                    f"{paths[i + 1]!r}",
                )
        path_lists[fs] = paths
    return path_lists


def create_storage_path(
    dest_fs: AbstractFileSystem,
    base_dir: str,
    source_fs: AbstractFileSystem,
    path: str,
) -> str:
    if source_fs.protocol == "file":
        # handle windows drive
        path = re.sub("^([A-Za-z]):", r"\1", path)
    path = dest_fs.sep.join(
        [base_dir] + path.lstrip(source_fs.sep).split(source_fs.sep),
    )
    return path


def copy_to_read_add_storage(
    data_object_paths: FSPathsMapping,
    annotation_paths: FSPathsMapping,
    read_add_location: StorageLocation,
    hashes: Mapping[AbstractFileSystem, Mapping[str, str]],
    strict_format: bool,
) -> Tuple[
    Dict[AbstractFileSystem, Dict[str, FileSystemPath]],
    Dict[AbstractFileSystem, Dict[str, FileSystemPath]],
]:
    dest_fs = fsspec.filesystem(
        read_add_location.protocol,
        **read_add_location.options,
    )
    read_add_path = (
        dest_fs._strip_protocol(  # pylint: disable=protected-access
            read_add_location.path,
        )
    )
    base_dir = dest_fs.sep.join(
        [
            *read_add_path.split("/"),
            "ldb-autoimport",
            date.today().isoformat(),
            unique_id(),
        ],
    )
    dest_fs.makedirs(base_dir, exist_ok=True)
    old_to_new_paths = {}
    old_to_new_annot_paths = {}
    for source_fs, paths in data_object_paths.items():
        fs_id = source_fs.protocol  # TODO use actual fs_id
        fs_base_dir = dest_fs.sep.join([base_dir, fs_id])

        fs_annotation_paths = set(annotation_paths.get(source_fs, []))

        fs_old_to_new_paths = {}
        fs_old_to_new_annot_paths = {}
        for path in paths:
            dest = create_storage_path(dest_fs, fs_base_dir, source_fs, path)
            annotation_dest = None
            annotation_path = data_object_path_to_annotation_path(path)
            if annotation_path in fs_annotation_paths:
                annotation_dest = create_storage_path(
                    dest_fs,
                    fs_base_dir,
                    source_fs,
                    annotation_path,
                )
            elif strict_format:
                continue
            try:
                dest_fs.makedirs(
                    dest_fs._parent(dest),  # pylint: disable=protected-access)
                    exist_ok=True,
                )
                if annotation_dest is not None:
                    cp_file_any_fs(
                        source_fs,
                        annotation_path,
                        dest_fs,
                        annotation_dest,
                    )
                cp_file_any_fs(source_fs, path, dest_fs, dest)
            except FileNotFoundError:
                # Use hash instead of preserving path if path is too long
                try:
                    data_object_hash = hashes[source_fs][path]
                except KeyError:
                    data_object_hash = get_file_hash(
                        source_fs,
                        path,
                    )
                dest = dest_fs.sep.join(
                    [
                        base_dir,
                        data_object_hash + get_fsspec_path_suffix(path),
                    ],
                )
                if annotation_dest is not None:
                    annotation_dest = data_object_path_to_annotation_path(dest)
                    cp_file_any_fs(
                        source_fs,
                        annotation_path,
                        dest_fs,
                        annotation_dest,
                    )
                cp_file_any_fs(source_fs, path, dest_fs, dest)
            fs_old_to_new_paths[path] = FileSystemPath(dest_fs, dest)
            if annotation_dest is not None:
                fs_old_to_new_annot_paths[annotation_path] = FileSystemPath(
                    dest_fs,
                    annotation_dest,
                )
        if fs_old_to_new_paths:
            old_to_new_paths[dest_fs] = fs_old_to_new_paths
        if fs_old_to_new_annot_paths:
            old_to_new_annot_paths[dest_fs] = fs_old_to_new_annot_paths
    return old_to_new_paths, old_to_new_annot_paths


def data_object_path_to_annotation_path(path: str) -> str:
    return os.path.splitext(path)[0] + ".json"


def separate_local_and_cloud_files(
    paths: FSPathsMapping,
) -> Tuple[FSPathsMapping, FSPathsMapping]:
    local = {}
    cloud = {}
    for fs, fs_paths in paths.items():
        if fs.protocol == "file":
            local[fs] = fs_paths
        else:
            cloud[fs] = fs_paths
    return local, cloud


def separate_storage_and_non_storage_files(
    fs_paths: FSPathsMapping,
    storage_locations: Sequence[StorageLocation],
) -> Tuple[FSPathsMapping, FSPathsMapping]:
    storage = {}
    non_storage = {}
    for fs, paths in fs_paths.items():
        matching_locs = filter_storage_locations(fs, storage_locations)
        if not matching_locs:
            non_storage[fs] = paths.copy()
        else:
            fs_storage = []
            fs_non_storage = []
            for path in paths:
                if in_storage_locations(path, matching_locs):
                    fs_storage.append(path)
                else:
                    fs_non_storage.append(path)
            if fs_storage:
                storage[fs] = fs_storage
            if fs_non_storage:
                non_storage[fs] = fs_non_storage
    return storage, non_storage


def separate_indexed_files(
    existing_hashes: Set[str],
    hashes: Mapping[AbstractFileSystem, Mapping[str, str]],
    fs_paths: FSPathsMapping,
) -> Tuple[FSPathsMapping, FSPathsMapping]:
    indexed = {}
    not_indexed = {}
    for fs, paths in fs_paths.items():
        fs_indexed = []
        fs_not_indexed = []
        for path in paths:
            try:
                hash_str: Optional[str] = hashes[fs][path]
            except KeyError:
                hash_str = None
            if hash_str is not None and hash_str in existing_hashes:
                fs_indexed.append(path)
            else:
                fs_not_indexed.append(path)
        if fs_indexed:
            indexed[fs] = fs_indexed
        if fs_not_indexed:
            not_indexed[fs] = fs_not_indexed
    return indexed, not_indexed


def validate_locations_in_storage(
    fs_paths: FSPathsMapping,
    storage_locations: Sequence[StorageLocation],
) -> None:
    for fs, paths in fs_paths.items():
        matching_locs = filter_storage_locations(fs, storage_locations)
        for path in paths:
            if not in_storage_locations(path, matching_locs):
                full_path = unstrip_protocol(fs, path)
                raise NotAStorageLocationError(
                    "Found file outside of configured storage locations: "
                    f"{full_path}",
                )


def filter_storage_locations(
    fs: AbstractFileSystem,
    storage_locations: Sequence[StorageLocation],
) -> List[StorageLocation]:
    # TODO use fs_id
    return [
        s for s in storage_locations if has_protocol(fs.protocol, s.protocol)
    ]


def in_storage_locations(
    path: str,
    storage_locations: Sequence[StorageLocation],
) -> bool:
    for loc in storage_locations:
        if fsp.isin(path.rstrip("/"), loc.path.rstrip("/")):
            return True
    return False


def datetime_fs_info(
    fs_info: Dict[str, Any],
    *keys: str,
) -> Optional[datetime]:
    return apply_optional(timestamp_to_datetime, get_first(fs_info, *keys))


def max_datetime_info(
    *datetimes: Union[datetime, str, None],
) -> Optional[datetime]:
    return max(
        (
            parse_datetime(d) if isinstance(d, str) else d
            for d in datetimes
            if d is not None
        ),
        default=None,
    )


def construct_data_object_meta(
    fs: AbstractFileSystem,
    path: str,
    prev_meta: Dict[str, Any],
    current_timestamp: str,
    tags: Collection[str] = (),
) -> Tuple[bool, DataObjectMeta]:
    # TODO: create dataclass for data object meta
    fs_info = fs.info(path)

    last_indexed_by: str = getpass.getuser()

    atime = datetime_fs_info(fs_info, "atime", "accessed", "time")
    if atime is None and fs.protocol == "file":
        stat_result = os.stat(
            fs._strip_protocol(path),  # pylint: disable=protected-access
            follow_symlinks=False,
        )
        atime = timestamp_to_datetime(stat_result.st_atime)

    mtime = get_modified_time(fs, path)
    ctime = datetime_fs_info(fs_info, "ctime", "created")
    size: int = fs_info.get("size", 0)
    mode: int = fs_info.get("mode", 0)
    uid: Optional[int] = fs_info.get("uid")
    gid: Optional[int] = fs_info.get("gid")
    filetype: str = get_filetype(path)

    if prev_meta:
        first_indexed = prev_meta["first_indexed"]
        new_tags = sorted(set(prev_meta["tags"]) | set(tags))
        alternate_paths = prev_meta["alternate_paths"]
        atime = max_datetime_info(atime, prev_meta["fs"]["atime"])
        mtime = max_datetime_info(mtime, prev_meta["fs"]["mtime"])
        ctime = max_datetime_info(ctime, prev_meta["fs"]["ctime"])
        filetype = filetype or prev_meta["type"]
    else:
        first_indexed = current_timestamp
        new_tags = sorted(set(tags))
        alternate_paths = []

    protocol: Union[str, List[str]] = fs.protocol
    path_info = {
        "fs_id": "",
        "protocol": protocol,
        "path": path,
    }
    if path_info not in alternate_paths:
        alternate_paths.append(path_info)
        found_new_path = True
    else:
        found_new_path = False
    return found_new_path, {
        "type": filetype,
        "first_indexed": first_indexed,
        "last_indexed": current_timestamp,
        "last_indexed_by": last_indexed_by,
        "tags": new_tags,  # type: ignore[dict-item]
        "alternate_paths": alternate_paths,
        "fs": {
            **path_info,
            "size": size,
            "mode": mode,
            "uid": uid,
            "gid": gid,
            "atime": apply_optional(format_datetime, atime),
            "mtime": apply_optional(format_datetime, mtime),
            "ctime": apply_optional(format_datetime, ctime),
        },
    }


def parse_annotation(raw_annotation: str) -> JSONDecoded:
    try:
        parsed_annotation: JSONDecoded = json.loads(raw_annotation)
    except Exception as exc:
        raise IndexingException(  # TODO: use more appropriate exception type
            "Unable to parse JSON annotation\n" f"{type(exc).__name__}: {exc}",
        ) from exc
    return parsed_annotation


def get_annotation_content(
    fs: AbstractFileSystem,
    path: str,
) -> JSONDecoded:
    try:
        with fs.open(path, "r") as file:
            raw_annotation = file.read()
        return parse_annotation(raw_annotation)
    except Exception as exc:
        raise IndexingException(
            f"Unable to read annotation file: {path}",
        ) from exc


def construct_annotation_meta(
    prev_annotation_meta: AnnotationMeta,
    current_timestamp: str,
    version: int,
    curr_mtime: Optional[datetime],
) -> AnnotationMeta:
    mtimes = []
    if prev_annotation_meta:
        prev_mtime: Optional[str] = prev_annotation_meta.get(  # type: ignore[assignment] # noqa: E501
            "mtime",
        )
        if prev_mtime is not None:
            mtimes.append(parse_datetime(prev_mtime))
        version = prev_annotation_meta["version"]  # type: ignore[assignment]
        first_indexed_time = prev_annotation_meta["first_indexed_time"]
    else:
        first_indexed_time = current_timestamp

    if curr_mtime is not None:
        mtimes.append(curr_mtime)

    mtime = format_datetime(max(mtimes)) if mtimes else None
    return {
        "version": version,
        "mtime": mtime,
        "first_indexed_time": first_indexed_time,
        "last_indexed_time": current_timestamp,
    }
