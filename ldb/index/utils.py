import getpass
import json
import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import (
    Any,
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
from fsspec.core import OpenFile
from fsspec.implementations.local import make_path_posix
from fsspec.spec import AbstractFileSystem

from ldb.data_formats import Format
from ldb.exceptions import IndexingException, NotAStorageLocationError
from ldb.storage import StorageLocation
from ldb.typing import JSONDecoded
from ldb.utils import (
    format_datetime,
    get_filetype,
    get_fsspec_path_suffix,
    hash_file,
    parse_datetime,
    timestamp_to_datetime,
    unique_id,
)

ENDING_DOUBLE_STAR_RE = r"(?:/+\*\*)+/*$"

AnnotationMeta = Dict[str, Union[str, int, None]]
DataObjectMeta = Dict[str, Union[str, Dict[str, Union[str, int]]]]
DataToWrite = Tuple[Path, bytes, bool]
IndexingJob = Tuple["IndexingConfig", List[str]]
IndexingJobMapping = Dict[AbstractFileSystem, List[IndexingJob]]
FSPathsMapping = Dict[AbstractFileSystem, List[str]]


class FileSystemPath(NamedTuple):
    fs: AbstractFileSystem
    path: str


@dataclass
class IndexingConfig:
    save_data_object_path_info: bool


DEFAULT_CONFIG = IndexingConfig(save_data_object_path_info=True)
INDEXED_EPHEMERAL_CONFIG = IndexingConfig(save_data_object_path_info=False)


def get_storage_files_for_paths(
    paths: List[str],
    default_format: bool = False,
) -> Dict[AbstractFileSystem, List[str]]:
    storage_files: Dict[AbstractFileSystem, Tuple[List[str], Set[str]]] = {}
    for path in paths:
        fs, paths_found = get_storage_files(
            path,
            default_format=default_format,
        )
        try:
            fs_paths, seen = storage_files[fs]
        except KeyError:
            fs_paths, seen = [], set()
            storage_files[fs] = fs_paths, seen
        for file in paths_found:
            if file not in seen:
                fs_paths.append(file)
                seen.add(file)
    return {fs: fs_paths for fs, (fs_paths, _) in storage_files.items()}


def get_storage_files(
    path: str,
    default_format: bool = False,
) -> Tuple[AbstractFileSystem, List[str]]:
    """
    Get storage files for indexing that match the glob, `path`.

    Because every file path under `path` is returned, any final "/**" does not
    change the result. First, `path` is expanded with any final "/**" removed
    and any matching files are included in the result. Corresponding data
    object file paths are added for matched annotation file paths and vice
    versa. Then everything under matched directories will be added to the
    result.

    The current implementation may result in some directory paths and some
    duplicate paths being included.
    """
    fs = fsspec.filesystem("file")
    if is_hidden_fsspec_path(path):
        return fs, []
    # "path/**", "path/**/**" or "path/**/" are treated the same as just "path"
    # so we strip off any "/**" or "/**/" at the end
    path = re.sub(ENDING_DOUBLE_STAR_RE, "", path)
    # find corresponding data object for annotation match and vice versa
    # for any files the expanded `path` glob matches
    file_match_globs = []
    for mpath in fs.expand_path(path):
        if not is_hidden_fsspec_path(mpath) and fs.isfile(mpath):
            file_match_globs.append(mpath)
            if default_format:
                # TODO: Check all extension levels (i.e. for abc.tar.gz use
                # .tar.gz and .gz)
                p_without_ext, ext = os.path.splitext(path)
                if ext == ".json":
                    file_match_globs.append(p_without_ext)
                    file_match_globs.append(p_without_ext + ".*")
                else:
                    file_match_globs.append(p_without_ext + ".json")
    files = (
        [i for f in file_match_globs for i in fs.glob(f)]
        if file_match_globs
        else []
    )
    # capture everything under any directories the `path` glob matches
    for file in fs.expand_path(path, recursive=True):
        if not is_hidden_fsspec_path(file) and fs.isfile(file):
            files.append(file)
    return fs, files


def is_hidden_fsspec_path(path: str) -> bool:
    return re.search(r"^\.|/\.", path) is not None


def group_storage_files_by_type(
    storage_files: Iterable[str],
) -> Tuple[List[str], List[str]]:
    data_object_files = []
    annotation_files = []
    seen = set()
    fs = fsspec.filesystem("file")
    for storage_file in storage_files:
        if storage_file not in seen:
            seen.add(storage_file)
            if fs.isfile(storage_file):
                if storage_file.endswith(".json"):
                    annotation_files.append(storage_file)
                else:
                    data_object_files.append(storage_file)
    return data_object_files, annotation_files


def expand_dir_paths(
    paths: Iterable[str],
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
    fs = fsspec.filesystem("file")
    for path in paths:
        fs_paths = path_sets.setdefault(fs, set())
        for p in fs.expand_path(paths):
            if fs.isdir(p):
                fs_paths.add(p)
    path_lists: Dict[AbstractFileSystem, List[str]] = {}
    for fs, fs_path_set in path_sets.items():
        paths = sorted(fs_path_set)
        for i in range(len(paths) - 1):
            if paths[i + 1].startswith(paths[i]):
                raise IndexingException(
                    f"Paths passed with the {Format.INFER} format should "
                    "match non-overlapping directories. Found overlapping "
                    "directories:\n"
                    f"{paths[i]!r}\n"
                    f"{paths[i + 1]!r}",
                )
        path_lists[fs] = paths
    return path_lists


def copy_to_read_add_storage(
    data_object_files: FSPathsMapping,
    annotation_files_by_path: FSPathsMapping,
    read_add_location: StorageLocation,
    hashes: Mapping[AbstractFileSystem, Mapping[str, str]],
    strict_format: bool,
) -> Tuple[
    Dict[AbstractFileSystem, Dict[str, FileSystemPath]],
    Dict[AbstractFileSystem, Dict[str, FileSystemPath]],
]:

    fs = fsspec.filesystem(read_add_location.protocol)
    assert (
        fs.protocol == "file"
    )  # until get_file below is replaced with transfer
    base_dir = fs.sep.join(
        [
            read_add_location.path,
            "ldb-autoimport",
            date.today().isoformat(),
            unique_id(),
        ],
    )
    fs.makedirs(base_dir, exist_ok=True)
    old_to_new_files = {}
    old_to_new_annot_files = {}
    for rfs, paths in data_object_files.items():
        fs_id = rfs.protocol  # TODO use actual fs_id
        rfs_base_dir = fs.sep.join([base_dir, fs_id])

        rfs_annotation_paths = set(annotation_files_by_path.get(rfs, []))

        fs_old_to_new_files = {}
        fs_old_to_new_annot_files = {}
        for path in paths:
            dest = fs.sep.join(
                [rfs_base_dir] + path.lstrip(rfs.sep).split(rfs.sep),
            )

            annotation_dest = None
            annotation_file = data_object_path_to_annotation_path(path)
            if annotation_file in rfs_annotation_paths:
                annotation_dest = fs.sep.join(
                    [rfs_base_dir]
                    + annotation_file.lstrip(rfs.sep).split(rfs.sep),
                )
            elif strict_format:
                continue
            try:
                fs.makedirs(
                    fs._parent(dest),  # pylint: disable=protected-access)
                    exist_ok=True,
                )
                if annotation_dest is not None:
                    # TODO: make transfer so this works with remote read-add storage
                    rfs.get_file(
                        annotation_file,
                        annotation_dest,
                    )
                rfs.get_file(path, dest)
            except FileNotFoundError:
                # Use hash instead of preserving path if path is too long
                try:
                    data_object_hash = hashes[rfs][path]
                except KeyError:
                    data_object_hash = hash_file(
                        rfs,
                        path,
                    )
                dest = fs.sep.join(
                    [
                        base_dir,
                        data_object_hash + get_fsspec_path_suffix(path),
                    ],
                )
                if annotation_dest is not None:
                    annotation_dest = data_object_path_to_annotation_path(dest)
                    rfs.get_file(
                        annotation_file,
                        annotation_dest,
                    )
                rfs.get_file(path, dest, protocol=fs.protocol)
            fs_old_to_new_files[path] = FileSystemPath(fs, dest)
            if annotation_dest is not None:
                fs_old_to_new_annot_files[annotation_file] = FileSystemPath(
                    fs,
                    annotation_dest,
                )
        if fs_old_to_new_files:
            old_to_new_files[fs] = fs_old_to_new_files
        if fs_old_to_new_annot_files:
            old_to_new_annot_files[fs] = fs_old_to_new_annot_files
    return old_to_new_files, old_to_new_annot_files


def data_object_path_to_annotation_path(path: str) -> str:
    return os.path.splitext(path)[0] + ".json"


def separate_local_and_cloud_files(
    storage_files: FSPathsMapping,
) -> Tuple[FSPathsMapping, FSPathsMapping]:
    local = {}
    cloud = {}
    for fs, paths in storage_files.items():
        if fs.protocol == "file":
            local[fs] = paths
        else:
            cloud[fs] = paths
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
            non_storage[fs] = [p for p in paths]
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
                hash_str = hashes[fs][path]
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
                raise NotAStorageLocationError(
                    "Found file outside of configured storage locations: "
                    f"{path}",
                )


def filter_storage_locations(
    fs: AbstractFileSystem,
    storage_locations: Sequence[StorageLocation],
) -> List[StorageLocation]:
    # TODO use fs_id
    return [s for s in storage_locations if s.protocol == fs.protocol]


def in_storage_locations(
    path: str,
    storage_locations: Sequence[StorageLocation],
) -> bool:
    for loc in storage_locations:
        if path.startswith(loc.path):
            return True
    return False


def construct_data_object_meta(
    fs_path: FileSystemPath,
    prev_meta: Dict[str, Any],
    current_timestamp: str,
) -> DataObjectMeta:
    path = fs_path.path
    fs_info = os.stat(path)  # TODO: use fsspec

    atime = timestamp_to_datetime(fs_info.st_atime)
    mtime = timestamp_to_datetime(fs_info.st_mtime)
    ctime = timestamp_to_datetime(fs_info.st_ctime)

    if prev_meta:
        first_indexed = prev_meta["first_indexed"]
        tags = prev_meta["tags"]
        alternate_paths = prev_meta["alternate_paths"]

        atime = max(atime, parse_datetime(prev_meta["fs"]["atime"]))
        mtime = max(mtime, parse_datetime(prev_meta["fs"]["mtime"]))
        ctime = max(ctime, parse_datetime(prev_meta["fs"]["ctime"]))
    else:
        first_indexed = current_timestamp
        tags = []
        alternate_paths = []

    path_info = {
        "fs_id": "",
        "protocol": "file",
        "path": path,
    }
    if path_info not in alternate_paths:
        alternate_paths.append(path_info)
    return {
        "type": get_filetype(path),
        "first_indexed": first_indexed,
        "last_indexed": current_timestamp,
        "last_indexed_by": getpass.getuser(),
        "tags": tags,
        "alternate_paths": alternate_paths,
        "fs": {
            **path_info,
            "size": fs_info.st_size,
            "mode": fs_info.st_mode,
            "uid": fs_info.st_uid,
            "gid": fs_info.st_gid,
            "atime": format_datetime(atime),
            "mtime": format_datetime(mtime),
            "ctime": format_datetime(ctime),
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
