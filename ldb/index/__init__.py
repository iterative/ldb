import getpass
import json
import os
import re
from abc import ABC
from dataclasses import dataclass, field
from datetime import date
from itertools import chain, repeat
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
from funcy.objects import cached_property

from ldb.data_formats import INDEX_FORMATS, Format
from ldb.dataset import get_collection_dir_keys
from ldb.exceptions import (
    DataObjectNotFoundError,
    IndexingException,
    LDBException,
    NotAStorageLocationError,
)
from ldb.path import InstanceDir
from ldb.storage import StorageLocation, get_storage_locations
from ldb.typing import JSONDecoded, JSONObject
from ldb.utils import (
    current_time,
    format_datetime,
    get_filetype,
    get_fsspec_path_suffix,
    get_hash_path,
    hash_data,
    hash_file,
    json_dumps,
    load_data_file,
    parse_datetime,
    timestamp_to_datetime,
    unique_id,
    write_data_file,
)

AnnotationMeta = Dict[str, Union[str, int, None]]
DataObjectMeta = Dict[str, Union[str, Dict[str, Union[str, int]]]]


class IndexedObjectResult(NamedTuple):
    found_data_object: bool
    found_annotation: bool
    new_data_object: bool
    new_annotation: bool
    data_object_hash: str


@dataclass
class IndexingResult:
    num_found_data_objects: int = 0
    num_found_annotations: int = 0
    num_new_data_objects: int = 0
    num_new_annotations: int = 0
    data_object_hashes: List[str] = field(default_factory=list)

    def append(self, item: IndexedObjectResult) -> None:
        self.num_found_data_objects += item.found_data_object
        self.num_found_annotations += item.found_annotation
        self.num_new_data_objects += item.new_data_object
        self.num_new_annotations += item.new_annotation
        self.data_object_hashes.append(item.data_object_hash)

    def summary(self, finished: bool = True) -> str:
        if finished:
            heading = "Finished indexing"
        else:
            heading = "Unable to finish indexing. Partial indexing results"
        return (
            f"\n{heading}:\n"
            f"  Found data objects: {self.num_found_data_objects:9d}\n"
            f"  Found annotations:  {self.num_found_annotations:9d}\n"
            f"  New data objects:   {self.num_new_data_objects:9d}\n"
            f"  New annotations:    {self.num_new_annotations:9d}"
        )


def autodetect_format(
    files: Sequence[OpenFile],
    annotation_files: Sequence[OpenFile],
) -> str:
    if annotation_files and not files:
        return Format.ANNOT
    return Format.STRICT


def index(
    ldb_dir: Path,
    paths: Sequence[str],
    read_any_cloud_location: bool = False,
    fmt: str = Format.AUTO,
) -> IndexingResult:
    fmt = INDEX_FORMATS[fmt]
    paths = [os.path.abspath(p) for p in paths]
    files = get_storage_files_for_paths(paths, default_format=True)
    if not files:
        raise LDBException(
            "No files or directories found matching the given paths.",
        )

    files, annotation_files = group_storage_files_by_type(
        files,
    )
    if fmt == Format.AUTO:
        fmt = autodetect_format(files, annotation_files)
    if fmt in (Format.STRICT, Format.BARE):
        return index_pairs(
            ldb_dir,
            files,
            annotation_files,
            read_any_cloud_location,
            fmt,
        )
    if fmt == Format.ANNOT:
        return index_annotation_only(ldb_dir, annotation_files)
    if fmt == Format.INFER:
        raise NotImplementedError
    raise ValueError(f"Not a valid indexing format: {fmt}")


def index_pairs(
    ldb_dir: Path,
    files: List[OpenFile],
    annotation_files: List[OpenFile],
    read_any_cloud_location: bool,
    fmt: str,
) -> IndexingResult:
    storage_locations = get_storage_locations(ldb_dir)
    local_files, cloud_files = separate_local_and_cloud_files(files)
    if not read_any_cloud_location:
        validate_locations_in_storage(cloud_files, storage_locations)
    (
        local_storage_files,
        ephemeral_files,
    ) = separate_storage_and_non_storage_files(local_files, storage_locations)
    ephemeral_hashes = {f.path: hash_file(f) for f in ephemeral_files}
    existing_hashes = set(
        get_collection_dir_keys(ldb_dir / InstanceDir.DATA_OBJECT_INFO),
    )
    indexed_ephemeral_files, ephemeral_files = separate_indexed_files(
        existing_hashes,
        ephemeral_hashes,
        ephemeral_files,
    )
    files = cloud_files + local_storage_files
    annotation_files_by_path = {f.path: f for f in annotation_files}
    if ephemeral_files:
        read_add_location = next(
            (loc for loc in storage_locations if loc.read_and_add),
            None,
        )
        if read_add_location is None:
            raise LDBException(
                "No read-add storage configured. See 'ldb add-storage -h'",
            )
        (
            added_storage_files,
            annotation_files_by_path,
        ) = copy_to_read_add_storage(
            ephemeral_files,
            annotation_files_by_path,
            read_add_location,
        )
        files.extend(added_storage_files)
    files.extend(indexed_ephemeral_files)
    indexed_ephemeral_bools = chain(
        repeat(False, len(files) - len(indexed_ephemeral_files)),
        repeat(True),
    )
    return index_files(
        ldb_dir,
        files,
        indexed_ephemeral_bools,
        annotation_files_by_path,
        ephemeral_hashes,
        strict_format=fmt == Format.STRICT,
    )


@dataclass
class IndexingItem(ABC):
    ldb_dir: Path
    current_timestamp: str

    @cached_property
    def data_object_dir(self) -> Path:
        return get_hash_path(
            self.ldb_dir / InstanceDir.DATA_OBJECT_INFO,
            self.data_object_hash,
        )

    @cached_property
    def data_object_meta_file_path(self) -> Path:
        return self.data_object_dir / "meta"

    @cached_property
    def annotation_meta_dir_path(self) -> Path:
        return self.data_object_dir / "annotations"

    @cached_property
    def annotation_meta_file_path(self) -> Path:
        return self.annotation_meta_dir_path / self.annotation_hash

    @cached_property
    def annotation_dir(self) -> Path:
        return get_hash_path(
            self.ldb_dir / InstanceDir.ANNOTATIONS,
            self.annotation_hash,
        )

    @cached_property
    def data_object_hash(self) -> str:
        raise NotImplementedError

    @cached_property
    def annotation_hash(self) -> str:
        return hash_data(
            self.annotation_ldb_content_bytes + self.annotation_content_bytes,
        )

    @cached_property
    def data_object_meta(self) -> DataObjectMeta:
        raise NotImplementedError

    @cached_property
    def annotation_ldb_content(  # pylint: disable=no-self-use
        self,
    ) -> JSONObject:
        return {
            "user_version": None,
            "schema_version": None,
        }

    @cached_property
    def annotation_content(self) -> JSONDecoded:
        raise NotImplementedError

    @cached_property
    def annotation_ldb_content_bytes(self) -> bytes:
        return json_dumps(self.annotation_ldb_content).encode()

    @cached_property
    def annotation_content_bytes(self) -> bytes:
        return json_dumps(self.annotation_content).encode()

    @cached_property
    def annotation_version(self) -> int:
        try:
            return len(list(self.annotation_meta_dir_path.iterdir())) + 1
        except FileNotFoundError:
            return 1


@dataclass
class AnnotationFileIndexingItem(IndexingItem):
    annotation_file: OpenFile

    @cached_property
    def annotation_meta(self) -> AnnotationMeta:
        fs_info = self.annotation_file.fs.info(self.annotation_file.path)
        curr_mtime = fs_info.get("created")
        prev_annotation = (
            load_data_file(self.annotation_meta_file_path)
            if self.annotation_meta_file_path.exists()
            else {}
        )
        return construct_annotation_meta(
            prev_annotation,
            self.current_timestamp,
            self.annotation_version,
            curr_mtime,
        )

    @cached_property
    def annotation_content(self) -> JSONDecoded:
        return get_annotation_content(self.annotation_file)


@dataclass
class DataObjectFileIndexingItem(IndexingItem):
    data_object_file: OpenFile
    save_data_object_path_info: bool
    data_object_hash_cache: Mapping[str, str]

    @cached_property
    def data_object_hash(self) -> str:
        hash_str = self.data_object_hash_cache.get(self.data_object_file.path)
        if hash_str is None:
            hash_str = hash_file(self.data_object_file)
        return hash_str

    @cached_property
    def data_object_meta(self) -> DataObjectMeta:
        if not self.save_data_object_path_info:
            meta_contents: DataObjectMeta = load_data_file(
                self.data_object_meta_file_path,
            )
            meta_contents["last_indexed"] = self.current_timestamp
        else:
            meta_contents = construct_data_object_meta(
                self.data_object_file,
                load_data_file(self.data_object_meta_file_path)
                if self.data_object_meta_file_path.exists()
                else {},
                self.current_timestamp,
            )
        return meta_contents


@dataclass
class PairIndexingItem(AnnotationFileIndexingItem, DataObjectFileIndexingItem):
    @cached_property
    def data_object_dir(self) -> Path:
        return get_hash_path(
            self.ldb_dir / InstanceDir.DATA_OBJECT_INFO,
            self.data_object_hash,
        )

    @cached_property
    def data_object_meta_file_path(self) -> Path:
        return self.data_object_dir / "meta"


@dataclass
class AnnotationOnlyIndexingItem(AnnotationFileIndexingItem):
    @cached_property
    def annotation_file_contents(self) -> JSONObject:
        return get_annotation_content(  # type: ignore[return-value]
            self.annotation_file,
        )

    @cached_property
    def data_object_hash(self) -> str:
        try:
            return self.annotation_file_contents["ldb_meta"]["data_object_id"]  # type: ignore[no-any-return] # noqa: E501
        except KeyError as exc:
            raise IndexingException(
                "Missing ldb_meta.data_object_id key for annotation-only "
                f"format: {self.annotation_file.path}",
            ) from exc

    @cached_property
    def data_object_meta(self) -> DataObjectMeta:
        meta_contents: DataObjectMeta = load_data_file(
            self.data_object_meta_file_path,
        )
        meta_contents["last_indexed"] = self.current_timestamp
        return meta_contents

    @cached_property
    def annotation_content(self) -> JSONObject:  # type: ignore[override]
        return self.annotation_file_contents["annotation"]  # type: ignore[no-any-return] # noqa: E501


def index_annotation_only(
    ldb_dir: Path,
    annotation_files: Iterable[OpenFile],
) -> IndexingResult:
    result = IndexingResult()
    try:
        for file in annotation_files:
            result.append(index_single_annotation_only_file(ldb_dir, file))
    except Exception:
        print(result.summary(finished=False))
        print()
        raise
    return result


def index_single_annotation_only_file(
    ldb_dir: Path,
    annotation_file: OpenFile,
) -> IndexedObjectResult:
    current_timestamp = format_datetime(current_time())
    item = AnnotationOnlyIndexingItem(
        ldb_dir,
        current_timestamp,
        annotation_file,
    )
    if not item.data_object_dir.exists():
        raise DataObjectNotFoundError(
            f"Data object not found: 0x{item.data_object_hash} "
            f"(annotation_file_path={annotation_file.path!r})",
        )
    to_write = []
    new_annotation = not item.annotation_meta_file_path.is_file()
    annotation_meta_bytes = json_dumps(item.annotation_meta).encode()
    to_write.append(
        (item.annotation_meta_file_path, annotation_meta_bytes, True),
    )
    if not item.annotation_dir.is_dir():
        to_write.append(
            (
                item.annotation_dir / "ldb",
                item.annotation_ldb_content_bytes,
                False,
            ),
        )
        to_write.append(
            (
                item.annotation_dir / "user",
                item.annotation_content_bytes,
                False,
            ),
        )
    to_write.append(
        (
            item.data_object_dir / "current",
            item.annotation_hash.encode(),
            True,
        ),
    )
    for file_path, data, overwrite_existing in to_write:
        write_data_file(file_path, data, overwrite_existing)
    return IndexedObjectResult(
        found_data_object=False,
        found_annotation=True,
        new_data_object=False,
        new_annotation=new_annotation,
        data_object_hash=item.data_object_hash,
    )


def copy_to_read_add_storage(
    data_object_files: Sequence[OpenFile],
    annotation_files_by_path: Dict[str, OpenFile],
    read_add_location: StorageLocation,
) -> Tuple[List[OpenFile], Dict[str, OpenFile]]:
    fs = fsspec.filesystem(read_add_location.protocol)
    base_dir = fs.sep.join(
        [
            read_add_location.path,
            "ldb-autoimport",
            date.today().isoformat(),
            unique_id(),
        ],
    )
    annotation_files_by_path = annotation_files_by_path.copy()
    fs.makedirs(base_dir, exist_ok=True)
    new_files = []
    for file in data_object_files:
        dest = file.path
        if file.fs.protocol == "file":
            dest = re.sub("^[A-Za-z]:", "", make_path_posix(dest))
        dest = fs.sep.join(
            [base_dir] + dest.lstrip(file.fs.sep).split(file.fs.sep),
        )

        annotation_file = annotation_files_by_path.get(
            data_object_path_to_annotation_path(file.path),
        )
        annotation_dest = None
        if annotation_file is not None:
            annotation_dest = data_object_path_to_annotation_path(dest)
        try:
            fs.makedirs(
                fs._parent(dest),  # pylint: disable=protected-access)
                exist_ok=True,
            )
            if annotation_dest is not None:
                file.fs.put_file(
                    annotation_file.path,
                    annotation_dest,
                    protocol=fs.protocol,
                )
            file.fs.put_file(file.path, dest, protocol=fs.protocol)
        except FileNotFoundError:
            data_object_hash = hash_file(file)
            dest = fs.sep.join(
                [
                    base_dir,
                    data_object_hash + get_fsspec_path_suffix(file.path),
                ],
            )
            if annotation_file is not None:
                annotation_dest = data_object_path_to_annotation_path(dest)
                file.fs.put_file(
                    annotation_file.path,
                    annotation_dest,
                    protocol=fs.protocol,
                )
            file.fs.put_file(file.path, dest, protocol=fs.protocol)
        new_files.append(OpenFile(fs, dest))
        if annotation_dest is not None:
            annotation_files_by_path[annotation_dest] = OpenFile(
                fs,
                annotation_dest,
            )
    return new_files, annotation_files_by_path


def index_files(
    ldb_dir: Path,
    data_object_files: List[OpenFile],
    indexed_ephemeral_bools: Iterable[bool],
    annotation_files_by_path: Dict[str, OpenFile],
    hashes: Mapping[str, str],
    strict_format: bool = True,
) -> IndexingResult:
    result = IndexingResult()
    try:
        for data_object_file, is_indexed_ephemeral in zip(
            data_object_files,
            indexed_ephemeral_bools,
        ):
            obj_result = index_single_object(
                ldb_dir,
                data_object_file,
                is_indexed_ephemeral,
                annotation_files_by_path,
                hashes,
                strict_format=strict_format,
            )
            if obj_result is not None:
                result.append(obj_result)
    except Exception:
        print(result.summary(finished=False))
        print()
        raise
    return result


def index_single_object(
    ldb_dir: Path,
    data_object_file: OpenFile,
    is_indexed_ephemeral: bool,
    annotation_files_by_path: Dict[str, OpenFile],
    hashes: Mapping[str, str],
    strict_format: bool = True,
) -> Optional[IndexedObjectResult]:
    annotation_file = annotation_files_by_path.get(
        data_object_path_to_annotation_path(data_object_file.path),
    )
    if annotation_file is None and strict_format:
        return None

    current_timestamp = format_datetime(current_time())
    item = PairIndexingItem(
        ldb_dir,
        current_timestamp,
        data_object_file,
        not is_indexed_ephemeral,
        hashes,
        annotation_file,
    )
    new_data_object = not item.data_object_dir.is_dir()
    to_write = [
        (
            item.data_object_meta_file_path,
            json_dumps(item.data_object_meta).encode(),
            True,
        ),
    ]

    found_annotation = item.annotation_file is not None
    new_annotation = False
    if found_annotation:
        found_annotation = True
        new_annotation = not item.annotation_meta_file_path.is_file()
        annotation_meta_bytes = json_dumps(item.annotation_meta).encode()
        to_write.append(
            (item.annotation_meta_file_path, annotation_meta_bytes, True),
        )
        if not item.annotation_dir.is_dir():
            to_write.append(
                (
                    item.annotation_dir / "ldb",
                    item.annotation_ldb_content_bytes,
                    False,
                ),
            )
            to_write.append(
                (
                    item.annotation_dir / "user",
                    item.annotation_content_bytes,
                    False,
                ),
            )
        to_write.append(
            (
                item.data_object_dir / "current",
                item.annotation_hash.encode(),
                True,
            ),
        )

    for file_path, data, overwrite_existing in to_write:
        write_data_file(file_path, data, overwrite_existing)

    return IndexedObjectResult(
        found_data_object=True,
        found_annotation=found_annotation,
        new_data_object=new_data_object,
        new_annotation=new_annotation,
        data_object_hash=item.data_object_hash,
    )


def data_object_path_to_annotation_path(path: str) -> str:
    return os.path.splitext(path)[0] + ".json"


def get_storage_files_for_paths(
    paths: List[str],
    default_format: bool = False,
) -> List[OpenFile]:
    seen = set()
    storage_files = []
    for path in paths:
        for file in get_storage_files(path, default_format=default_format):
            if file.path not in seen:
                storage_files.append(file)
                seen.add(file.path)
    return storage_files


def get_storage_files(
    path: str,
    default_format: bool = False,
) -> List[OpenFile]:
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
    if is_hidden_fsspec_path(path):
        return []
    fs = fsspec.filesystem("file")
    # "path/**", "path/**/**" or "path/**/" are treated the same as just "path"
    # so we strip off any "/**" or "/**/" at the end
    path = re.sub(r"(?:/\*\*)+/?$", "", path)
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
        list(fsspec.open_files(file_match_globs)) if file_match_globs else []
    )
    # capture everything under any directories the `path` glob matches
    for file in fsspec.open_files(path + "/**"):
        if not is_hidden_fsspec_path(file.path):
            files.append(file)
    return files


def separate_local_and_cloud_files(
    storage_files: Sequence[OpenFile],
) -> Tuple[List[OpenFile], List[OpenFile]]:
    local = []
    cloud = []
    for file in storage_files:
        if file.fs.protocol == "file":
            local.append(file)
        else:
            cloud.append(file)
    return local, cloud


def separate_storage_and_non_storage_files(
    files: Sequence[OpenFile],
    storage_locations: List[StorageLocation],
) -> Tuple[List[OpenFile], List[OpenFile]]:
    storage = []
    non_storage = []
    for file in files:
        if in_storage_locations(
            file.path,
            file.fs.protocol,
            storage_locations,
        ):
            storage.append(file)
        else:
            non_storage.append(file)
    return storage, non_storage


def separate_indexed_files(
    existing_hashes: Set[str],
    hashes: Mapping[str, str],
    files: Iterable[OpenFile],
) -> Tuple[List[OpenFile], List[OpenFile]]:
    indexed = []
    not_indexed = []
    for file in files:
        hash_str = hashes.get(file.path)
        if hash_str is not None and hash_str in existing_hashes:
            indexed.append(file)
        else:
            not_indexed.append(file)
    return indexed, not_indexed


def validate_locations_in_storage(
    storage_files: Sequence[OpenFile],
    storage_locations: List[StorageLocation],
) -> None:
    for storage_file in storage_files:
        if in_storage_locations(
            storage_file.path,
            storage_file.fs.protocol,
            storage_locations,
        ):
            raise NotAStorageLocationError(
                "Found file outside of configured storage locations: "
                f"{storage_file.path}",
            )


def in_storage_locations(
    path: str,
    protocol: str,
    storage_locations: Sequence[StorageLocation],
) -> bool:
    return any(
        loc.protocol == protocol and path.startswith(loc.path)
        for loc in storage_locations
    )


def is_hidden_fsspec_path(path: str) -> bool:
    return re.search(r"^\.|/\.", path) is not None


def group_storage_files_by_type(
    storage_files: Iterable[OpenFile],
) -> Tuple[List[OpenFile], List[OpenFile]]:
    data_object_files = []
    annotation_files = []
    seen = set()
    for storage_file in storage_files:
        if storage_file.path not in seen:
            seen.add(storage_file.path)
            if storage_file.fs.isfile(storage_file.path):
                if storage_file.path.endswith(".json"):
                    annotation_files.append(storage_file)
                else:
                    data_object_files.append(storage_file)
    return data_object_files, annotation_files


def construct_data_object_meta(
    file: OpenFile,
    prev_meta: Dict[str, Any],
    current_timestamp: str,
) -> DataObjectMeta:
    fs_info = os.stat(file.path)

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
        "path": file.path,
    }
    if path_info not in alternate_paths:
        alternate_paths.append(path_info)
    return {
        "type": get_filetype(file.path),
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


def get_annotation_content(
    annotation_file: OpenFile,
) -> JSONDecoded:
    try:
        with annotation_file as open_annotation_file:
            annotation_str = open_annotation_file.read()
        original_content: JSONDecoded = json.loads(annotation_str)
    except Exception as exc:
        raise IndexingException(
            f"Unable to parse JSON annotation: {annotation_file.path!r}\n"
            f"{type(exc).__name__}: {exc}",
        ) from exc
    return original_content


def construct_annotation_meta(
    prev_annotation_meta: AnnotationMeta,
    current_timestamp: str,
    version: int,
    curr_mtime: Optional[float],
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
        mtimes.append(timestamp_to_datetime(curr_mtime))

    mtime = format_datetime(max(mtimes)) if mtimes else None
    return {
        "version": version,
        "mtime": mtime,
        "first_indexed_time": first_indexed_time,
        "last_indexed_time": current_timestamp,
    }
