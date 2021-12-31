# pylint: disable=too-many-lines
import getpass
import json
import os
import re
from abc import ABC
from dataclasses import dataclass, field
from datetime import date, datetime
from itertools import chain, repeat
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
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

ENDING_DOUBLE_STAR_RE = r"(?:/+\*\*)+/*$"

AnnotationMeta = Dict[str, Union[str, int, None]]
DataObjectMeta = Dict[str, Union[str, Dict[str, Union[str, int]]]]
DataToWrite = Tuple[Path, bytes, bool]


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


def expand_dir_paths(paths: Iterable[str]) -> List[str]:
    fs = fsspec.filesystem("file")
    for path in paths:
        path, num = re.subn(ENDING_DOUBLE_STAR_RE, "", path)
        if num:
            raise IndexingException(
                f"Paths passed with the {Format.INFER} format should only "
                "match directories, so globs with a final /** should not be "
                "used",
            )

    paths = sorted({p for p in fs.expand_path(paths) if fs.isdir(p)})
    for i in range(len(paths) - 1):
        if paths[i + 1].startswith(paths[i]):
            raise IndexingException(
                f"Paths passed with the {Format.INFER} format should match "
                "non-overlapping directories. Found overlapping directories:\n"
                f"{paths[i]!r}\n"
                f"{paths[i + 1]!r}",
            )
    return paths


def index(
    ldb_dir: Path,
    paths: Sequence[str],
    read_any_cloud_location: bool = False,
    fmt: str = Format.AUTO,
) -> IndexingResult:
    fmt = INDEX_FORMATS[fmt]
    paths = [os.path.abspath(p) for p in paths]

    dir_paths = []
    if fmt == Format.INFER:
        dir_paths = expand_dir_paths(paths)
        file_seqs = [
            fsspec.open_files(p.rstrip("/") + "/**") for p in dir_paths
        ]
        dir_path_to_files = {d: f for d, f in zip(dir_paths, file_seqs) if f}
        files = list(chain(*dir_path_to_files.values()))
    else:
        files = get_storage_files_for_paths(
            paths,
            default_format=fmt in (Format.STRICT, Format.BARE),
        )
    if not files:
        raise LDBException(
            "No files found matching the given paths.",
        )

    files, annotation_files = group_storage_files_by_type(
        files,
    )
    if fmt == Format.AUTO:
        fmt = autodetect_format(files, annotation_files)
    if fmt in (Format.STRICT, Format.BARE):
        indexer = PairIndexer(
            ldb_dir,
            files,
            annotation_files,
            read_any_cloud_location,
            fmt == Format.STRICT,
        )
        indexer.index()
        return indexer.result
    if fmt == Format.INFER:
        if annotation_files:
            first_path = annotation_files[0].path
            raise IndexingException(
                f"No annotation files should be present for {Format.INFER} "
                "format.\n"
                f"Found {len(annotation_files)} JSON files.\n"
                f"First path: {first_path}",
            )
        indexer = InferredIndexer(
            ldb_dir,
            files,
            read_any_cloud_location,
            fmt == Format.STRICT,
            dir_path_to_files,
        )
        indexer.index()
        return indexer.result
    if fmt == Format.ANNOT:
        return index_annotation_only(ldb_dir, annotation_files)
    raise ValueError(f"Not a valid indexing format: {fmt}")


class Indexer:
    def __init__(self, ldb_dir: Path) -> None:
        self.ldb_dir = ldb_dir
        self.result = IndexingResult()
        self.hashes: Dict[str, str] = {}

    def index(self) -> None:
        try:
            self._index()
        except Exception:
            print(self.result.summary(finished=False), "\n", sep="")
            raise

    def _index(self) -> None:
        raise NotImplementedError

    def process_files(self) -> Any:
        raise NotImplementedError


class PairIndexer(Indexer):
    def __init__(
        self,
        ldb_dir: Path,
        files: List[OpenFile],
        annotation_files: List[OpenFile],
        read_any_cloud_location: bool,
        strict_format: bool,
    ) -> None:
        super().__init__(ldb_dir)
        self.files = files
        self.annotation_files = annotation_files
        self.read_any_cloud_location = read_any_cloud_location
        self.strict_format = strict_format
        self.old_to_new_files: Dict[OpenFile, OpenFile] = {}
        self.old_to_new_annot_files: Dict[OpenFile, OpenFile] = {}

    def _index(self) -> None:
        (
            files,
            indexed_ephemeral_bools,
            annotation_files_by_path,
        ) = self.process_files()
        self.index_files(
            files,
            indexed_ephemeral_bools,
            annotation_files_by_path,
        )

    def process_files(
        self,
    ) -> Tuple[List[OpenFile], Iterator[bool], Dict[str, OpenFile]]:
        storage_locations = get_storage_locations(self.ldb_dir)
        local_files, cloud_files = separate_local_and_cloud_files(self.files)
        if not self.read_any_cloud_location:
            validate_locations_in_storage(cloud_files, storage_locations)
        (
            local_storage_files,
            ephemeral_files,
        ) = separate_storage_and_non_storage_files(
            local_files,
            storage_locations,
        )
        for f in ephemeral_files:
            self.hashes[f.path] = hash_file(f)
        existing_hashes = set(
            get_collection_dir_keys(
                self.ldb_dir / InstanceDir.DATA_OBJECT_INFO,
            ),
        )
        indexed_ephemeral_files, ephemeral_files = separate_indexed_files(
            existing_hashes,
            self.hashes,
            ephemeral_files,
        )
        files = cloud_files + local_storage_files
        annotation_files_by_path = {f.path: f for f in self.annotation_files}
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
                self.old_to_new_files,
                self.old_to_new_annot_files,
            ) = copy_to_read_add_storage(
                ephemeral_files,
                annotation_files_by_path,
                read_add_location,
                self.hashes,
                strict_format=self.strict_format,
            )

            added_storage_files = self.old_to_new_files.values()
            for f in self.old_to_new_annot_files.values():
                annotation_files_by_path[f.path] = f
            files.extend(added_storage_files)
        files.extend(indexed_ephemeral_files)
        indexed_ephemeral_bools = chain(
            repeat(False, len(files) - len(indexed_ephemeral_files)),
            repeat(True),
        )
        return (
            files,
            indexed_ephemeral_bools,
            annotation_files_by_path,
        )

    def index_files(
        self,
        data_object_files: List[OpenFile],
        indexed_ephemeral_bools: Iterable[bool],
        annotation_files_by_path: Dict[str, OpenFile],
    ) -> None:
        for data_object_file, is_indexed_ephemeral in zip(
            data_object_files,
            indexed_ephemeral_bools,
        ):
            annotation_file = annotation_files_by_path.get(
                data_object_path_to_annotation_path(data_object_file.path),
            )
            if annotation_file is None and self.strict_format:
                continue
            obj_result = self.index_single_pair(
                data_object_file,
                is_indexed_ephemeral,
                annotation_file,
            )
            self.result.append(obj_result)

    def index_single_pair(
        self,
        data_object_file: OpenFile,
        is_indexed_ephemeral: bool,
        annotation_file: OpenFile,
    ) -> IndexedObjectResult:
        return PairIndexingItem(
            self.ldb_dir,
            current_time(),
            data_object_file,
            not is_indexed_ephemeral,
            self.hashes,
            annotation_file,
        ).index_data()


class InferredIndexer(PairIndexer):
    def __init__(
        self,
        ldb_dir: Path,
        files: List[OpenFile],
        read_any_cloud_location: bool,
        strict_format: bool,
        dir_path_to_files: Mapping[str, Sequence[OpenFile]],
    ) -> None:
        super().__init__(
            ldb_dir,
            sorted(files, key=lambda f: f.path),  # type: ignore[no-any-return]
            [],
            read_any_cloud_location,
            strict_format,
        )
        self.dir_path_to_files = dir_path_to_files

    def infer_annotations(self) -> Dict[OpenFile, JSONObject]:
        annotations: Dict[OpenFile, JSONObject] = {}
        for dir_path, file_seq in self.dir_path_to_files.items():
            len_dir_path = len(dir_path)
            for file in file_seq:
                raw_label = (
                    file.fs._parent(  # pylint: disable=protected-access
                        file.path[len_dir_path:],
                    )
                )  # pylint: disable=protected-access)
                label_parts = raw_label.lstrip("/").split("/")
                label = label_parts[-1]
                for p in label_parts[-2::-1]:
                    label = {p: label}
                annotations[file] = {"label": label}
        return annotations

    def _index(self) -> None:
        annotations = self.infer_annotations()
        (
            files,
            indexed_ephemeral_bools,
            _,
        ) = self.process_files()

        annotations_by_data_object_path = {
            (self.old_to_new_files.get(f) or f).path: annot
            for f, annot in annotations.items()
        }
        self.index_inferred_files(
            files,
            indexed_ephemeral_bools,
            annotations_by_data_object_path,
        )

    def index_inferred_files(
        self,
        data_object_files: List[OpenFile],
        indexed_ephemeral_bools: Iterable[bool],
        annotations_by_data_object_path: Dict[str, JSONObject],
    ) -> None:
        for data_object_file, is_indexed_ephemeral in zip(
            data_object_files,
            indexed_ephemeral_bools,
        ):
            item = InferredIndexingItem(
                self.ldb_dir,
                current_time(),
                data_object_file,
                not is_indexed_ephemeral,
                self.hashes,
                annotations_by_data_object_path[data_object_file.path],
            )
            self.result.append(item.index_data())


@dataclass
class IndexingItem(ABC):
    ldb_dir: Path
    curr_time: datetime
    _to_write: List[DataToWrite] = field(
        init=False,
        default_factory=list,
    )

    @cached_property
    def current_timestamp(self) -> str:
        return format_datetime(self.curr_time)

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
    def annotation_meta(self) -> AnnotationMeta:
        raise NotImplementedError

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

    @cached_property
    def has_annotation(self) -> bool:
        raise NotImplementedError

    def index_data(self) -> IndexedObjectResult:
        new_data_object = not self.data_object_dir.is_dir()
        self.enqueue_data(self.data_object_to_write())

        found_annotation = self.has_annotation
        new_annotation = False
        if found_annotation:
            new_annotation = not self.annotation_meta_file_path.is_file()
            self.enqueue_data(self.annotation_to_write())

        self.write_data()
        return IndexedObjectResult(
            found_data_object=True,
            found_annotation=found_annotation,
            new_data_object=new_data_object,
            new_annotation=new_annotation,
            data_object_hash=self.data_object_hash,
        )

    def data_object_to_write(self) -> List[DataToWrite]:
        return [
            (
                self.data_object_meta_file_path,
                json_dumps(self.data_object_meta).encode(),
                True,
            ),
        ]

    def annotation_to_write(self) -> List[DataToWrite]:
        annotation_meta_bytes = json_dumps(self.annotation_meta).encode()
        to_write = [
            (self.annotation_meta_file_path, annotation_meta_bytes, True),
        ]
        if not self.annotation_dir.is_dir():
            to_write.append(
                (
                    self.annotation_dir / "ldb",
                    self.annotation_ldb_content_bytes,
                    False,
                ),
            )
            to_write.append(
                (
                    self.annotation_dir / "user",
                    self.annotation_content_bytes,
                    False,
                ),
            )
        to_write.append(
            (
                self.data_object_dir / "current",
                self.annotation_hash.encode(),
                True,
            ),
        )
        return to_write

    def enqueue_data(self, data: Iterable[DataToWrite]) -> None:
        self._to_write.extend(data)

    def write_data(self) -> None:
        for file_path, data, overwrite_existing in self._to_write:
            write_data_file(file_path, data, overwrite_existing)


@dataclass
class AnnotationFileIndexingItem(IndexingItem):
    annotation_file: OpenFile

    @cached_property
    def annotation_meta(self) -> AnnotationMeta:
        fs_info = self.annotation_file.fs.info(self.annotation_file.path)
        curr_mtime_float = fs_info.get("created")
        if curr_mtime_float is not None:
            curr_mtime: Optional[datetime] = timestamp_to_datetime(
                curr_mtime_float,
            )
        else:
            curr_mtime = None
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

    @cached_property
    def has_annotation(self) -> bool:
        return self.annotation_file is not None


@dataclass
class InferredIndexingItem(DataObjectFileIndexingItem):
    _annotation_content: JSONDecoded

    @cached_property
    def annotation_meta(self) -> AnnotationMeta:
        prev_annotation = (
            load_data_file(self.annotation_meta_file_path)
            if self.annotation_meta_file_path.exists()
            else {}
        )
        return construct_annotation_meta(
            prev_annotation,
            self.current_timestamp,
            self.annotation_version,
            self.curr_time,
        )

    @cached_property
    def annotation_content(self) -> JSONDecoded:
        return self._annotation_content

    @cached_property
    def has_annotation(self) -> bool:
        return True


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
    item = AnnotationOnlyIndexingItem(
        ldb_dir,
        current_time(),
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
    hashes: Mapping[str, str],
    strict_format: bool,
) -> Tuple[Dict[OpenFile, OpenFile], Dict[OpenFile, OpenFile]]:
    fs = fsspec.filesystem(read_add_location.protocol)
    base_dir = fs.sep.join(
        [
            read_add_location.path,
            "ldb-autoimport",
            date.today().isoformat(),
            unique_id(),
        ],
    )
    # annotation_files_by_path = annotation_files_by_path.copy()
    fs.makedirs(base_dir, exist_ok=True)
    old_to_new_files = {}
    old_to_new_annot_files = {}
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
        elif strict_format:
            continue
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
            # Use hash instead of preserving path if path is too long
            data_object_hash = hashes.get(file.path) or hash_file(file)
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
        old_to_new_files[file] = OpenFile(fs, dest)
        if annotation_dest is not None:
            old_to_new_annot_files[annotation_file] = OpenFile(
                fs,
                annotation_dest,
            )
    return old_to_new_files, old_to_new_annot_files


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
