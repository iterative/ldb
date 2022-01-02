import os
from abc import ABC
from dataclasses import dataclass, field
from datetime import datetime
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
    Tuple,
    Union,
)

import fsspec
from fsspec.core import OpenFile
from funcy.objects import cached_property

from ldb.data_formats import INDEX_FORMATS, Format
from ldb.dataset import get_collection_dir_keys
from ldb.exceptions import IndexingException, LDBException
from ldb.index.utils import (
    construct_annotation_meta,
    construct_data_object_meta,
    copy_to_read_add_storage,
    data_object_path_to_annotation_path,
    expand_dir_paths,
    get_annotation_content,
    get_storage_files_for_paths,
    group_storage_files_by_type,
    separate_indexed_files,
    separate_local_and_cloud_files,
    separate_storage_and_non_storage_files,
    validate_locations_in_storage,
)
from ldb.path import InstanceDir
from ldb.storage import get_storage_locations
from ldb.typing import JSONDecoded, JSONObject
from ldb.utils import (
    current_time,
    format_datetime,
    get_hash_path,
    hash_data,
    hash_file,
    json_dumps,
    load_data_file,
    timestamp_to_datetime,
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


class Preprocessor:
    def __init__(self, fmt: str, paths: Sequence[str]) -> None:
        self.fmt = INDEX_FORMATS[fmt]
        self.paths = [os.path.abspath(p) for p in paths]

    def get_storage_files(self) -> List[OpenFile]:
        return get_storage_files_for_paths(
            self.paths,
            default_format=self.fmt in (Format.STRICT, Format.BARE),
        )

    @cached_property
    def files_by_type(self) -> Tuple[List[OpenFile], List[OpenFile]]:
        files = self.get_storage_files()
        # if not files:
        #    raise LDBException(
        #        "No files found matching the given paths.",
        #    )
        return group_storage_files_by_type(files)

    @cached_property
    def data_object_files(self) -> List[OpenFile]:
        return self.files_by_type[0]

    @cached_property
    def annotation_files(self) -> List[OpenFile]:
        return self.files_by_type[1]


class InferredPreprocessor(Preprocessor):
    @cached_property
    def dir_path_to_files(self) -> Dict[str, List[OpenFile]]:
        dir_paths = expand_dir_paths(self.paths)
        file_seqs = [
            fsspec.open_files(p.rstrip("/") + "/**") for p in dir_paths
        ]
        return {d: f for d, f in zip(dir_paths, file_seqs) if f}

    def get_storage_files(self) -> List[OpenFile]:
        return list(chain(*self.dir_path_to_files.values()))

    @cached_property
    def files_by_type(self) -> Tuple[List[OpenFile], List[OpenFile]]:
        files, annotation_files = super().files_by_type
        if annotation_files:
            first_path = annotation_files[0].path
            raise IndexingException(
                f"No annotation files should be present for {Format.INFER} "
                "format.\n"
                f"Found {len(annotation_files)} JSON files.\n"
                f"First path: {first_path}",
            )
        return files, annotation_files


class Indexer:
    def __init__(
        self,
        ldb_dir: Path,
        preprocessor: Preprocessor,
    ) -> None:
        self.ldb_dir = ldb_dir
        self.preprocessor = preprocessor
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
        preprocessor: Preprocessor,
        read_any_cloud_location: bool,
        strict_format: bool,
    ) -> None:
        super().__init__(ldb_dir, preprocessor)
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
        files, annotation_files = self.preprocessor.files_by_type
        local_files, cloud_files = separate_local_and_cloud_files(files)
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
        preprocessor: InferredPreprocessor,
        read_any_cloud_location: bool,
        strict_format: bool,
    ) -> None:
        self.preprocessor: InferredPreprocessor
        super().__init__(
            ldb_dir,
            preprocessor,
            read_any_cloud_location,
            strict_format,
        )

    def infer_annotations(self) -> Dict[OpenFile, JSONObject]:
        annotations: Dict[OpenFile, JSONObject] = {}
        for dir_path, file_seq in self.preprocessor.dir_path_to_files.items():
            len_dir_path = len(dir_path)
            for file in file_seq:
                raw_label = (
                    file.path[len_dir_path:].rsplit("/", 1)[0].strip("/")
                )
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
