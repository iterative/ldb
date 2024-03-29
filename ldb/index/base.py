from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Collection,
    Dict,
    Generator,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
)

from fsspec.spec import AbstractFileSystem
from funcy.objects import cached_property
from rich.progress import Progress

from ldb.core import LDBClient
from ldb.dataset import get_annotation, get_root_collection_annotation_hash
from ldb.db.abstract import AbstractDB
from ldb.exceptions import (
    DataObjectNotFoundError,
    IndexingException,
    LDBException,
)
from ldb.fs.utils import get_file_hash, get_modified_time
from ldb.index.utils import (
    DEFAULT_CONFIG,
    INDEXED_EPHEMERAL_CONFIG,
    AnnotationMeta,
    AnnotMergeStrategy,
)
from ldb.index.utils import DataObjectMeta as DataObjectMetaT
from ldb.index.utils import (
    FileSystemPath,
    FSPathsMapping,
    IndexingJobMapping,
    construct_annotation_meta,
    construct_data_object_meta,
    copy_to_read_add_storage,
    data_object_path_to_annotation_path,
    expand_indexing_paths,
    get_annotation_content,
    group_indexing_paths_by_type,
    separate_indexed_files,
    separate_local_and_cloud_files,
    separate_storage_and_non_storage_files,
    validate_locations_in_storage,
)
from ldb.objects.annotation import Annotation
from ldb.params import ParamConfig, ParamFunc
from ldb.path import InstanceDir
from ldb.progress import get_progressbar
from ldb.storage import StorageLocation
from ldb.typing import JSONDecoded, JSONObject
from ldb.utils import (
    DATA_OBJ_ID_PREFIX,
    current_time,
    format_datetime,
    get_hash_path,
    json_dumps,
)

if TYPE_CHECKING:
    from ldb.add import AddResult

ENDING_DOUBLE_STAR_RE = r"(?:/+\*\*)+/*$"


class IndexedObjectResult(NamedTuple):
    found_data_object: bool
    found_annotation: bool
    new_data_object: bool
    new_annotation: bool
    new_data_object_path: bool
    data_object_hash: str
    annotation_hash: str
    transform_hashes: Optional[Sequence[str]]


@dataclass
class IndexingResult:
    num_found_data_objects: int = 0
    num_found_annotations: int = 0
    num_new_data_objects: int = 0
    num_new_annotations: int = 0
    num_new_data_object_paths: int = 0
    collection: Dict[str, str] = field(
        default_factory=dict,
    )
    transforms: Dict[str, Sequence[str]] = field(default_factory=dict)
    add_result: Optional["AddResult"] = None

    def append(self, item: IndexedObjectResult) -> None:
        self.num_found_data_objects += item.found_data_object
        self.num_found_annotations += item.found_annotation
        self.num_new_data_objects += item.new_data_object
        self.num_new_annotations += item.new_annotation
        self.num_new_data_object_paths += item.new_data_object_path
        self.collection[item.data_object_hash] = item.annotation_hash
        if item.transform_hashes is not None:
            self.transforms[item.data_object_hash] = item.transform_hashes

    def summary(self, finished: bool = True) -> str:
        if finished:
            heading = "Finished indexing"
        else:
            heading = "Unable to finish indexing. Partial indexing results"

        if self.add_result is None:
            ending = ""
        else:
            ending = f"\n\n{self.add_result.summary()}"
        return (
            f"\n{heading}:\n"
            f"  Found data objects:    {self.num_found_data_objects:9d}\n"
            f"  Found annotations:     {self.num_found_annotations:9d}\n"
            f"  New data objects:      {self.num_new_data_objects:9d}\n"
            f"  New annotations:       {self.num_new_annotations:9d}\n"
            f"  New data object paths: {self.num_new_data_object_paths:9d}\n"
            f"  Transform sets:        {len(self.transforms):9d}"
            f"{ending}"
        )


class Preprocessor(ParamConfig):
    def __init__(
        self,
        paths: Sequence[str],
        storage_locations: Sequence[StorageLocation],
        params: Mapping[str, str],
        fmt: str,
        param_processors: Optional[Mapping[str, Optional[ParamFunc]]] = None,
    ) -> None:
        super().__init__(param_processors=param_processors)
        self.paths: List[str] = list(paths)
        self.storage_locations = storage_locations
        self.params = self.process_params(params, fmt)

    def get_storage_files(self) -> FSPathsMapping:
        print("Collecting paths...")
        return expand_indexing_paths(
            self.paths,
            self.storage_locations,
            default_format=True,
        )

    @cached_property
    def files_by_type(self) -> Tuple[FSPathsMapping, FSPathsMapping]:
        data_obj_files = {}
        annot_files = {}
        for fs, fs_paths in self.get_storage_files().items():
            data_obj_files[fs], annot_files[fs] = group_indexing_paths_by_type(
                fs,
                fs_paths,
            )
        return data_obj_files, annot_files

    @cached_property
    def data_object_paths(self) -> FSPathsMapping:
        return self.files_by_type[0]

    @cached_property
    def annotation_paths(self) -> FSPathsMapping:
        return self.files_by_type[1]


class Indexer(ABC):
    def __init__(
        self,
        ldb_dir: Path,
        preprocessor: Preprocessor,
        tags: Collection[str] = (),
        annot_merge_strategy: AnnotMergeStrategy = AnnotMergeStrategy.REPLACE,
        ephemeral_remote: bool = False,
    ) -> None:
        self.ldb_dir = ldb_dir
        self.preprocessor = preprocessor
        self.tags = tags
        self.annot_merge_strategy = annot_merge_strategy
        self.ephemeral_remote = ephemeral_remote
        self.result = IndexingResult()
        self.hashes: Dict[AbstractFileSystem, Dict[str, str]] = {}
        self.disable_progress_bar = False
        self.client = LDBClient(ldb_dir)

    @property
    def db(self) -> AbstractDB:
        return self.client.db

    def index(self) -> None:
        try:
            self._index()
        except Exception:
            self.db.write_all()
            print(self.result.summary(finished=False), "\n", sep="")
            raise
        self.db.write_all()

    @abstractmethod
    def _index(self) -> None:
        raise NotImplementedError

    def process_files(self) -> Any:
        raise NotImplementedError

    @contextmanager
    def progress_bar(self) -> Generator[Progress, None, None]:
        with get_progressbar(
            transient=True,
            disable=self.disable_progress_bar,
        ) as progress:
            yield progress


class PairIndexer(Indexer):
    def __init__(
        self,
        ldb_dir: Path,
        preprocessor: Preprocessor,
        read_any_cloud_location: bool,
        strict_format: bool,
        tags: Collection[str] = (),
        annot_merge_strategy: AnnotMergeStrategy = AnnotMergeStrategy.REPLACE,
        ephemeral_remote: bool = False,
    ) -> None:
        super().__init__(
            ldb_dir,
            preprocessor,
            tags,
            annot_merge_strategy,
            ephemeral_remote,
        )
        self.read_any_cloud_location = read_any_cloud_location
        self.strict_format = strict_format
        self.old_to_new_files: Dict[
            AbstractFileSystem,
            Dict[str, FileSystemPath],
        ] = {}
        self.old_to_new_annot_files: Dict[
            AbstractFileSystem,
            Dict[str, FileSystemPath],
        ] = {}

    def _index(self) -> None:
        indexing_jobs, annotation_paths = self.process_files()
        self.index_files(
            indexing_jobs,
            annotation_paths,
        )

    def process_files(
        self,
    ) -> Tuple[IndexingJobMapping, FSPathsMapping]:
        storage_locations = self.preprocessor.storage_locations
        if self.ephemeral_remote:
            possibly_ephemeral_files = self.preprocessor.data_object_paths
            storage_only_files: FSPathsMapping = {}
        else:
            possibly_ephemeral_files, storage_only_files = separate_local_and_cloud_files(
                self.preprocessor.data_object_paths,
            )
        if not self.read_any_cloud_location:
            validate_locations_in_storage(
                storage_only_files,
                storage_locations,
            )
        local_storage_files, ephemeral_files = separate_storage_and_non_storage_files(
            possibly_ephemeral_files,
            storage_locations,
        )

        for fs, paths in ephemeral_files.items():
            fs_hashes = self.hashes.setdefault(fs, {})
            for path in paths:
                fs_hashes[path] = get_file_hash(fs, path)

        existing_hashes = self.client.db.get_existing_data_object_ids(
            i for fs_values in self.hashes.values() for i in fs_values.values()
        )
        indexed_ephemeral_files, ephemeral_files = separate_indexed_files(
            existing_hashes,
            self.hashes,
            ephemeral_files,
        )
        files = {
            fs: storage_only_files.get(fs, []) + local_storage_files.get(fs, [])
            for fs in storage_only_files.keys() | local_storage_files.keys()
        }
        annotation_paths = {
            fs: paths.copy() for fs, paths in self.preprocessor.annotation_paths.items()
        }

        if ephemeral_files:
            read_add_location = next(
                (loc for loc in storage_locations if loc.read_and_add),
                None,
            )
            if read_add_location is None:
                raise LDBException(
                    "Local paths outside of configured storage locations "
                    "found, but no read-add storage configured.\n"
                    "Either add storage locations containing all indexing "
                    "paths or add a read-add storage location to handle "
                    "ephemeral data paths.\n\n"
                    "For more information, run:\n\n"
                    "\tldb add-storage --help\n",
                )
            self.old_to_new_files, self.old_to_new_annot_files = copy_to_read_add_storage(
                ephemeral_files,
                self.preprocessor.annotation_paths,
                read_add_location,
                self.hashes,
                strict_format=self.strict_format,
            )
            num_ephemeral_data_objects = sum(
                map(len, self.old_to_new_files.values()),
            )
            num_ephemeral_annotations = sum(
                map(len, self.old_to_new_annot_files.values()),
            )
            path = read_add_location.path
            if read_add_location.protocol != "file":
                path = f"{read_add_location.protocol}://{path}"
            print(
                f"\nCopied ephemeral files to storage: {path}\n"
                f"  Data objects:       {num_ephemeral_data_objects:9d}\n"
                f"  Annotations:        {num_ephemeral_annotations:9d}",
            )

            for _, fs_paths in self.old_to_new_annot_files.items():
                for _, new in fs_paths.items():
                    annotation_paths.setdefault(new.fs, []).append(new.path)
            for _, fs_paths in self.old_to_new_files.items():
                for _, new in fs_paths.items():
                    files.setdefault(new.fs, []).append(new.path)
        indexing_jobs: IndexingJobMapping = {}
        for config_type, fs_jobs in [
            (DEFAULT_CONFIG, files),
            (INDEXED_EPHEMERAL_CONFIG, indexed_ephemeral_files),
        ]:
            for fs, job in fs_jobs.items():
                indexing_jobs.setdefault(fs, []).append((config_type, job))
        return (
            indexing_jobs,
            annotation_paths,
        )

    def index_files(
        self,
        indexing_jobs: IndexingJobMapping,
        annotation_paths: FSPathsMapping,
    ) -> None:
        num_files = sum(
            len(path_seq) for jobs in indexing_jobs.values() for _, path_seq in jobs
        )
        print("Indexing data...")
        with self.progress_bar() as bar:
            task = bar.add_task("index_files", total=num_files)
            for fs, jobs in indexing_jobs.items():
                fs_annotation_paths = set(annotation_paths.get(fs, []))
                for config, path_seq in jobs:
                    for data_object_path in path_seq:
                        annotation_path = data_object_path_to_annotation_path(
                            data_object_path,
                        )
                        try:
                            fs_annotation_paths.remove(
                                annotation_path,
                            )
                        except KeyError:
                            annotation_path = ""
                        if not self.strict_format or annotation_path:
                            obj_result = self.index_single_pair(
                                fs,
                                data_object_path,
                                config.save_data_object_path_info,
                                annotation_path,
                                self.tags,
                                self.annot_merge_strategy,
                            )
                            self.result.append(obj_result)
                        bar.update(task, advance=1)

    def index_single_pair(
        self,
        fs: AbstractFileSystem,
        data_object_path: str,
        save_data_object_path_info: bool,
        annotation_path: str,
        tags: Collection[str],
        annot_merge_strategy: AnnotMergeStrategy,
    ) -> IndexedObjectResult:
        return PairIndexingItem(
            self.ldb_dir,
            self.client,
            current_time(),
            tags,
            annot_merge_strategy,
            FileSystemPath(fs, data_object_path),
            save_data_object_path_info,
            self.hashes,
            FileSystemPath(fs, annotation_path) if annotation_path else None,
        ).index_data()


@dataclass
class IndexingItem(ABC):
    ldb_dir: Path
    client: LDBClient
    curr_time: datetime
    tags: Collection[str]
    annot_merge_strategy: AnnotMergeStrategy
    found_new_data_object_path: bool = field(init=False, default=False)

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
    @abstractmethod
    def data_object_hash(self) -> str:
        raise NotImplementedError

    @cached_property
    def annotation_hash(self) -> str:
        return self.annotation.oid

    @cached_property
    @abstractmethod
    def data_object_meta(self) -> DataObjectMetaT:
        raise NotImplementedError

    @cached_property
    def annotation_ldb_content(
        self,
    ) -> JSONObject:
        return self.annotation.meta

    @cached_property
    def annotation_meta(self) -> AnnotationMeta:
        raise NotImplementedError

    @cached_property
    def raw_annotation_content(self) -> JSONDecoded:
        return None

    @cached_property
    def annotation(self) -> Annotation:
        if self.annot_merge_strategy == AnnotMergeStrategy.REPLACE:
            value = self.raw_annotation_content
        elif self.annot_merge_strategy == AnnotMergeStrategy.MERGE:
            value = self.get_merged_annotation_content()
        else:
            raise ValueError(f"Invalid annotation merge strategy: {self.annot_merge_strategy}")
        annot = Annotation(
            value,
            {
                "user_version": None,
                "schema_version": None,
            },
        )
        annot.digest()
        return annot

    @cached_property
    def annotation_content(self) -> JSONDecoded:
        return self.annotation.value

    def get_merged_annotation_content(self) -> JSONDecoded:
        if not isinstance(self.raw_annotation_content, Mapping):
            return self.raw_annotation_content
        # TODO use db
        last_annot_hash = get_root_collection_annotation_hash(
            get_hash_path(
                self.ldb_dir / InstanceDir.DATA_OBJECT_INFO,
                self.data_object_hash,
            ),
        )
        if last_annot_hash is None:
            return self.raw_annotation_content
        last_annot = get_annotation(self.ldb_dir, last_annot_hash)
        if not isinstance(last_annot, Mapping):
            return self.raw_annotation_content
        return {**last_annot, **self.raw_annotation_content}

    @cached_property
    def annotation_ldb_content_bytes(self) -> bytes:
        return json_dumps(self.annotation_ldb_content).encode()

    @cached_property
    def annotation_content_bytes(self) -> bytes:
        return json_dumps(self.annotation_content).encode()

    @cached_property
    def annotation_version(self) -> int:
        return self.client.db.count_pairs(self.data_object_hash) + 1

    @cached_property
    def has_annotation(self) -> bool:
        raise NotImplementedError

    def index_data(self) -> IndexedObjectResult:
        new_data_object = not self.data_object_dir.is_dir()

        found_annotation = self.has_annotation
        new_annotation = False
        if found_annotation:
            new_annotation = not self.annotation_meta_file_path.is_file()

        self.write_data()
        return IndexedObjectResult(
            found_data_object=True,
            found_annotation=found_annotation,
            new_data_object=new_data_object,
            new_annotation=new_annotation,
            new_data_object_path=self.found_new_data_object_path,
            data_object_hash=self.data_object_hash,
            annotation_hash=self.annotation_hash,
            transform_hashes=None,
        )

    def write_data(self) -> None:
        if self.has_annotation:
            annot = self.annotation
            annot_meta = self.annotation_meta
        else:
            annot = None
            annot_meta = None
        self.client.db.add_pair(
            self.data_object_hash,
            self.data_object_meta,
            annot,
            annot_meta,
        )


@dataclass
class AnnotationFileIndexingItem(IndexingItem):
    annotation_fsp: Optional[FileSystemPath]

    @cached_property
    def annotation_meta(self) -> AnnotationMeta:
        if self.annotation_fsp is None:
            raise IndexingException("Missing annotation_fsp")
        record = self.client.db.get_pair_meta(
            self.data_object_hash,
            self.annotation.oid,
        )
        prev = record[2] if record is not None else {}
        return construct_annotation_meta(
            prev,
            self.current_timestamp,
            self.annotation_version,
            get_modified_time(
                self.annotation_fsp.fs,
                self.annotation_fsp.path,
            ),
        )

    @cached_property
    def raw_annotation_content(self) -> JSONDecoded:
        if self.annotation_fsp is None:
            return None
        return get_annotation_content(
            *self.annotation_fsp,
        )


@dataclass
class DataObjectFileIndexingItem(IndexingItem):
    data_object: FileSystemPath
    save_data_object_path_info: bool
    data_object_hash_cache: Mapping[AbstractFileSystem, Mapping[str, str]]

    @cached_property
    def data_object_hash(self) -> str:
        hash_str = None
        fs_hashes = self.data_object_hash_cache.get(self.data_object.fs)
        if fs_hashes is not None:
            hash_str = fs_hashes.get(self.data_object.path)
        if hash_str is None:
            hash_str = get_file_hash(
                self.data_object.fs,
                self.data_object.path,
            )
        return hash_str

    @cached_property
    def data_object_meta(self) -> DataObjectMetaT:
        data_object_id = self.data_object_hash
        record = self.client.db.get_data_object_meta(data_object_id)
        if not self.save_data_object_path_info:
            if record is None:
                raise DataObjectNotFoundError(
                    f"Data object not found: {DATA_OBJ_ID_PREFIX}{data_object_id}"
                )
            meta_contents = record[1]
            meta_contents["last_indexed"] = self.current_timestamp
            meta_contents["tags"] = sorted(  # type: ignore[assignment]
                set(meta_contents["tags"]) | set(self.tags),  # type: ignore[arg-type]
            )
        else:
            if record is None:
                old_meta: DataObjectMetaT = {}
            else:
                old_meta = record[1]
            self.found_new_data_object_path, meta_contents = construct_data_object_meta(
                *self.data_object,
                old_meta,
                self.current_timestamp,
                tags=self.tags,
            )
        return meta_contents

    @cached_property
    def has_annotation(self) -> bool:
        return False


@dataclass
class PairIndexingItem(
    AnnotationFileIndexingItem,
    DataObjectFileIndexingItem,
):
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
        return self.annotation_fsp is not None
