from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from typing import Collection, Dict, List, Tuple, Union

from fsspec.spec import AbstractFileSystem
from funcy.objects import cached_property

from ldb.data_formats import Format
from ldb.exceptions import IndexingException
from ldb.index.base import (
    DataObjectFileIndexingItem,
    PairIndexer,
    Preprocessor,
)
from ldb.index.utils import (
    AnnotationMeta,
    FileSystemPath,
    FSPathsMapping,
    IndexingJobMapping,
    construct_annotation_meta,
    expand_dir_paths,
    is_hidden_fsspec_path,
)
from ldb.typing import JSONDecoded, JSONObject
from ldb.utils import current_time, load_data_file


class InferredPreprocessor(Preprocessor):
    @cached_property
    def dir_path_to_files(
        self,
    ) -> Dict[AbstractFileSystem, Dict[str, List[str]]]:
        dir_path_lists = expand_dir_paths(self.paths)
        file_seqs: Dict[AbstractFileSystem, Dict[str, List[str]]] = {}
        for fs, dir_paths in dir_path_lists.items():
            fs_seq: Dict[str, List[str]] = file_seqs.setdefault(fs, {})
            for p in dir_paths:
                file_paths: List[str] = [
                    p
                    for p in fs.find(p.rstrip("/"))
                    if not is_hidden_fsspec_path(p)
                ]
                if file_paths:
                    fs_seq[p] = file_paths
        return file_seqs

    def get_storage_files(self) -> Dict[AbstractFileSystem, List[str]]:
        return {
            fs: list(chain(*seqs.values()))
            for fs, seqs in self.dir_path_to_files.items()
        }

    @cached_property
    def files_by_type(self) -> Tuple[FSPathsMapping, FSPathsMapping]:
        files, annotation_files = super().files_by_type
        for fs, path_seq in annotation_files.items():
            if path_seq:
                first_path = path_seq[0]
                raise IndexingException(
                    "No annotation files should be present for "
                    f"{Format.INFER} format.\n"
                    f"Found {len(annotation_files)} JSON files.\n"
                    f"First path: {first_path} (protocol={fs.protocol})",
                )
        return files, annotation_files

    @cached_property
    def data_object_paths(self) -> FSPathsMapping:
        return {
            fs: [
                path for dir_paths in path_maps.values() for path in dir_paths
            ]
            for fs, path_maps in self.dir_path_to_files.items()
        }


class InferredIndexer(PairIndexer):
    def __init__(
        self,
        ldb_dir: Path,
        preprocessor: InferredPreprocessor,
        read_any_cloud_location: bool,
        strict_format: bool,
        tags: Collection[str] = (),
    ) -> None:
        self.preprocessor: InferredPreprocessor
        super().__init__(
            ldb_dir,
            preprocessor,
            read_any_cloud_location,
            strict_format,
            tags,
        )

    def infer_annotations(
        self,
    ) -> Dict[AbstractFileSystem, Dict[str, JSONObject]]:
        annotations: Dict[AbstractFileSystem, Dict[str, JSONObject]] = {}
        for fs, seqs in self.preprocessor.dir_path_to_files.items():
            fs_annots: Dict[str, JSONObject] = annotations.setdefault(fs, {})
            for dir_path, file_seq in seqs.items():
                len_dir_path = len(dir_path)
                for file in file_seq:
                    raw_label = (
                        file[len_dir_path:].rsplit("/", 1)[0].strip("/")
                    )
                    label_parts = raw_label.lstrip("/").split("/")
                    label: Union[str, JSONObject] = label_parts[-1]
                    for p in label_parts[-2::-1]:
                        label = {p: label}
                    fs_annots[file] = {"label": label}
        return annotations

    def _index(self) -> None:
        annotations = self.infer_annotations()
        indexing_jobs, _ = self.process_files()

        annotations_by_data_object_path: Dict[
            Tuple[AbstractFileSystem, str],
            JSONObject,
        ] = {}

        for fs, fs_annotations in annotations.items():
            fs_old_to_new = self.old_to_new_files.get(fs, {})
            for f, annot in fs_annotations.items():
                path = fs_old_to_new.get(f)
                if path is None:
                    path = FileSystemPath(fs, f)
                annotations_by_data_object_path[path] = annot
        self.index_inferred_files(
            indexing_jobs,
            annotations_by_data_object_path,
            self.tags,
        )

    def index_inferred_files(
        self,
        indexing_jobs: IndexingJobMapping,
        annotations_by_data_object_path: Dict[
            Tuple[AbstractFileSystem, str],
            JSONObject,
        ],
        tags: Collection[str],
    ) -> None:
        for fs, jobs in indexing_jobs.items():
            for config, path_seq in jobs:
                for data_object_path in path_seq:
                    item = InferredIndexingItem(
                        self.ldb_dir,
                        current_time(),
                        tags,
                        FileSystemPath(fs, data_object_path),
                        config.save_data_object_path_info,
                        self.hashes,
                        annotations_by_data_object_path[fs, data_object_path],
                    )
                    self.result.append(item.index_data())


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
