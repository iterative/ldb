from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from typing import Dict, Iterable, List, Tuple, Union

from fsspec.core import OpenFile
from fsspec.spec import AbstractFileSystem
from funcy.objects import cached_property

from ldb.data_formats import Format
from ldb.exceptions import IndexingException
from ldb.index.base import (
    AnnotationMeta,
    DataObjectFileIndexingItem,
    PairIndexer,
    Preprocessor,
)
from ldb.index.utils import construct_annotation_meta, expand_dir_paths
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
                file_paths = fs.find(p.rstrip("/"))
                if file_paths:
                    fs_seq[p] = file_paths
        return file_seqs

    def get_storage_files(self) -> Dict[AbstractFileSystem, List[str]]:
        return {
            fs: list(chain(*seqs.values()))
            for fs, seqs in self.dir_path_to_files.items()
        }

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

    def infer_annotations(
        self,
    ) -> Dict[AbstractFileSystem, Dict[str, JSONObject]]:
        annotations: Dict[AbstractFileSystem, Dict[str, JSONObject]] = {}
        for fs, seqs in self.preprocessor.dir_path_to_files.items():
            fs_annots = annotations.setdefault(fs, {})
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
        (
            files,
            indexed_ephemeral_bools,
            _,
        ) = self.process_files()

        old_to_new_files = {}
        for old, new in self.old_to_new_files.items():
            old_to_new_files[old.fs, old.path] = new

        annotations_by_data_object_path = {
            (old_to_new_files.get((fs, f)) or OpenFile(fs, f)).path: annot
            for fs, fs_annotations in annotations.items()
            for f, annot in fs_annotations.items()
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
