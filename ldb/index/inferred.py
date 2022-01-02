from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import fsspec
from fsspec.core import OpenFile
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
