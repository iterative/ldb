import warnings
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from typing import Collection, Dict, List, Optional, Tuple

from fsspec.spec import AbstractFileSystem
from funcy.objects import cached_property

from ldb.data_formats import Format
from ldb.exceptions import IndexingException
from ldb.fs.utils import unstrip_protocol
from ldb.index.base import (
    DataObjectFileIndexingItem,
    PairIndexer,
    Preprocessor,
)
from ldb.index.utils import (
    AnnotationMeta,
    AnnotMergeStrategy,
    FileSystemPath,
    FSPathsMapping,
    IndexingJobMapping,
    construct_annotation_meta,
    expand_dir_paths,
    is_hidden_fsspec_path,
)
from ldb.jmespath.parser import parse_identifier_expression
from ldb.params import ParamConfig
from ldb.typing import JSONDecoded, JSONObject
from ldb.utils import current_time, load_data_file

BASE_DIR_HELP = (
    "To index files in base dirs, provide a label, with the "
    "tensorflow-inferred format's base-label parameter:\n\n"
    "\tldb index --format tensorflow-inferred --param base-label=<label> "
    "<path> [<path> ...]\n"
)


class InferredParamConfig(ParamConfig):
    PARAM_PROCESSORS = {
        "label-key": parse_identifier_expression,
        "base-label": None,
    }


class InferredPreprocessor(InferredParamConfig, Preprocessor):
    @cached_property
    def dir_path_to_files(
        self,
    ) -> Dict[AbstractFileSystem, Dict[str, List[str]]]:
        dir_path_lists = expand_dir_paths(self.paths, self.storage_locations)
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
                first_path = unstrip_protocol(fs, path_seq[0])
                num_annotation_files = sum(
                    len(m) for m in annotation_files.values()
                )
                raise IndexingException(
                    "No annotation files should be present for "
                    f"{Format.INFER} format.\n"
                    f"Found {num_annotation_files} JSON files.\n"
                    f"First path: {first_path}",
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
        annot_merge_strategy: AnnotMergeStrategy = AnnotMergeStrategy.REPLACE,
        ephemeral_remote: bool = False,
    ) -> None:
        self.preprocessor: InferredPreprocessor
        super().__init__(
            ldb_dir,
            preprocessor,
            read_any_cloud_location,
            strict_format,
            tags,
            annot_merge_strategy,
            ephemeral_remote,
        )

    def infer_annotations(
        self,
    ) -> Dict[AbstractFileSystem, Dict[str, Optional[JSONObject]]]:
        label_key = self.preprocessor.params.get("label-key", ["label"])
        base_dir_label: Optional[str] = self.preprocessor.params.get(
            "base-label",
        )

        if not label_key:
            raise ValueError("label-key param cannot be empty")
        annotations: Dict[
            AbstractFileSystem,
            Dict[str, Optional[JSONObject]],
        ] = {}

        warning_count = 0
        for fs, seqs in self.preprocessor.dir_path_to_files.items():
            fs_annots: Dict[
                str,
                Optional[JSONObject],
            ] = annotations.setdefault(fs, {})
            for dir_path, file_seq in seqs.items():
                len_dir_path = len(dir_path)
                for file in file_seq:
                    raw_label = (
                        file[len_dir_path:].rsplit("/", 1)[0].strip("/")
                    )
                    label_parts = list(
                        filter(None, raw_label.lstrip("/").split("/")),
                    )
                    if not label_parts and base_dir_label is not None:
                        label_parts = [base_dir_label]

                    if not label_parts:
                        if not warning_count:
                            warnings.warn(
                                "Skipping file(s) found in base dir.\n"
                                f"{BASE_DIR_HELP}",
                                RuntimeWarning,
                                stacklevel=2,
                            )
                        warning_count += 1
                        fs_annots[file] = None
                    else:
                        label_parts = label_key + label_parts
                        label: JSONObject = {label_parts[-2]: label_parts[-1]}
                        for p in label_parts[-3::-1]:
                            label = {p: label}
                        fs_annots[file] = label
        return annotations

    def _index(self) -> None:
        annotations = self.infer_annotations()
        indexing_jobs, _ = self.process_files()

        annotations_by_data_object_path: Dict[
            Tuple[AbstractFileSystem, str],
            Optional[JSONObject],
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
            self.annot_merge_strategy,
        )

    def index_inferred_files(
        self,
        indexing_jobs: IndexingJobMapping,
        annotations_by_data_object_path: Dict[
            Tuple[AbstractFileSystem, str],
            Optional[JSONObject],
        ],
        tags: Collection[str],
        annot_merge_strategy: AnnotMergeStrategy = AnnotMergeStrategy.REPLACE,
    ) -> None:
        for fs, jobs in indexing_jobs.items():
            for config, path_seq in jobs:
                for data_object_path in path_seq:
                    annot = annotations_by_data_object_path[
                        fs,
                        data_object_path,
                    ]
                    if annot is not None:
                        item = InferredIndexingItem(
                            self.ldb_dir,
                            current_time(),
                            tags,
                            annot_merge_strategy,
                            FileSystemPath(fs, data_object_path),
                            config.save_data_object_path_info,
                            self.hashes,
                            annot,
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
            None,
        )

    @cached_property
    def raw_annotation_content(self) -> JSONDecoded:
        return self._annotation_content

    @cached_property
    def has_annotation(self) -> bool:
        return True
