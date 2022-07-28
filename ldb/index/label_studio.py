import json
from pathlib import Path
from typing import Collection, List, Mapping, Sequence, Set, Tuple

import fsspec
import jmespath
from fsspec.utils import get_protocol
from funcy.objects import cached_property

from ldb.exceptions import IndexingException
from ldb.fs.utils import get_file_hash
from ldb.index.base import PairIndexer, Preprocessor
from ldb.index.inferred import InferredIndexingItem
from ldb.index.utils import (
    AnnotMergeStrategy,
    FileSystemPath,
    FSPathsMapping,
    IndexingJobMapping,
    get_annotation_content,
)
from ldb.storage import StorageLocation, get_filesystem
from ldb.typing import JSONObject
from ldb.utils import current_time


class LabelStudioPreprocessor(Preprocessor):
    def __init__(
        self,
        paths: Sequence[str],
        storage_locations: Sequence[StorageLocation],
        params: Mapping[str, str],
        fmt: str,
        path_key: str = "",
    ) -> None:
        super().__init__(paths, storage_locations, params, fmt)
        self.path_key = path_key

    @cached_property
    def data_object_path_and_annotation_pairs(
        self,
    ) -> List[Tuple[FileSystemPath, JSONObject]]:
        result = []
        for annot_fs, annot_paths in self.annotation_paths.items():
            for annot_path in annot_paths:
                orig_annot = get_annotation_content(annot_fs, annot_path)
                annotations: List[JSONObject]
                if isinstance(orig_annot, list):
                    annotations = orig_annot
                elif isinstance(orig_annot, dict):
                    annotations = [orig_annot]
                else:
                    raise IndexingException(
                        "Annotation file must contain a JSON array or JSON "
                        "object for label-studio format. Incorrectly "
                        f"formatted file: {annot_path}",
                    )
                for annot in annotations:
                    data = annot["data"]
                    data_object_info = data.setdefault("data-object-info", {})

                    path_key = data.get("path_key")
                    if path_key is None:
                        if self.path_key:
                            path_key = self.path_key
                        else:
                            path_key = infer_data_object_path_key(annot)
                            self.path_key = path_key
                            print(f"Inferred data object path key: {path_key}")
                        data_object_info["path_key"] = path_key
                    try:
                        orig_path = jmespath.search(path_key, annot)
                        assert isinstance(orig_path, str)
                    except Exception as exc:
                        raise IndexingException(
                            f"Invalid data object path key: {path_key}",
                        ) from exc

                    protocol = get_protocol(orig_path)
                    fs_cls = fsspec.get_filesystem_class(protocol)
                    path: str = fs_cls._strip_protocol(  # pylint: disable=protected-access # noqa: E501
                        orig_path,
                    )
                    obj_fs = get_filesystem(
                        path,
                        protocol,
                        self.storage_locations,
                    )
                    if "md5" not in data_object_info:
                        data_object_info["md5"] = get_file_hash(obj_fs, path)
                    result.append((FileSystemPath(obj_fs, path), annot))
        return result

    @cached_property
    def annotations(self) -> List[JSONObject]:
        return [a for _, a in self.data_object_path_and_annotation_pairs]

    @cached_property
    def data_object_paths(self) -> FSPathsMapping:
        paths: FSPathsMapping = {}
        for (fs, path), _ in self.data_object_path_and_annotation_pairs:
            paths.setdefault(fs, []).append(path)
        return paths


class LabelStudioIndexer(PairIndexer):
    def __init__(
        self,
        ldb_dir: Path,
        preprocessor: LabelStudioPreprocessor,
        read_any_cloud_location: bool,
        strict_format: bool,
        tags: Collection[str] = (),
        annot_merge_strategy: AnnotMergeStrategy = AnnotMergeStrategy.REPLACE,
        ephemeral_remote: bool = False,
    ) -> None:
        self.preprocessor: LabelStudioPreprocessor
        if annot_merge_strategy == AnnotMergeStrategy.MERGE:
            raise ValueError(
                "Invalid annotation merge strategy for LabelStudioIndexer: "
                f"{annot_merge_strategy}",
            )
        super().__init__(
            ldb_dir,
            preprocessor,
            read_any_cloud_location,
            strict_format,
            tags,
            annot_merge_strategy,
            ephemeral_remote,
        )

    def _index(self) -> None:
        indexing_jobs, _ = self.process_files()
        self.index_label_studio_files(
            indexing_jobs,
            self.preprocessor.annotations,
            self.tags,
        )

    def index_label_studio_files(
        self,
        indexing_jobs: IndexingJobMapping,
        annotations: List[JSONObject],
        tags: Collection[str],
    ) -> None:
        annot_iter = iter(annotations)

        for fs, jobs in indexing_jobs.items():
            for config, path_seq in jobs:
                for data_object_path in path_seq:
                    obj_result = InferredIndexingItem(
                        self.ldb_dir,
                        current_time(),
                        tags,
                        AnnotMergeStrategy.REPLACE,
                        FileSystemPath(fs, data_object_path),
                        config.save_data_object_path_info,
                        self.hashes,
                        next(annot_iter),
                    ).index_data()
                    self.result.append(obj_result)


def infer_data_object_path_key(annot: JSONObject) -> str:
    try:
        data = annot["data"]
    except KeyError as exc:
        raise IndexingException(
            "Cannot infer data object path key. "
            'Missing top-level "data" key',
        ) from exc
    data_keys: Set[str] = data.keys() - {"data-object-info"}
    if len(data_keys) != 1:
        raise IndexingException(
            "Cannot infer data object path key. "
            "Found multiple possible keys under top-level "
            f'"data" key: {data_keys}',
        )
    subkey = json.dumps(data_keys.pop())
    return f'"data".{subkey}'
