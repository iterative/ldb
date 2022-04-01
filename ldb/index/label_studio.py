from pathlib import Path
from typing import List, Sequence, Tuple

from fsspec.utils import get_protocol
from funcy.objects import cached_property

from ldb.exceptions import IndexingException
from ldb.index.base import PairIndexer, Preprocessor
from ldb.index.inferred import InferredIndexingItem
from ldb.index.utils import (
    FileSystemPath,
    FSPathsMapping,
    IndexingJobMapping,
    get_annotation_content,
)
from ldb.storage import StorageLocation, get_filesystem
from ldb.typing import JSONObject
from ldb.utils import current_time, get_file_hash


def set_data_object_id(annot: JSONObject, hash_str: str) -> None:
    try:
        annot["data_object_id"]["md5"] = hash_str
    except KeyError:
        annot["data_object_id"] = {"md5": hash_str}


class LabelStudioPreprocessor(Preprocessor):
    def __init__(
        self,
        paths: Sequence[str],
        storage_locations: Sequence[StorageLocation],
        url_key: str = "image",
    ) -> None:
        super().__init__(paths, storage_locations)
        self.url_key = url_key

    @cached_property
    def data_object_path_and_annotation_pairs(
        self,
    ) -> List[Tuple[FileSystemPath, JSONObject]]:
        result = []
        for fs, paths in self.annotation_paths.items():
            for path in paths:
                annotations = get_annotation_content(fs, path)
                if not isinstance(annotations, list):
                    raise IndexingException(
                        "Annotation file must contain a JSON array for "
                        "label-studio format. Incorrectly formatted file: "
                        f"{path}",
                    )
                for annot in annotations:
                    path = annot["data"][self.url_key]
                    protocol = get_protocol(path)
                    fs = get_filesystem(path, protocol, self.storage_locations)
                    set_data_object_id(annot["data"], get_file_hash(fs, path))
                    result.append((FileSystemPath(fs, path), annot))
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
    ) -> None:
        self.preprocessor: LabelStudioPreprocessor
        super().__init__(
            ldb_dir,
            preprocessor,
            read_any_cloud_location,
            strict_format,
        )

    def _index(self) -> None:
        indexing_jobs, _ = self.process_files()
        self.index_label_studio_files(
            indexing_jobs,
            self.preprocessor.annotations,
        )

    def index_label_studio_files(
        self,
        indexing_jobs: IndexingJobMapping,
        annotations: List[JSONObject],
    ) -> None:
        annot_iter = iter(annotations)

        for fs, jobs in indexing_jobs.items():
            for config, path_seq in jobs:
                for data_object_path in path_seq:
                    obj_result = InferredIndexingItem(
                        self.ldb_dir,
                        current_time(),
                        FileSystemPath(fs, data_object_path),
                        config.save_data_object_path_info,
                        self.hashes,
                        next(annot_iter),
                    ).index_data()
                    self.result.append(obj_result)
