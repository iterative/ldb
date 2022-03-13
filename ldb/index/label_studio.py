from pathlib import Path
from typing import List, Sequence

from fsspec.implementations.local import make_path_posix
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
from ldb.storage import StorageLocation
from ldb.typing import JSONObject
from ldb.utils import current_time


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
    def annotations(self) -> List[JSONObject]:
        result = []
        for fs, paths in self.annotation_paths.items():
            for path in paths:
                annotation = get_annotation_content(fs, path)
                if not isinstance(annotation, list):
                    raise IndexingException(
                        "Annotation file must contain a JSON array for "
                        "label-studio format. Incorrectly formatted file: "
                        f"{path}",
                    )
                result.extend(annotation)
        return result

    @cached_property
    def data_object_paths(self) -> FSPathsMapping:
        fs = next(iter(self.annotation_paths.keys()))
        return {
            fs: [
                make_path_posix(a["data"][self.url_key])
                for a in self.annotations
            ],
        }


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

        print("HERE")
        for fs, jobs in indexing_jobs.items():
            for config, path_seq in jobs:
                print(len(path_seq))
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
