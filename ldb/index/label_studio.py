from pathlib import Path
from typing import Iterable, List, Sequence

import fsspec
from fsspec.core import OpenFile
from funcy.objects import cached_property

from ldb.exceptions import IndexingException
from ldb.index.base import PairIndexer, Preprocessor
from ldb.index.inferred import InferredIndexingItem
from ldb.index.utils import get_annotation_content
from ldb.typing import JSONObject
from ldb.utils import current_time


class LabelStudioPreprocessor(Preprocessor):
    def __init__(
        self,
        paths: Sequence[str],
        url_key: str = "image",
    ) -> None:
        super().__init__(paths)
        self.url_key = url_key

    @cached_property
    def annotations(self) -> List[JSONObject]:
        result = []
        for file in self.annotation_files:
            annotation = get_annotation_content(file)
            if not isinstance(annotation, list):
                raise IndexingException(
                    "Annotation file must contain a JSON array for "
                    "label-studio format. Incorrectly formatted file: "
                    f"{file.path}",
                )
            result.extend(annotation)
        return result

    @cached_property
    def data_object_files(self) -> List[OpenFile]:
        return list(
            fsspec.open_files(
                [a["data"][self.url_key] for a in self.annotations],
            ),
        )


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
        (
            files,
            indexed_ephemeral_bools,
            _,
        ) = self.process_files()
        self.index_label_studio_files(
            files,
            indexed_ephemeral_bools,
            self.preprocessor.annotations,
        )

    def index_label_studio_files(
        self,
        data_object_files: List[OpenFile],
        indexed_ephemeral_bools: Iterable[bool],
        annotations: List[JSONObject],
    ) -> None:
        for data_object_file, is_indexed_ephemeral, annotation in zip(
            data_object_files,
            indexed_ephemeral_bools,
            annotations,
        ):
            obj_result = InferredIndexingItem(
                self.ldb_dir,
                current_time(),
                data_object_file,
                not is_indexed_ephemeral,
                self.hashes,
                annotation,
            ).index_data()
            self.result.append(obj_result)
