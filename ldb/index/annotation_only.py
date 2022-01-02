from dataclasses import dataclass

from funcy.objects import cached_property

from ldb.exceptions import DataObjectNotFoundError, IndexingException
from ldb.index.base import (
    AnnotationFileIndexingItem,
    DataObjectMeta,
    IndexedObjectResult,
    Indexer,
)
from ldb.index.utils import get_annotation_content
from ldb.typing import JSONObject
from ldb.utils import current_time, load_data_file


class AnnotationOnlyIndexer(Indexer):
    def _index(self) -> None:
        for annotation_file in self.preprocessor.annotation_files:
            item = AnnotationOnlyIndexingItem(
                self.ldb_dir,
                current_time(),
                annotation_file,
            )
            self.result.append(item.index_data())


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

    def index_data(self) -> IndexedObjectResult:
        if not self.data_object_dir.exists():
            raise DataObjectNotFoundError(
                f"Data object not found: 0x{self.data_object_hash} "
                f"(annotation_file_path={self.annotation_file.path!r})",
            )

        new_annotation = not self.annotation_meta_file_path.is_file()
        self.enqueue_data(self.annotation_to_write())

        self.write_data()
        return IndexedObjectResult(
            found_data_object=False,
            found_annotation=True,
            new_data_object=False,
            new_annotation=new_annotation,
            data_object_hash=self.data_object_hash,
        )
