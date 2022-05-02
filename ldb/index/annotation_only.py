from dataclasses import dataclass

from funcy.objects import cached_property

from ldb.exceptions import DataObjectNotFoundError, IndexingException
from ldb.index.base import (
    AnnotationFileIndexingItem,
    IndexedObjectResult,
    Indexer,
)
from ldb.index.utils import (
    DataObjectMeta,
    FileSystemPath,
    get_annotation_content,
)
from ldb.typing import JSONObject
from ldb.utils import DATA_OBJ_ID_PREFIX, current_time, load_data_file


class AnnotationOnlyIndexer(Indexer):
    def _index(self) -> None:
        for fs, paths in self.preprocessor.annotation_paths.items():
            for path in paths:
                item = AnnotationOnlyIndexingItem(
                    self.ldb_dir,
                    current_time(),
                    self.tags,
                    FileSystemPath(fs, path),
                )
                self.result.append(item.index_data())


@dataclass
class AnnotationOnlyIndexingItem(AnnotationFileIndexingItem):
    annotation_fsp: FileSystemPath

    @cached_property
    def annotation_file_content(self) -> JSONObject:
        return get_annotation_content(  # type: ignore[return-value]
            *self.annotation_fsp,
        )

    @cached_property
    def data_object_hash(self) -> str:
        try:
            return self.annotation_file_content[  # type: ignore[no-any-return]
                "ldb_meta"
            ]["data_object_id"]
        except KeyError as exc:
            raise IndexingException(
                "Missing ldb_meta.data_object_id key for annotation-only "
                f"format: {self.annotation_fsp.path}",
            ) from exc

    @cached_property
    def data_object_meta(self) -> DataObjectMeta:
        meta_content: DataObjectMeta = load_data_file(
            self.data_object_meta_file_path,
        )
        meta_content["last_indexed"] = self.current_timestamp
        meta_content["tags"] = sorted(  # type: ignore[assignment]
            set(meta_content["tags"])  # type: ignore[arg-type]
            | set(self.tags),
        )
        return meta_content

    @cached_property
    def annotation_content(self) -> JSONObject:  # type: ignore[override]
        return self.annotation_file_content["annotation"]  # type: ignore[no-any-return] # noqa: E501

    def index_data(self) -> IndexedObjectResult:
        if not self.data_object_dir.exists():
            raise DataObjectNotFoundError(
                "Data object not found: "
                f"{DATA_OBJ_ID_PREFIX}{self.data_object_hash} "
                f"(annotation_file_path={self.annotation_fsp.path!r})",
            )

        new_annotation = not self.annotation_meta_file_path.is_file()
        self.enqueue_data(self.data_object_to_write())
        self.enqueue_data(self.annotation_to_write())
        self.write_data()
        return IndexedObjectResult(
            found_data_object=False,
            found_annotation=True,
            new_data_object=False,
            new_annotation=new_annotation,
            data_object_hash=self.data_object_hash,
        )
