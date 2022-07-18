from dataclasses import dataclass

import fsspec
from fsspec.utils import get_protocol
from funcy.objects import cached_property

from ldb.exceptions import DataObjectNotFoundError, IndexingException
from ldb.fs.utils import unstrip_protocol
from ldb.index.base import (
    AnnotationFileIndexingItem,
    DataObjectFileIndexingItem,
    IndexedObjectResult,
    Indexer,
    Preprocessor,
)
from ldb.index.utils import (
    DataObjectMeta,
    FileSystemPath,
    get_annotation_content,
)
from ldb.storage import get_containing_storage_location
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
                    self.annot_merge_strategy,
                    FileSystemPath(fs, path),
                    self.preprocessor,
                )
                self.result.append(item.index_data())


@dataclass
class AnnotationOnlyIndexingItem(AnnotationFileIndexingItem):
    annotation_fsp: FileSystemPath
    preprocessor: Preprocessor
    check_hash: bool = True

    @cached_property
    def annotation_file_content(self) -> JSONObject:
        return get_annotation_content(  # type: ignore[return-value]
            *self.annotation_fsp,
        )

    @cached_property
    def data_object_hash(self) -> str:
        try:
            return self.annotation_file_content[  # type: ignore[no-any-return]
                "data-object-info"
            ]["md5"]
        except KeyError:
            return ""

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
    def raw_annotation_content(self) -> JSONObject:  # type: ignore[override]
        return self.annotation_file_content["annotation"]  # type: ignore[no-any-return] # noqa: E501

    def index_data(self) -> IndexedObjectResult:
        if not self.data_object_hash or not self.data_object_dir.exists():
            try:
                path = self.annotation_file_content["data-object-info"]["path"]
            except KeyError:
                raise DataObjectNotFoundError(  # pylint: disable=raise-missing-from # noqa: E501
                    "Data object not found: "
                    f"{DATA_OBJ_ID_PREFIX}{self.data_object_hash} "
                    f"(annotation_file_path={self.annotation_fsp.path!r})\n"
                    "To index in the annotation-only format, provide a path "
                    "under the data-object-info.path key or provide the md5 "
                    "hash under the data-object-info.md5 key",
                )
            protocol = get_protocol(path)
            fs_cls = fsspec.get_filesystem_class(protocol)
            path = fs_cls._strip_protocol(  # pylint: disable=protected-access
                path,
            )  # pylint: disable=protected-access
            loc = get_containing_storage_location(
                path,
                protocol,
                self.preprocessor.storage_locations,
            )
            fs = fs_cls(**loc.options) if loc is not None else fs_cls()
            data_obj_item = DataObjectFileIndexingItem(
                self.ldb_dir,
                current_time(),
                self.tags,
                self.annot_merge_strategy,
                FileSystemPath(fs, path),
                True,
                {},
            )
            if loc is None:
                if (
                    not self.data_object_hash
                    and data_obj_item.data_object_dir.exists()
                ):
                    # if we didn't have the hash from the annotation file but
                    # the computed hash exists in the ldb index we have a
                    # previously indexed ephemeral file
                    data_obj_item.save_data_object_path_info = False
                else:
                    raise IndexingException(f"Not in storage location: {path}")
                # TODO consider handling all ephemeral data object files
            if not self.data_object_hash:
                self.data_object_hash = data_obj_item.data_object_hash
            elif not self.check_hash:
                # simply use the hash provided in the annotation file
                # instead of computing the data object hash
                data_obj_item.data_object_hash = self.data_object_hash
            elif self.data_object_hash != data_obj_item.data_object_hash:
                raise IndexingException(
                    "Data object md5 hashes do not match:\n"
                    f"found hash:    {self.data_object_hash}\n"
                    f"computed hash: {data_obj_item.data_object_hash}\n"
                    "annotation path: "
                    f"{unstrip_protocol(*self.annotation_fsp)}\n"
                    f"data object path: {unstrip_protocol(fs, path)}\n\n"
                    "The found hash at an annotation file's "
                    "data-object-info.md5 key must match the computed hash of "
                    "the file located at data-object-info.path if both keys "
                    "are provided. A mismatch can happen if the file at this "
                    "data object path was modified or replaced.",
                )
            data_obj_result = data_obj_item.index_data()
            found_data_object = True
            new_data_object = data_obj_result.new_data_object
            new_data_object_path = data_obj_result.new_data_object_path
        else:
            found_data_object = False
            new_data_object = False
            new_data_object_path = False
            self.enqueue_data(self.data_object_to_write())

        new_annotation = not self.annotation_meta_file_path.is_file()
        self.enqueue_data(self.annotation_to_write())
        self.write_data()
        return IndexedObjectResult(
            found_data_object=found_data_object,
            found_annotation=True,
            new_data_object=new_data_object,
            new_annotation=new_annotation,
            new_data_object_path=new_data_object_path,
            data_object_hash=self.data_object_hash,
        )
