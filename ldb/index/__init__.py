from pathlib import Path
from typing import Iterable, Sequence

from fsspec.core import OpenFile

from ldb.data_formats import INDEX_FORMATS, Format
from ldb.exceptions import DataObjectNotFoundError
from ldb.index.indexer import (
    AnnotationOnlyIndexingItem,
    IndexedObjectResult,
    IndexingResult,
    InferredIndexer,
    InferredPreprocessor,
    PairIndexer,
    Preprocessor,
)
from ldb.utils import current_time, json_dumps, write_data_file


def index(
    ldb_dir: Path,
    paths: Sequence[str],
    read_any_cloud_location: bool = False,
    fmt: str = Format.AUTO,
) -> IndexingResult:
    fmt = INDEX_FORMATS[fmt]
    if fmt in (Format.AUTO, Format.STRICT, Format.BARE, Format.ANNOT):
        preprocessor = Preprocessor(fmt, paths)
        if fmt == Format.AUTO:
            fmt = autodetect_format(
                preprocessor.data_object_files,
                preprocessor.annotation_files,
            )
        if fmt in (Format.STRICT, Format.BARE):
            indexer = PairIndexer(
                ldb_dir,
                preprocessor,
                read_any_cloud_location,
                fmt == Format.STRICT,
            )
        elif fmt == Format.ANNOT:
            return index_annotation_only(
                ldb_dir,
                preprocessor.annotation_files,
            )
    elif fmt == Format.INFER:
        preprocessor = InferredPreprocessor(fmt, paths)
        indexer = InferredIndexer(
            ldb_dir,
            preprocessor,
            read_any_cloud_location,
            fmt == Format.STRICT,
        )
    else:
        raise ValueError(f"Not a valid indexing format: {fmt}")
    indexer.index()
    return indexer.result


def autodetect_format(
    files: Sequence[OpenFile],
    annotation_files: Sequence[OpenFile],
) -> str:
    if annotation_files and not files:
        return Format.ANNOT
    return Format.STRICT


def index_annotation_only(
    ldb_dir: Path,
    annotation_files: Iterable[OpenFile],
) -> IndexingResult:
    result = IndexingResult()
    try:
        for file in annotation_files:
            result.append(index_single_annotation_only_file(ldb_dir, file))
    except Exception:
        print(result.summary(finished=False))
        print()
        raise
    return result


def index_single_annotation_only_file(
    ldb_dir: Path,
    annotation_file: OpenFile,
) -> IndexedObjectResult:
    item = AnnotationOnlyIndexingItem(
        ldb_dir,
        current_time(),
        annotation_file,
    )
    if not item.data_object_dir.exists():
        raise DataObjectNotFoundError(
            f"Data object not found: 0x{item.data_object_hash} "
            f"(annotation_file_path={annotation_file.path!r})",
        )
    to_write = []
    new_annotation = not item.annotation_meta_file_path.is_file()
    annotation_meta_bytes = json_dumps(item.annotation_meta).encode()
    to_write.append(
        (item.annotation_meta_file_path, annotation_meta_bytes, True),
    )
    if not item.annotation_dir.is_dir():
        to_write.append(
            (
                item.annotation_dir / "ldb",
                item.annotation_ldb_content_bytes,
                False,
            ),
        )
        to_write.append(
            (
                item.annotation_dir / "user",
                item.annotation_content_bytes,
                False,
            ),
        )
    to_write.append(
        (
            item.data_object_dir / "current",
            item.annotation_hash.encode(),
            True,
        ),
    )
    for file_path, data, overwrite_existing in to_write:
        write_data_file(file_path, data, overwrite_existing)
    return IndexedObjectResult(
        found_data_object=False,
        found_annotation=True,
        new_data_object=False,
        new_annotation=new_annotation,
        data_object_hash=item.data_object_hash,
    )
