from pathlib import Path
from typing import Sequence

from fsspec.core import OpenFile

from ldb.data_formats import INDEX_FORMATS, Format
from ldb.index.annotation_only import AnnotationOnlyIndexer
from ldb.index.base import Indexer, IndexingResult, PairIndexer, Preprocessor
from ldb.index.inferred import InferredIndexer, InferredPreprocessor
from ldb.index.label_studio import LabelStudioIndexer, LabelStudioPreprocessor


def index(
    ldb_dir: Path,
    paths: Sequence[str],
    read_any_cloud_location: bool = False,
    fmt: str = Format.AUTO,
) -> IndexingResult:
    indexer: Indexer
    fmt = INDEX_FORMATS.get(fmt, fmt)
    print(f"Data format: {fmt}")
    if fmt in (Format.AUTO, Format.STRICT, Format.BARE, Format.ANNOT):
        preprocessor = Preprocessor(paths)
        if fmt == Format.AUTO:
            fmt = autodetect_format(
                preprocessor.data_object_files,
                preprocessor.annotation_files,
            )
            print(f"Auto-detected data format: {fmt}")
        if fmt in (Format.STRICT, Format.BARE):
            indexer = PairIndexer(
                ldb_dir,
                preprocessor,
                read_any_cloud_location,
                fmt == Format.STRICT,
            )
        elif fmt == Format.ANNOT:
            indexer = AnnotationOnlyIndexer(
                ldb_dir,
                preprocessor,
            )
    elif fmt == Format.INFER:
        preprocessor = InferredPreprocessor(paths)
        indexer = InferredIndexer(
            ldb_dir,
            preprocessor,
            read_any_cloud_location,
            fmt == Format.STRICT,
        )
    elif fmt == Format.LABEL_STUDIO:
        preprocessor = LabelStudioPreprocessor(paths)
        indexer = LabelStudioIndexer(
            ldb_dir,
            preprocessor,
            read_any_cloud_location,
            fmt == Format.STRICT,
        )
    else:
        raise ValueError(f"Not a valid indexing format: {fmt}")
    print("\nIndexing paths...")
    indexer.index()
    return indexer.result


def autodetect_format(
    files: Sequence[OpenFile],
    annotation_files: Sequence[OpenFile],
) -> str:
    if annotation_files and not files:
        return Format.ANNOT
    return Format.STRICT
