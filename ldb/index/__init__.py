from pathlib import Path
from typing import Collection, Sequence

from ldb.data_formats import INDEX_FORMATS, Format
from ldb.index.annotation_only import AnnotationOnlyIndexer
from ldb.index.base import Indexer, IndexingResult, PairIndexer, Preprocessor
from ldb.index.inferred import InferredIndexer, InferredPreprocessor
from ldb.index.label_studio import LabelStudioIndexer, LabelStudioPreprocessor
from ldb.index.utils import FSPathsMapping
from ldb.storage import get_storage_locations


def index(
    ldb_dir: Path,
    paths: Sequence[str],
    read_any_cloud_location: bool = False,
    fmt: str = Format.AUTO,
    tags: Collection[str] = (),
) -> IndexingResult:
    indexer: Indexer
    fmt = INDEX_FORMATS.get(fmt, fmt)
    print(f"Data format: {fmt}")
    storage_locations = get_storage_locations(ldb_dir)
    if fmt in (Format.AUTO, Format.STRICT, Format.BARE, Format.ANNOT):
        preprocessor = Preprocessor(paths, storage_locations)
        if fmt == Format.AUTO:
            fmt = autodetect_format(
                preprocessor.data_object_paths,
                preprocessor.annotation_paths,
            )
            print(f"Auto-detected data format: {fmt}")
        if fmt in (Format.STRICT, Format.BARE):
            indexer = PairIndexer(
                ldb_dir,
                preprocessor,
                read_any_cloud_location,
                fmt == Format.STRICT,
                tags=tags,
            )
        elif fmt == Format.ANNOT:
            indexer = AnnotationOnlyIndexer(
                ldb_dir,
                preprocessor,
                tags=tags,
            )
    elif fmt == Format.INFER:
        preprocessor = InferredPreprocessor(paths, storage_locations)
        indexer = InferredIndexer(
            ldb_dir,
            preprocessor,
            read_any_cloud_location,
            fmt == Format.STRICT,
            tags=tags,
        )
    elif fmt == Format.LABEL_STUDIO:
        preprocessor = LabelStudioPreprocessor(paths, storage_locations)
        indexer = LabelStudioIndexer(
            ldb_dir,
            preprocessor,
            read_any_cloud_location,
            fmt == Format.STRICT,
            tags=tags,
        )
    else:
        raise ValueError(f"Not a valid indexing format: {fmt}")
    print("\nIndexing paths...")
    indexer.index()
    return indexer.result


def autodetect_format(
    data_object_paths: FSPathsMapping,
    annotation_paths: FSPathsMapping,
) -> str:
    if any(annotation_paths.values()) and not any(data_object_paths.values()):
        return Format.ANNOT
    return Format.STRICT
