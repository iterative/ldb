from pathlib import Path
from typing import Collection, Mapping, Optional, Sequence

from ldb.data_formats import INDEX_FORMATS, Format
from ldb.index.annotation_only import (
    AnnotationOnlyIndexer,
    AnnotOnlyParamConfig,
    SingleAnnotationIndexer,
)
from ldb.index.base import Indexer, IndexingResult, PairIndexer, Preprocessor
from ldb.index.inferred import InferredIndexer, InferredPreprocessor
from ldb.index.label_studio import LabelStudioIndexer, LabelStudioPreprocessor
from ldb.index.utils import AnnotMergeStrategy, FSPathsMapping
from ldb.storage import get_storage_locations


def index(
    ldb_dir: Path,
    paths: Sequence[str],
    read_any_cloud_location: bool = False,
    fmt: str = Format.AUTO,
    tags: Collection[str] = (),
    annot_merge_strategy: AnnotMergeStrategy = AnnotMergeStrategy.REPLACE,
    params: Optional[Mapping[str, str]] = None,
    ephemeral_remote: bool = False,
) -> IndexingResult:
    if params is None:
        params = {}
    indexer: Indexer
    fmt = INDEX_FORMATS.get(fmt, fmt)
    print(f"Data format: {fmt}")
    storage_locations = get_storage_locations(ldb_dir)
    if fmt in (Format.AUTO, Format.STRICT, Format.BARE, Format.ANNOT):
        if fmt == Format.ANNOT:
            param_processors = AnnotOnlyParamConfig.PARAM_PROCESSORS
        else:
            param_processors = None
        preprocessor = Preprocessor(
            paths,
            storage_locations,
            params,
            fmt,
            param_processors=param_processors,
        )
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
                annot_merge_strategy=annot_merge_strategy,
                ephemeral_remote=ephemeral_remote,
            )
        elif fmt == Format.ANNOT:
            if preprocessor.params.get("single-file", False):
                indexer = SingleAnnotationIndexer(
                    ldb_dir,
                    preprocessor,
                    tags=tags,
                    annot_merge_strategy=annot_merge_strategy,
                    ephemeral_remote=ephemeral_remote,
                )
            else:
                indexer = AnnotationOnlyIndexer(
                    ldb_dir,
                    preprocessor,
                    tags=tags,
                    annot_merge_strategy=annot_merge_strategy,
                    ephemeral_remote=ephemeral_remote,
                )
    elif fmt == Format.INFER:
        preprocessor = InferredPreprocessor(
            paths,
            storage_locations,
            params,
            fmt,
        )
        indexer = InferredIndexer(
            ldb_dir,
            preprocessor,
            read_any_cloud_location,
            fmt == Format.STRICT,
            tags=tags,
            annot_merge_strategy=annot_merge_strategy,
            ephemeral_remote=ephemeral_remote,
        )
    elif fmt == Format.LABEL_STUDIO:
        preprocessor = LabelStudioPreprocessor(
            paths,
            storage_locations,
            params,
            fmt,
        )
        indexer = LabelStudioIndexer(
            ldb_dir,
            preprocessor,
            read_any_cloud_location,
            fmt == Format.STRICT,
            tags=tags,
            annot_merge_strategy=annot_merge_strategy,
            ephemeral_remote=ephemeral_remote,
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
