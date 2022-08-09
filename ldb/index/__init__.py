import os
from pathlib import Path
from typing import Collection, Mapping, Optional, Sequence, Union

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
from ldb.path import WorkspacePath
from ldb.storage import get_storage_locations
from ldb.utils import format_dataset_identifier, json_dumps
from ldb.workspace import load_workspace_dataset


def index(
    ldb_dir: Path,
    paths: Sequence[str],
    read_any_cloud_location: bool = False,
    fmt: str = Format.AUTO,
    tags: Collection[str] = (),
    annot_merge_strategy: AnnotMergeStrategy = AnnotMergeStrategy.REPLACE,
    params: Optional[Mapping[str, str]] = None,
    ephemeral_remote: bool = False,
    workspace_path: Optional[Union[str, Path]] = None,
) -> IndexingResult:
    if workspace_path is not None:
        workspace_path = Path(os.path.normpath(workspace_path))
        ds_name = load_workspace_dataset(workspace_path).dataset_name
    else:
        ds_name = ""

    if params is None:
        params = {}
    indexer: Optional[Indexer] = None
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
    if indexer is None:
        raise ValueError(f"Not a valid indexing format: {fmt}")

    indexer.index()
    if workspace_path is not None:
        from ldb.add import (  # pylint: disable=import-outside-toplevel
            AddResult,
            add_to_collection_dir,
        )

        collection_dir_path = workspace_path / WorkspacePath.COLLECTION
        transform_dir_path = workspace_path / WorkspacePath.TRANSFORM_MAPPING

        ds_ident = format_dataset_identifier(ds_name)
        print(f"Adding to {ds_ident} at ws:{workspace_path}")

        collection_list = list(indexer.result.collection.items())
        num_data_objects = add_to_collection_dir(
            collection_dir_path,
            collection_list,
        )
        transform_data = [
            (data_obj_id, json_dumps(transforms))
            for data_obj_id, transforms in indexer.result.transforms.items()
        ]
        num_transforms = add_to_collection_dir(
            transform_dir_path,
            transform_data,
        )
        indexer.result.add_result = AddResult(
            collection_list,
            num_data_objects,
            num_transforms,
            ds_name,
            str(workspace_path),
        )
    return indexer.result


def autodetect_format(
    data_object_paths: FSPathsMapping,
    annotation_paths: FSPathsMapping,
) -> str:
    if any(annotation_paths.values()) and not any(data_object_paths.values()):
        return Format.ANNOT
    return Format.STRICT
