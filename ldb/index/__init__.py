import os
import re
from itertools import chain
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple, Union

import fsspec
from fsspec.core import OpenFile

from ldb.data_formats import INDEX_FORMATS, Format
from ldb.exceptions import (
    DataObjectNotFoundError,
    IndexingException,
    LDBException,
)
from ldb.index.indexer import (
    AnnotationOnlyIndexingItem,
    IndexedObjectResult,
    IndexingResult,
    InferredIndexer,
    PairIndexer,
)
from ldb.utils import current_time, json_dumps, write_data_file

ENDING_DOUBLE_STAR_RE = r"(?:/+\*\*)+/*$"

AnnotationMeta = Dict[str, Union[str, int, None]]
DataObjectMeta = Dict[str, Union[str, Dict[str, Union[str, int]]]]
DataToWrite = Tuple[Path, bytes, bool]


def index(
    ldb_dir: Path,
    paths: Sequence[str],
    read_any_cloud_location: bool = False,
    fmt: str = Format.AUTO,
) -> IndexingResult:
    fmt = INDEX_FORMATS[fmt]
    paths = [os.path.abspath(p).replace("\\", "/") for p in paths]

    dir_paths = []
    if fmt == Format.INFER:
        dir_paths = expand_dir_paths(paths)
        file_seqs = [
            fsspec.open_files(p.rstrip("/") + "/**") for p in dir_paths
        ]
        dir_path_to_files = {d: f for d, f in zip(dir_paths, file_seqs) if f}
        files = list(chain(*dir_path_to_files.values()))
    else:
        files = get_storage_files_for_paths(
            paths,
            default_format=fmt in (Format.STRICT, Format.BARE),
        )
    if not files:
        raise LDBException(
            "No files found matching the given paths.",
        )

    files, annotation_files = group_storage_files_by_type(
        files,
    )
    if fmt == Format.AUTO:
        fmt = autodetect_format(files, annotation_files)
    if fmt in (Format.STRICT, Format.BARE):
        indexer = PairIndexer(
            ldb_dir,
            files,
            annotation_files,
            read_any_cloud_location,
            fmt == Format.STRICT,
        )
        indexer.index()
        return indexer.result
    if fmt == Format.INFER:
        if annotation_files:
            first_path = annotation_files[0].path
            raise IndexingException(
                f"No annotation files should be present for {Format.INFER} "
                "format.\n"
                f"Found {len(annotation_files)} JSON files.\n"
                f"First path: {first_path}",
            )
        indexer = InferredIndexer(
            ldb_dir,
            files,
            read_any_cloud_location,
            fmt == Format.STRICT,
            dir_path_to_files,
        )
        indexer.index()
        return indexer.result
    if fmt == Format.ANNOT:
        return index_annotation_only(ldb_dir, annotation_files)
    raise ValueError(f"Not a valid indexing format: {fmt}")


def autodetect_format(
    files: Sequence[OpenFile],
    annotation_files: Sequence[OpenFile],
) -> str:
    if annotation_files and not files:
        return Format.ANNOT
    return Format.STRICT


def expand_dir_paths(paths: Iterable[str]) -> List[str]:
    fs = fsspec.filesystem("file")
    for path in paths:
        path, num = re.subn(ENDING_DOUBLE_STAR_RE, "", path)
        if num:
            raise IndexingException(
                f"Paths passed with the {Format.INFER} format should only "
                "match directories, so globs with a final /** should not be "
                "used",
            )

    paths = sorted({p for p in fs.expand_path(paths) if fs.isdir(p)})
    for i in range(len(paths) - 1):
        if paths[i + 1].startswith(paths[i]):
            raise IndexingException(
                f"Paths passed with the {Format.INFER} format should match "
                "non-overlapping directories. Found overlapping directories:\n"
                f"{paths[i]!r}\n"
                f"{paths[i + 1]!r}",
            )
    return paths


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


def get_storage_files_for_paths(
    paths: List[str],
    default_format: bool = False,
) -> List[OpenFile]:
    seen = set()
    storage_files = []
    for path in paths:
        for file in get_storage_files(path, default_format=default_format):
            if file.path not in seen:
                storage_files.append(file)
                seen.add(file.path)
    return storage_files


def get_storage_files(
    path: str,
    default_format: bool = False,
) -> List[OpenFile]:
    """
    Get storage files for indexing that match the glob, `path`.

    Because every file path under `path` is returned, any final "/**" does not
    change the result. First, `path` is expanded with any final "/**" removed
    and any matching files are included in the result. Corresponding data
    object file paths are added for matched annotation file paths and vice
    versa. Then everything under matched directories will be added to the
    result.

    The current implementation may result in some directory paths and some
    duplicate paths being included.
    """
    if is_hidden_fsspec_path(path):
        return []
    fs = fsspec.filesystem("file")
    # "path/**", "path/**/**" or "path/**/" are treated the same as just "path"
    # so we strip off any "/**" or "/**/" at the end
    path = re.sub(ENDING_DOUBLE_STAR_RE, "", path)
    # find corresponding data object for annotation match and vice versa
    # for any files the expanded `path` glob matches
    file_match_globs = []
    for mpath in fs.expand_path(path):
        if not is_hidden_fsspec_path(mpath) and fs.isfile(mpath):
            file_match_globs.append(mpath)
            if default_format:
                # TODO: Check all extension levels (i.e. for abc.tar.gz use
                # .tar.gz and .gz)
                p_without_ext, ext = os.path.splitext(path)
                if ext == ".json":
                    file_match_globs.append(p_without_ext)
                    file_match_globs.append(p_without_ext + ".*")
                else:
                    file_match_globs.append(p_without_ext + ".json")
    files = (
        list(fsspec.open_files(file_match_globs)) if file_match_globs else []
    )
    # capture everything under any directories the `path` glob matches
    for file in fsspec.open_files(path + "/**"):
        if not is_hidden_fsspec_path(file.path):
            files.append(file)
    return files


def is_hidden_fsspec_path(path: str) -> bool:
    return re.search(r"^\.|/\.", path) is not None


def group_storage_files_by_type(
    storage_files: Iterable[OpenFile],
) -> Tuple[List[OpenFile], List[OpenFile]]:
    data_object_files = []
    annotation_files = []
    seen = set()
    for storage_file in storage_files:
        if storage_file.path not in seen:
            seen.add(storage_file.path)
            if storage_file.fs.isfile(storage_file.path):
                if storage_file.path.endswith(".json"):
                    annotation_files.append(storage_file)
                else:
                    data_object_files.append(storage_file)
    return data_object_files, annotation_files
