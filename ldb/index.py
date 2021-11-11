import getpass
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List

import fsspec
from fsspec.core import OpenFile

from ldb.exceptions import LDBException
from ldb.path import InstanceDir
from ldb.utils import (
    current_time,
    format_datetime,
    get_filetype,
    get_hash_path,
    hash_data,
    hash_file,
    load_data_file,
    parse_datetime,
    timestamp_to_datetime,
    write_data_file,
)


@dataclass
class IndexingResult:
    num_found_data_objects: int
    num_found_annotations: int
    num_new_data_objects: int
    num_new_annotations: int
    data_object_hashes: List[str]

    def summary(self) -> str:
        return (
            "Finished indexing:\n"
            f"  Found data objects: {self.num_found_data_objects:9d}\n"
            f"  Found annotations:  {self.num_found_annotations:9d}\n"
            f"  New data objects:   {self.num_new_data_objects:9d}\n"
            f"  New annotations:    {self.num_new_annotations:9d}"
        )


def index(ldb_dir: Path, paths: List[str]) -> IndexingResult:
    storage_files = [f for p in paths for f in get_storage_files(p)]
    if not storage_files:
        raise LDBException(
            "No files or directories found matching the given paths.",
        )

    data_object_files, annotation_files_by_path = group_storage_files_by_type(
        storage_files,
    )
    data_object_hashes = []
    num_annotations_indexed = 0
    num_new_data_objects = 0
    num_new_annotations = 0
    for data_object_file in data_object_files:
        hash_str = hash_file(data_object_file)
        data_object_hashes.append(hash_str)
        data_object_dir = get_hash_path(
            ldb_dir / InstanceDir.DATA_OBJECT_INFO,
            hash_str,
        )
        if not data_object_dir.is_dir():
            num_new_data_objects += 1
        data_object_meta_file_path = data_object_dir / "meta"

        current_timestamp = format_datetime(current_time())
        meta_contents = construct_data_object_meta(
            data_object_file,
            load_data_file(data_object_meta_file_path)
            if data_object_meta_file_path.exists()
            else {},
            current_timestamp,
        )
        to_write = [
            (
                data_object_meta_file_path,
                json.dumps(meta_contents).encode(),
                True,
            ),
        ]

        annotation_path = os.path.splitext(data_object_file.path)[0] + ".json"
        annotation_file = annotation_files_by_path.get(annotation_path)
        if annotation_file is not None:
            ldb_content, user_content = get_annotation_content(annotation_file)
            ldb_content_bytes = json.dumps(ldb_content).encode()
            user_content_bytes = json.dumps(user_content).encode()
            annotation_hash = hash_data(ldb_content_bytes + user_content_bytes)

            annotation_meta_dir_path = data_object_dir / "annotations"
            annotation_meta_file_path = (
                annotation_meta_dir_path / annotation_hash
            )

            try:
                version = len(list(annotation_meta_dir_path.iterdir())) + 1
            except FileNotFoundError:
                version = 1
            annotation_meta = construct_annotation_meta(
                annotation_file,
                (
                    load_data_file(annotation_meta_file_path)
                    if annotation_meta_file_path.exists()
                    else {}
                ),
                current_timestamp,
                version,
            )
            annotation_meta_bytes = json.dumps(annotation_meta).encode()
            annotation_dir = get_hash_path(
                ldb_dir / InstanceDir.ANNOTATIONS,
                annotation_hash,
            )
            to_write.append(
                (annotation_meta_file_path, annotation_meta_bytes, True),
            )
            if not annotation_meta_file_path.is_file():
                num_new_annotations += 1
            if not annotation_dir.is_dir():
                to_write.append(
                    (annotation_dir / "ldb", ldb_content_bytes, False),
                )
                to_write.append(
                    (
                        annotation_dir / "user",
                        user_content_bytes,
                        False,
                    ),
                )
            to_write.append(
                (data_object_dir / "current", annotation_hash.encode(), True),
            )
            num_annotations_indexed += 1

        for file_path, data, overwrite_existing in to_write:
            write_data_file(file_path, data, overwrite_existing)

    return IndexingResult(
        num_found_data_objects=len(data_object_files),
        num_found_annotations=num_annotations_indexed,
        num_new_data_objects=num_new_data_objects,
        num_new_annotations=num_new_annotations,
        data_object_hashes=data_object_hashes,
    )


def get_storage_files(path: str) -> List[OpenFile]:
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
    path = re.sub(r"(?:/\*\*)+/?$", "", path)
    # find corresponding data object for annotation match and vice versa
    # for any files the expanded `path` glob matches
    file_match_globs = []
    for mpath in fs.expand_path(path):
        if not is_hidden_fsspec_path(mpath) and fs.isfile(mpath):
            file_match_globs.append(mpath)
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


def is_hidden_fsspec_path(path: str):
    return re.search(r"^\.|/\.", path) is not None


def group_storage_files_by_type(storage_files: Iterable[OpenFile]):
    annotation_files_by_path = {}
    data_object_files = []
    seen = set()
    for storage_file in storage_files:
        if storage_file.path not in seen:
            seen.add(storage_file.path)
            if storage_file.fs.isfile(storage_file.path):
                if storage_file.path.endswith(".json"):
                    annotation_files_by_path[storage_file.path] = storage_file
                else:
                    data_object_files.append(storage_file)
    return data_object_files, annotation_files_by_path


def construct_data_object_meta(
    file: OpenFile,
    prev_meta: Dict[str, Any],
    current_timestamp: str,
):
    fs_info = os.stat(file.path)

    atime = timestamp_to_datetime(fs_info.st_atime)
    mtime = timestamp_to_datetime(fs_info.st_mtime)
    ctime = timestamp_to_datetime(fs_info.st_ctime)

    if prev_meta:
        first_indexed = prev_meta["first_indexed"]
        tags = prev_meta["tags"]
        alternate_paths = prev_meta["alternate_paths"]

        atime = max(atime, parse_datetime(prev_meta["fs"]["atime"]))
        mtime = max(mtime, parse_datetime(prev_meta["fs"]["mtime"]))
        ctime = max(ctime, parse_datetime(prev_meta["fs"]["ctime"]))
    else:
        first_indexed = current_timestamp
        tags = []
        alternate_paths = []

    path_info = {
        "fs_id": "",
        "protocol": "file",
        "path": file.path,
    }
    if path_info not in alternate_paths:
        alternate_paths.append(path_info)
    return {
        "type": get_filetype(file.path),
        "first_indexed": first_indexed,
        "last_indexed": current_timestamp,
        "last_indexed_by": getpass.getuser(),
        "tags": tags,
        "alternate_paths": alternate_paths,
        "fs": {
            **path_info,
            "size": fs_info.st_size,
            "mode": fs_info.st_mode,
            "uid": fs_info.st_uid,
            "gid": fs_info.st_gid,
            "atime": format_datetime(atime),
            "mtime": format_datetime(mtime),
            "ctime": format_datetime(ctime),
        },
    }


def get_annotation_content(annotation_file):
    with annotation_file as open_annotation_file:
        annotation_str = open_annotation_file.read()
    original_content = json.loads(annotation_str)
    ldb_content = {
        "user_version": None,
        "schema_version": None,
    }
    return ldb_content, original_content


def construct_annotation_meta(
    annotation_file,
    prev_annotation_meta,
    current_timestamp,
    version,
):
    mtimes = []
    if prev_annotation_meta:
        prev_mtime = prev_annotation_meta.get("mtime")
        if prev_mtime is not None:
            mtimes.append(parse_datetime(prev_mtime))
        version = prev_annotation_meta["version"]
        first_indexed_time = prev_annotation_meta["first_indexed_time"]
    else:
        first_indexed_time = current_timestamp

    fs_info = annotation_file.fs.info(annotation_file)
    curr_mtime = fs_info.get("created")
    if curr_mtime is not None:
        mtimes.append(timestamp_to_datetime(curr_mtime))

    mtime = format_datetime(max(mtimes)) if mtimes else None
    return {
        "version": version,
        "mtime": mtime,
        "first_indexed_time": first_indexed_time,
        "last_indexed_time": current_timestamp,
    }
