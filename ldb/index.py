import getpass
import json
import os
from datetime import datetime
from typing import Any, Dict

import fsspec
from fsspec.core import OpenFile

from ldb.config import get_ldb_dir
from ldb.path import InstanceDir
from ldb.utils import (
    format_datetime,
    get_filetype,
    get_hash_path,
    hash_data,
    hash_file,
    load_data_file,
    parse_datetime,
    write_data_file,
)


def index(path: str) -> None:
    ldb_dir = get_ldb_dir()
    annotation_files = {}
    data_object_files = {}
    for storage_file in fsspec.open_files(path.rstrip("/") + "/**"):
        if storage_file.path.endswith(".json"):
            annotation_files[storage_file.path] = storage_file
        else:
            data_object_files[storage_file.path] = storage_file

    for data_object_file in data_object_files.values():
        hash_str = hash_file(data_object_file)
        data_object_dir = get_hash_path(
            ldb_dir / InstanceDir.DATA_OBJECT_INFO,
            hash_str,
        )
        data_object_meta_file_path = data_object_dir / "meta"

        current_timestamp = format_datetime(datetime.now())
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
        annotation_file = annotation_files.get(annotation_path)
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

        for file_path, data, overwrite_existing in to_write:
            write_data_file(file_path, data, overwrite_existing)


def construct_data_object_meta(
    file: OpenFile,
    prev_meta: Dict[str, Any],
    current_timestamp: str,
):
    fs_info = os.stat(file.path)

    atime = datetime.fromtimestamp(fs_info.st_atime).astimezone()
    mtime = datetime.fromtimestamp(fs_info.st_mtime).astimezone()
    ctime = datetime.fromtimestamp(fs_info.st_ctime).astimezone()

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
    return {
        "type": get_filetype(file.path),
        "first_indexed": first_indexed,
        "last_indexed": current_timestamp,
        "last_indexed_by": getpass.getuser(),
        "tags": tags,
        "alternate_paths": alternate_paths,
        "fs": {
            "fs_id": "",
            "path": file.path,
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
        mtimes.append(datetime.fromtimestamp(curr_mtime).astimezone())

    mtime = format_datetime(max(mtimes)) if mtimes else None
    return {
        "version": version,
        "mtime": mtime,
        "first_indexed_time": first_indexed_time,
        "last_indexed_time": current_timestamp,
    }
