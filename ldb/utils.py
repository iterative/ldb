import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from typing import BinaryIO

from fsspec.core import OpenFile

CHUNK_SIZE = 2 ** 20
HASH_DIR_SPLIT_POINT = 3


def write_data_file(
    file_path: Path,
    data: bytes,
    overwrite_existing: bool = True,
) -> None:
    if overwrite_existing or not file_path.exists():
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with file_path.open("wb") as file:
            file.write(data)


def hash_file(file: OpenFile) -> str:
    hash_obj = hashlib.md5()
    with file as open_file:
        for chunk in iter_chunks(open_file):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()


def hash_data(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def iter_chunks(file: BinaryIO, chunk_size: int = CHUNK_SIZE):
    data = file.read(chunk_size)
    while data:
        yield data
        data = file.read(chunk_size)


def get_hash_path(base_dir: Path, hash_str: str) -> Path:
    return (
        base_dir
        / hash_str[:HASH_DIR_SPLIT_POINT]
        / hash_str[HASH_DIR_SPLIT_POINT:]
    )


def format_datetime(dt_obj: datetime):
    return dt_obj.astimezone().isoformat(" ")


def parse_datetime(dt_str: str):
    return datetime.fromisoformat(dt_str)


def load_data_file(path: Path):
    with path.open() as file:
        return json.load(file)


def get_filetype(path: str) -> str:
    return os.path.splitext(path)[1].lstrip(".")