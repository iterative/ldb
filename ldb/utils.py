import hashlib
import json
import os
import random
import re
import stat
import string
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO, Iterator, Optional, Tuple, Union

from fsspec.core import OpenFile

from ldb.exceptions import LDBException

DATASET_PREFIX = "ds:"
ROOT = "root"
CHUNK_SIZE = 2 ** 20
HASH_DIR_SPLIT_POINT = 3
UNIQUE_ID_ALPHABET = string.ascii_lowercase + string.digits


def json_dumps(obj: Any, **kwargs: Any) -> str:
    return json.dumps(obj, sort_keys=True, **kwargs)


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


def iter_chunks(
    file: BinaryIO,
    chunk_size: int = CHUNK_SIZE,
) -> Iterator[bytes]:
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


def format_datetime(dt_obj: datetime) -> str:
    return dt_obj.astimezone().isoformat(" ")


def parse_datetime(dt_str: str) -> datetime:
    return datetime.fromisoformat(dt_str)


def timestamp_to_datetime(timestamp: float) -> datetime:
    return datetime.fromtimestamp(timestamp).astimezone()


def current_time() -> datetime:
    return datetime.now().astimezone()


def load_data_file(path: Path) -> Any:
    with path.open() as file:
        return json.load(file)


def get_filetype(path: str) -> str:
    return os.path.splitext(path)[1].lstrip(".")


def parse_dataset_identifier(
    dataset_identifier: str,
) -> Tuple[str, Optional[int]]:
    match = re.search(
        rf"^{DATASET_PREFIX}([A-Za-z0-9_-]+)(?:\.v(\d+))?$",
        dataset_identifier,
    )
    if match is None:
        raise LDBException(
            'dataset identifier must be in the form "ds:name" or '
            '"ds:name.vN"\n'
            'where "name" can contain characters in the group '
            "[A-Za-z0-9_-]\n"
            'and ".vN" denotes the version number (e.g. ".v1")',
        )
    name, version = match.groups()
    return name, int(version) if version is not None else None


def format_dataset_identifier(
    name: str,
    version: Optional[int] = None,
) -> str:
    version_suffix = f".v{version}" if version else ""
    return f"{DATASET_PREFIX}{name}{version_suffix}"


def get_fsspec_path_suffix(path: str) -> str:
    match = re.search(r"\.[^/]*/*$", path)
    if match is None:
        return ""
    return match.group()


def parse_data_object_hash_identifier(hash_identifier: str) -> str:
    match = re.search("^0x([0-9a-f]{32})$", hash_identifier)
    if match is None:
        raise ValueError(
            "hash_identifier must start with '0x' followed by 32 hexadecimal "
            "characters",
        )
    return match.group(1)


def unique_id(n: int = 8) -> str:
    return "".join(random.choices(UNIQUE_ID_ALPHABET, k=n))


@contextmanager
def chdir(path: Union[str, bytes, Path]) -> Iterator[None]:
    old = os.getcwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(old)


def chmod_minus_x(path: Union[str, bytes, Path]) -> int:
    """
    Turn off the executable bit for everyone.

    Equivalent the unix `chmod -x`
    """
    mode = os.stat(path).st_mode & ~(
        stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
    )
    os.chmod(path, mode)
    return mode
