import hashlib
import json
import os
import random
import re
import stat
import string
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Iterator,
    Optional,
    Tuple,
    TypeVar,
    Union,
    overload,
)

from fsspec.spec import AbstractFileSystem

from ldb.fs.utils import first_protocol

if TYPE_CHECKING:
    from _typeshed import SupportsGetItem, SupportsRead

DATASET_PREFIX = "ds:"
WORKSPACE_DATASET_PREFIX = "ws:"
DATA_OBJ_ID_PREFIX = "id:"
ROOT = "root"
CHUNK_SIZE = 2**20
HASH_DIR_SPLIT_POINT = 3
UNIQUE_ID_ALPHABET = string.ascii_lowercase + string.digits
DATA_OBJ_ID_PATTERN = "^(?:id:)?([0-9a-f]{32})$"

_KT_contra = TypeVar("_KT_contra", contravariant=True)
_VT_co = TypeVar("_VT_co", covariant=True)
_T = TypeVar("_T")


@overload
def get_first(
    container: "SupportsGetItem[_KT_contra, _VT_co]",
    *keys: _KT_contra,
) -> Optional[_VT_co]:
    ...


@overload
def get_first(
    container: "SupportsGetItem[_KT_contra, _VT_co]",
    *key: _KT_contra,
    default: _T,
) -> Union[_VT_co, _T]:
    ...


def get_first(
    container: "SupportsGetItem[_KT_contra, _VT_co]",
    *keys: _KT_contra,
    default: Optional[_T] = None,
) -> Union[_VT_co, Optional[_T]]:
    for key in keys:
        try:
            return container[key]
        except LookupError:
            pass
    return default


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


def get_etag_md5_match(etag: str) -> str:
    # The e-tag will be a json string, so look for double quotes
    md5_hash_match = re.match(r"(?i)\"([a-f\d]{32})\"", etag)
    if md5_hash_match is None:
        return ""
    return md5_hash_match.group(1)


def get_file_hash(fs: AbstractFileSystem, path: str) -> str:
    if first_protocol(fs.protocol) in ("s3", "s3a"):
        return get_etag_md5_match(fs.info(path).get("ETag", "")) or hash_file(
            fs,
            path,
        )
    return hash_file(fs, path)


def hash_file(fs: AbstractFileSystem, path: str) -> str:
    hash_obj = hashlib.md5()
    with fs.open(path, "rb") as file:
        for chunk in iter_chunks(file):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()


def hash_data(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def iter_chunks(
    file: "SupportsRead[bytes]",
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
    return dt_obj.astimezone(timezone.utc).isoformat(" ")


def parse_datetime(dt_str: str) -> datetime:
    return datetime.fromisoformat(dt_str).astimezone(timezone.utc)


def timestamp_to_datetime(timestamp: float) -> datetime:
    return datetime.fromtimestamp(timestamp, timezone.utc)


def current_time() -> datetime:
    return datetime.now(timezone.utc)


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
        raise ValueError(
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
    match = re.search(DATA_OBJ_ID_PATTERN, hash_identifier)
    if match is None:
        raise ValueError(
            "hash_identifier must be 32 hexadecimal characters, optionally "
            f"prefixed with {DATA_OBJ_ID_PREFIX}",
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
