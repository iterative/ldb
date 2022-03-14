import os
from typing import Any, Dict, Optional, Sequence, Union

from fsspec.callbacks import _DEFAULT_CALLBACK, Callback
from fsspec.spec import AbstractFileSystem

FSProtocol = Union[str, Sequence[str]]


def first_protocol(fs_protocol: FSProtocol) -> str:
    if isinstance(fs_protocol, str):
        return fs_protocol
    return fs_protocol[0]


def has_protocol(fs_protocol: FSProtocol, protocol: str) -> bool:
    if isinstance(fs_protocol, str):
        return protocol == fs_protocol
    return protocol in fs_protocol


def cp_file_any_fs(
    source_fs: AbstractFileSystem,
    source_path: str,
    dest_fs: AbstractFileSystem,
    dest_path: str,
) -> None:
    if source_fs is dest_fs:
        source_fs.cp_file(source_path, dest_path)
    elif source_fs.protocol == "file":
        dest_fs.put_file(source_path, dest_path)
    elif dest_fs.protocol == "file":
        source_fs.get_file(source_path, dest_path)
    else:
        cp_file_across_fs(source_fs, source_path, dest_fs, dest_path)


def cp_file_across_fs(
    source_fs: AbstractFileSystem,
    source_path: str,
    dest_fs: AbstractFileSystem,
    dest_path: str,
    callback: Callback = _DEFAULT_CALLBACK,
    source_kwargs: Optional[Dict[str, Any]] = None,
    dest_kwargs: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Transfer single file between two filesystems.

    These could both be remote, and the implementation combines parts
    of:   fsspec.spec.AbstractFileSystem.put_file
    fsspec.spec.AbstractFileSystem.get_file
    """
    if source_fs.isdir(source_path):
        dest_fs.makedirs(dest_path, exist_ok=True)
        return None

    if source_kwargs is None:
        source_kwargs = {}
    if dest_kwargs is None:
        dest_kwargs = {}
    with source_fs.open(source_path, "rb", **source_kwargs) as f1:
        callback.set_size(getattr(f1, "size", None))
        parent = dest_fs._parent(  # pylint: disable=protected-access
            os.fspath(dest_path),
        )
        dest_fs.mkdirs(parent, exist_ok=True)
        with dest_fs.open(dest_path, "wb", **dest_kwargs) as f2:
            data = True
            while data:
                data = f1.read(source_fs.blocksize)
                segment_len = f2.write(data)
                callback.relative_update(segment_len)
