import json
import os
import shutil
import subprocess
from contextlib import contextmanager
from pathlib import Path
from subprocess import Popen
from tempfile import TemporaryDirectory
from typing import (
    IO,
    TYPE_CHECKING,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
)

from ldb.data_formats import Format
from ldb.exceptions import DataObjectNotFoundError
from ldb.instantiate import InstConfig, instantiate_collection_directly
from ldb.path import InstanceDir
from ldb.storage import get_storage_locations

if TYPE_CHECKING:
    StrPopen = Popen[str]  # pylint: disable=unsubscriptable-object)
else:
    StrPopen = Popen


def sort_collection(
    ldb_dir: Path,
    collection: Iterable[Tuple[str, str]],
    proc_args: List[str],
) -> Iterator[Tuple[str, str]]:
    collection_dict = dict(collection)
    storage_locations = get_storage_locations(ldb_dir)
    with TemporaryDirectory() as temp_dir:
        result = instantiate_collection_directly(
            InstConfig(
                ldb_dir=ldb_dir,
                storage_locations=storage_locations,
                dest_dir=temp_dir,
            ),
            collection_dict,
            Format.BARE,
        )
        data_sequences = (
            collection_dict,
            result.data_object_paths,
            result.annotation_paths,
        )
        data = json.dumps(list(zip(*data_sequences)))
        paths = [str(ldb_dir / InstanceDir.USER_FILTERS)]
        for data_obj_hash in run_sort_process(proc_args, data, paths=paths):
            try:
                annot_hash = collection_dict[data_obj_hash]
            except KeyError:
                raise DataObjectNotFoundError(data_obj_hash) from KeyError
            yield data_obj_hash, annot_hash


@contextmanager
def open_proc(
    proc_args: Sequence[str],
    cwd: Optional[str] = None,
) -> Iterator[StrPopen]:
    with Popen(  # noqa: S603
        proc_args,
        stdout=subprocess.PIPE,
        stdin=subprocess.PIPE,
        text=True,
        cwd=cwd,
    ) as proc:
        yield proc


@contextmanager
def open_plugin(
    proc_args: Sequence[str],
    paths: Sequence[str] = (),
    set_cwd: bool = False,
) -> Iterator[StrPopen]:
    proc_path, other_args = proc_args[0], proc_args[1:]
    cwd = None
    if paths and not os.path.split(proc_path)[0]:
        for path in paths:
            full_path = shutil.which(proc_path, path=path)
            if full_path is not None:
                try:
                    if set_cwd:
                        cwd = os.path.split(full_path)[0] or None
                    with open_proc([full_path, *other_args], cwd=cwd) as proc:
                        yield proc
                except OSError:
                    pass
                else:
                    return
    if set_cwd:
        full_path = shutil.which(proc_path)
        if full_path is not None:
            cwd = os.path.split(full_path)[0] or None
            proc_path = full_path
    with open_proc([proc_path, *other_args], cwd=cwd) as proc:
        yield proc


def run_sort_process(
    proc_args: Sequence[str],
    data: str,
    paths: Sequence[str] = (),
) -> Iterator[str]:
    with open_plugin(proc_args, paths) as proc:
        stdin: IO[str] = proc.stdin  # type: ignore[assignment]
        stdout: IO[str] = proc.stdout  # type: ignore[assignment]
        stdin.write(data)
        stdin.close()
        for line in stdout.readlines():
            yield line.rstrip()
