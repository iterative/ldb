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
from ldb.instantiate import instantiate_collection
from ldb.path import InstanceDir

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
    with TemporaryDirectory() as temp_dir:
        result = instantiate_collection(
            ldb_dir,
            collection_dict,
            temp_dir,
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
def open_proc(proc_args: Sequence[str]) -> Iterator[StrPopen]:
    with Popen(
        proc_args,
        stdout=subprocess.PIPE,
        stdin=subprocess.PIPE,
        text=True,
    ) as proc:
        yield proc


@contextmanager
def open_plugin(
    proc_args: Sequence[str],
    paths: Optional[Sequence[str]] = None,
) -> Iterator[StrPopen]:
    proc_path, other_args = proc_args[0], proc_args[1:]
    if paths is not None and not os.path.split(proc_path)[0]:
        for path in paths:
            full_path = shutil.which(proc_path, path=path)
            if full_path is not None:
                try:
                    with open_proc([full_path, *other_args]) as proc:
                        yield proc
                except OSError:
                    pass
                else:
                    return
    with open_proc(proc_args) as proc:
        yield proc


def run_sort_process(
    proc_args: List[str],
    data: str,
    paths: Optional[Sequence[str]] = None,
) -> Iterator[str]:
    with open_plugin(proc_args, paths) as proc:
        stdin: IO[str] = proc.stdin  # type: ignore[assignment]
        stdout: IO[str] = proc.stdout  # type: ignore[assignment]
        stdin.write(data)
        stdin.close()
        for line in stdout.readlines():
            yield line.rstrip()
