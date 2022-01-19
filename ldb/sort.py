import json
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import IO, Iterable, Iterator, List, Tuple

from ldb.data_formats import Format
from ldb.exceptions import DataObjectNotFoundError
from ldb.instantiate import instantiate_collection


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
        data = list(
            zip(
                collection_dict,
                result.data_object_paths,
                result.annotation_paths,
            ),
        )
        data_str = json.dumps(data)
        for data_obj_hash in run_sort_process(proc_args, data_str):
            try:
                annot_hash = collection_dict[data_obj_hash]
            except KeyError:
                raise DataObjectNotFoundError(data_obj_hash) from KeyError
            yield data_obj_hash, annot_hash


def run_sort_process(proc_args: List[str], data: str) -> Iterator[str]:
    with subprocess.Popen(
        proc_args,
        stdout=subprocess.PIPE,
        stdin=subprocess.PIPE,
        text=True,
    ) as proc:
        stdin: IO[str] = proc.stdin  # type: ignore[assignment]
        stdout: IO[str] = proc.stdout  # type: ignore[assignment]
        stdin.write(data)
        stdin.close()
        for line in stdout.readlines():
            yield line.rstrip()
