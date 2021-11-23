import json
from itertools import repeat
from pathlib import Path
from typing import Any, Generator, Iterable, List, Sequence, Tuple

from ldb.add import ArgType, get_arg_type, process_args_for_ls
from ldb.path import InstanceDir
from ldb.query import query
from ldb.utils import get_hash_path


def evaluate(
    ldb_dir: Path,
    query_str: str,
    paths: Sequence[str],
) -> Generator[Tuple[str, Any], None, None]:
    if not paths:
        paths = ["."]
        arg_type = ArgType.WORKSPACE_DATASET
    else:
        arg_type = get_arg_type(paths)

    data_object_hashes, annotation_hashes, _ = process_args_for_ls(
        ldb_dir,
        arg_type,
        paths,
    )

    if annotation_hashes is None:
        query_results: Iterable[Any] = repeat(None)
    else:
        query_results = query(
            query_str,
            get_annotations(ldb_dir, annotation_hashes),
        )
    yield from zip(data_object_hashes, query_results)


def get_annotations(
    ldb_dir: Path,
    annotation_hashes: Iterable[str],
) -> List[str]:
    annotations = []
    for annotation_hash in annotation_hashes:
        if annotation_hash:
            user_annotation_file_path = (
                get_hash_path(
                    ldb_dir / InstanceDir.ANNOTATIONS,
                    annotation_hash,
                )
                / "user"
            )
            annotations.append(
                json.loads(user_annotation_file_path.read_text()),
            )
        else:
            annotations.append(None)
    return annotations
