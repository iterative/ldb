from pathlib import Path
from typing import Any, Iterator, Sequence, Tuple

from ldb.add import ArgType, get_arg_type, process_args_for_ls
from ldb.dataset import get_annotations
from ldb.query import get_search_func


def evaluate(
    ldb_dir: Path,
    query_str: str,
    paths: Sequence[str],
) -> Iterator[Tuple[str, Any]]:
    search = get_search_func(query_str)
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

    query_results = search(get_annotations(ldb_dir, annotation_hashes))
    yield from zip(data_object_hashes, query_results)
