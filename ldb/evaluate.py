from pathlib import Path
from typing import Any, Iterator, Sequence, Tuple

from ldb.add import process_args_for_ls
from ldb.dataset import get_annotations
from ldb.query import get_search_func


def evaluate(
    ldb_dir: Path,
    query_str: str,
    paths: Sequence[str],
) -> Iterator[Tuple[str, Any]]:
    search = get_search_func(query_str)
    data_object_hashes, annotation_hashes, _ = process_args_for_ls(
        ldb_dir,
        paths,
    )
    query_results = search(get_annotations(ldb_dir, annotation_hashes))
    yield from zip(data_object_hashes, query_results)
