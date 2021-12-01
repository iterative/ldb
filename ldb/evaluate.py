from pathlib import Path
from typing import Any, Iterator, List, Optional, Sequence, Tuple, Union

from ldb.add import process_args_for_ls
from ldb.dataset import get_annotations, get_data_object_meta
from ldb.query import get_search_func

QuerySubject = Union[List[str], List[Optional[str]]]


def evaluate(
    ldb_dir: Path,
    query_str: str,
    paths: Sequence[str],
    use_file_attributes: bool = False,
) -> Iterator[Tuple[str, Any]]:
    search = get_search_func(query_str)
    data_object_hashes, annotation_hashes, _ = process_args_for_ls(
        ldb_dir,
        paths,
    )
    if use_file_attributes:
        query_subject: QuerySubject = get_data_object_meta(
            ldb_dir,
            data_object_hashes,
        )
    else:
        query_subject = get_annotations(ldb_dir, annotation_hashes)
    query_results = search(query_subject)
    yield from zip(data_object_hashes, query_results)
