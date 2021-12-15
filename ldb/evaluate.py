from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Sequence, Tuple, Union

from ldb.add import process_args_for_delete, process_args_for_ls
from ldb.dataset import get_annotations, get_data_object_meta
from ldb.func_utils import apply_optional
from ldb.query.search import get_search_func
from ldb.typing import JSONDecoded

EvaluateResult = Union[
    Tuple[str, JSONDecoded],
    Tuple[str, JSONDecoded, JSONDecoded],
]
QuerySubject = Union[List[str], List[Optional[str]]]


def evaluate(
    ldb_dir: Path,
    paths: Sequence[str],
    annotation_query: Optional[str] = None,
    file_query: Optional[str] = None,
) -> Iterator[EvaluateResult]:
    search = apply_optional(get_search_func, annotation_query)
    file_search = apply_optional(get_search_func, file_query)
    if search is None and file_search is not None:
        data_object_hashes: Iterable[str] = sorted(
            process_args_for_delete(
                ldb_dir,
                paths,
            ),
        )
        search_results: List[Iterable[JSONDecoded]] = [
            file_search(get_data_object_meta(ldb_dir, data_object_hashes)),
        ]
    else:
        data_object_hashes, annotation_hashes, _ = process_args_for_ls(
            ldb_dir,
            paths,
        )
        if search is None and file_search is None:
            search_results = [get_annotations(ldb_dir, annotation_hashes)]
        else:
            search_results = []
            if file_search is not None:
                search_results.append(
                    file_search(
                        get_data_object_meta(ldb_dir, data_object_hashes),
                    ),
                )
            if search is not None:
                search_results.append(
                    search(get_annotations(ldb_dir, annotation_hashes)),
                )
    yield from zip(data_object_hashes, *search_results)  # type: ignore[misc]
