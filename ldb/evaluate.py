from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Sequence, Tuple, Union

from ldb.add import process_args_for_delete, process_args_for_ls
from ldb.dataset import get_annotations, get_data_object_metas
from ldb.func_utils import apply_optional
from ldb.op_type import OpType
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
            file_search(get_data_object_metas(ldb_dir, data_object_hashes)),
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
                        get_data_object_metas(ldb_dir, data_object_hashes),
                    ),
                )
            if search is not None:
                search_results.append(
                    search(get_annotations(ldb_dir, annotation_hashes)),
                )
    yield from zip(data_object_hashes, *search_results)  # type: ignore[misc]


def process_query_args(
    query_args: Iterable[Tuple[str, str]],
) -> Tuple[Optional[str], Optional[str]]:
    annotation_query_args = []
    file_query_args = []
    for op_type, arg in query_args:
        if op_type == OpType.ANNOTATION_QUERY:
            annotation_query_args.append(arg)
        elif op_type == OpType.FILE_QUERY:
            file_query_args.append(arg)
    if len(annotation_query_args) > 1:
        raise ValueError("--query may only be used once")
    if len(file_query_args) > 1:
        raise ValueError("--file may only be used once")
    annot_query = annotation_query_args[0] if annotation_query_args else None
    file_query = file_query_args[0] if file_query_args else None
    return annot_query, file_query
