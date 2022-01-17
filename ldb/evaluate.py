from itertools import tee
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Sequence, Tuple, Union

from ldb.add import process_args_for_ls
from ldb.dataset import apply_queries, get_annotations, get_data_object_metas
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
    collection_ops: Iterable[Tuple[str, str]],
) -> Iterator[EvaluateResult]:
    annotation_query, file_query, collection_ops = process_query_args(
        collection_ops,
    )
    search = apply_optional(get_search_func, annotation_query)
    file_search = apply_optional(get_search_func, file_query)
    data_object_hashes, annotation_hashes, _ = process_args_for_ls(
        ldb_dir,
        paths,
    )
    collection = apply_queries(
        ldb_dir,
        data_object_hashes,
        annotation_hashes,
        collection_ops,
    )
    collection1, collection2 = tee(collection)
    data_object_hashes1, data_object_hashes2 = tee(d for d, _ in collection1)
    annotation_hashes = (a for _, a in collection2)
    if search is None and file_search is None:
        search_results = [get_annotations(ldb_dir, annotation_hashes)]
    else:
        search_results = []
        if file_search is not None:
            search_results.append(
                file_search(
                    get_data_object_metas(ldb_dir, data_object_hashes1),
                ),
            )
        if search is not None:
            search_results.append(
                search(get_annotations(ldb_dir, annotation_hashes)),
            )

    result: Iterator[EvaluateResult] = zip(  # type: ignore[assignment]
        data_object_hashes2,
        *search_results,
    )
    yield from result


def process_query_args(
    query_args: Iterable[Tuple[str, str]],
) -> Tuple[Optional[str], Optional[str], List[Tuple[str, str]]]:
    query_args = list(query_args)
    search_args = {}
    possible_search_op_types = {OpType.ANNOTATION_QUERY, OpType.FILE_QUERY}
    while query_args and possible_search_op_types:
        op_type, arg = query_args[-1]
        if op_type in possible_search_op_types:
            query_args.pop()
            search_args[op_type] = arg
            possible_search_op_types.remove(op_type)
        else:
            break
    return (
        search_args.get(OpType.ANNOTATION_QUERY),
        search_args.get(OpType.FILE_QUERY),
        query_args,
    )
