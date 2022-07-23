from functools import partial
from itertools import tee
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Sequence, Tuple, Union

from ldb.add import process_args_for_ls
from ldb.dataset import apply_queries, get_annotations, get_data_object_metas
from ldb.func_utils import apply_optional
from ldb.op_type import OpType
from ldb.query.search import SearchFunc, get_search_func
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
    warn: bool = True,
) -> Iterator[EvaluateResult]:
    search, file_search, collection_ops = process_query_args(
        collection_ops,
    )
    (
        data_object_hashes,
        annotation_hashes,
        _,
        _,
    ) = process_args_for_ls(  # pylint: disable=duplicate-code
        ldb_dir,
        paths,
    )
    collection = apply_queries(
        ldb_dir,
        data_object_hashes,
        annotation_hashes,
        collection_ops,
        warn=warn,
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
                file_search(  # type: ignore[arg-type]
                    get_data_object_metas(ldb_dir, data_object_hashes1),
                ),
            )
        if search is not None:
            search_results.append(
                search(get_annotations(ldb_dir, annotation_hashes)),  # type: ignore[arg-type] # noqa: E501
            )

    result: Iterator[EvaluateResult] = zip(  # type: ignore[assignment]
        data_object_hashes2,
        *search_results,
    )
    yield from result


def process_query_args(
    query_args: Iterable[Tuple[str, str]],
    warn: bool = True,
) -> Tuple[Optional[SearchFunc], Optional[SearchFunc], List[Tuple[str, str]]]:
    query_args = list(query_args)
    search = None
    file_search = None
    if not query_args:
        return search, file_search, query_args
    op_type, arg = query_args.pop()
    if op_type == OpType.ANNOTATION_QUERY:
        search = apply_optional(
            partial(get_search_func, use_custom=True, warn=warn),
            arg,
        )
    elif op_type == OpType.JP_ANNOTATION_QUERY:
        search = apply_optional(
            partial(get_search_func, use_custom=False, warn=warn),
            arg,
        )
    elif op_type == OpType.FILE_QUERY:
        file_search = apply_optional(
            partial(get_search_func, warn=warn),
            arg,
        )
    else:
        query_args.append((op_type, arg))
    return search, file_search, query_args
