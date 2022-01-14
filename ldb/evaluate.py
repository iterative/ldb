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
    annotation_query_args = []
    file_query_args = []
    other_query_args = []
    for op_type, arg in query_args:
        if op_type == OpType.ANNOTATION_QUERY:
            annotation_query_args.append(arg)
        elif op_type == OpType.FILE_QUERY:
            file_query_args.append(arg)
        elif op_type == OpType.LIMIT:
            other_query_args.append((op_type, arg))
        else:
            raise ValueError(
                f"Invalid op type for evaluate command: {op_type}",
            )
    if len(annotation_query_args) > 1:
        raise ValueError("--query may only be used once")
    if len(file_query_args) > 1:
        raise ValueError("--file may only be used once")
    annot_query = annotation_query_args[0] if annotation_query_args else None
    file_query = file_query_args[0] if file_query_args else None
    return annot_query, file_query, other_query_args
