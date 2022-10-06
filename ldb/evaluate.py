from pathlib import Path
from typing import (
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

from ldb.add import process_args_for_ls
from ldb.core import LDBClient
from ldb.dataset import PipelineData, apply_queries
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
    show_ops: Iterable[Tuple[str, str]],
    warn: bool = True,
) -> Iterator[EvaluateResult]:
    client = LDBClient(ldb_dir)
    searches, file_searches = process_show_args(show_ops)
    (
        data_object_hashes,
        annotation_hashes,
        _,
        _,
    ) = process_args_for_ls(  # pylint: disable=duplicate-code
        ldb_dir,
        paths,
    )
    data_object_hashes = list(data_object_hashes)
    annotation_hashes = list(annotation_hashes)
    data = PipelineData(client.db, data_object_hashes, annotation_hashes)
    collection = apply_queries(
        ldb_dir,
        data_object_hashes,
        annotation_hashes,
        collection_ops,
        data=data,
        warn=warn,
    )
    collection_list = list(collection)
    annotation_hashes = (a for _, a in collection_list)
    search_results: List[Iterable[JSONDecoded]]
    if not searches and not file_searches:
        search_results = [[data.annotations.get(i) for i in annotation_hashes]]
    else:
        search_results = []

        if file_searches:
            data_object_hashes = [d for d, _ in collection_list]
            data_object_metas = cast(
                List[JSONDecoded], [data.data_object_metas[d] for d in data_object_hashes]
            )
            for file_search in file_searches:
                search_results.append(file_search(data_object_metas))

        if searches:
            annotations = [data.annotations.get(i) for i in annotation_hashes]
            for search in searches:
                search_results.append(search(annotations))

    result: Iterator[EvaluateResult] = zip(  # type: ignore[assignment]
        (d for d, _ in collection_list),
        *search_results,
    )
    yield from result


def process_show_args(
    show_args: Iterable[Tuple[str, str]],
    warn: bool = True,
) -> Tuple[List[SearchFunc], List[SearchFunc]]:
    show_args = list(show_args)
    searches: List[SearchFunc] = []
    file_searches: List[SearchFunc] = []
    if not show_args:
        return searches, file_searches
    for op_type, arg in show_args:
        if op_type == OpType.ANNOTATION_QUERY:
            searches.append(get_search_func(arg, use_custom=True, warn=warn))
        elif op_type == OpType.JP_ANNOTATION_QUERY:
            searches.append(get_search_func(arg, use_custom=False, warn=warn))
        elif op_type == OpType.FILE_QUERY:
            file_searches.append(get_search_func(arg, warn=warn))
    return searches, file_searches
