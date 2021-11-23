from typing import Any, Callable, Generator, Iterable

import jmespath


def get_search_func(
    query_str: str,
) -> Callable[[Iterable[Any]], Generator[Any, None, None]]:
    """Compile `query_str` and return a search function."""
    query_obj = jmespath.compile(query_str)

    def search(objects: Iterable[Any]) -> Generator[Any, None, None]:
        for obj in objects:
            yield query_obj.search(obj)

    return search
