from typing import Iterable, Iterator, TypeVar

T = TypeVar("T")


def take(iterable: Iterable[T], n: int = 1) -> Iterator[T]:
    for i, item in enumerate(iterable):
        if i < n:
            yield item
