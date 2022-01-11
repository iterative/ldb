from abc import ABC
from pathlib import Path
from typing import Dict, Iterator, Mapping, TypeVar

KT = TypeVar("KT")
VT = TypeVar("VT")


class MappingCache(ABC, Mapping[KT, VT]):
    def __init__(self) -> None:
        self._cache: Dict[KT, VT] = {}

    def __getitem__(self, key: KT) -> VT:
        try:
            return self._cache[key]
        except KeyError:
            value = self.get_new(key)
            self._cache[key] = value
            return value

    def get_new(self, key: KT) -> VT:
        raise NotImplementedError

    def __iter__(self) -> Iterator[KT]:
        return iter(self._cache)

    def __len__(self) -> int:
        return len(self._cache)


class LDBMappingCache(MappingCache[KT, VT]):
    def __init__(self, ldb_dir: Path) -> None:
        super().__init__()
        self.ldb_dir = ldb_dir
