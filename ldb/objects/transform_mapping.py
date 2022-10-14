from collections import UserDict
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)

from ldb.utils import hash_data, json_dumps

if TYPE_CHECKING:
    BaseCollectionObject = UserDict[str, List[str]]
else:
    BaseCollectionObject = UserDict


class TransformMapping(BaseCollectionObject):
    def __init__(
        self,
        dict: Optional[  # pylint: disable=redefined-builtin
            Union[
                Mapping[str, List[str]],
                Iterable[Tuple[str, List[str]]],
            ]
        ] = None,
        /,
        **kwargs: Any,
    ) -> None:
        super().__init__(dict, **kwargs)
        self.bytes: bytes = b""
        self.oid: str = ""

    def digest(self) -> None:
        self.bytes = json_dumps(self.data).encode()
        self.oid = hash_data(self.bytes)
