from collections import UserDict
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Mapping,
    Optional,
    Tuple,
    Union,
)

from ldb.utils import hash_data, json_dumps

if TYPE_CHECKING:
    BaseCollectionObject = UserDict[str, Optional[str]]
else:
    BaseCollectionObject = UserDict


class CollectionObject(BaseCollectionObject):
    def __init__(
        self,
        dict: Optional[  # pylint: disable=redefined-builtin
            Union[
                Mapping[str, Optional[str]],
                Iterable[Tuple[str, Optional[str]]],
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
