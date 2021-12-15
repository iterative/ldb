from typing import (
    Any,
    Dict,
    List,
    Mapping,
    MutableMapping,
    Sequence,
    Tuple,
    Union,
)

from typing_extensions import Protocol

JSONKey = Union[str, int, float, bool, None]
# possible return values of `json.decoder.JSONDecoder`
# `Any` should be `JSONDecoded`, but mypy doesn't support cyclic definitions
JSONDecoded = Union[Dict[JSONKey, Any], List[Any], str, int, float, bool, None]


class JSONFunc(Protocol):
    def __call__(self, *args: JSONDecoded) -> JSONDecoded:
        ...


class JSONInstanceFunc(Protocol):
    def __call__(self, self_: Any, *args: JSONDecoded) -> JSONDecoded:
        ...


JSONArgTypes = Sequence[str]
JSONFuncDef = Tuple[JSONFunc, JSONArgTypes]
JSONFuncMapping = Mapping[str, JSONFuncDef]
JSONFuncMutableMapping = MutableMapping[str, JSONFuncDef]
