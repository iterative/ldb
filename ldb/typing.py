from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    MutableMapping,
    Sequence,
    Tuple,
    Union,
)

from jmespath.visitor import _Expression as JMESPathExpression
from typing_extensions import Protocol


class SupportsHash(Protocol):
    def __hash__(self) -> int:
        ...


JSONKey = Union[str, int, float, bool, None]
JSONObject = Dict[JSONKey, Any]
JSONArray = List[Any]
# possible return values of `json.decoder.JSONDecoder`
# `Any` should be `JSONDecoded`, but mypy doesn't support cyclic definitions
JSONDecoded = Union[str, int, float, bool, None, JSONObject, JSONArray]
JMESPathValue = Union[JSONDecoded, JMESPathExpression]


class JSONFunc(Protocol):
    def __call__(self, *args: JMESPathValue) -> JMESPathValue:
        ...


class JSONInstanceFunc(Protocol):
    def __call__(self, self_: Any, *args: JMESPathValue) -> JMESPathValue:
        ...


JSONArgTypes = Sequence[str]
JSONFuncDef = Tuple[JSONFunc, JSONArgTypes]
JSONFuncMapping = Mapping[str, JSONFuncDef]
JSONFuncMutableMapping = MutableMapping[str, JSONFuncDef]
JSONBinFunc = Callable[[JMESPathValue, JMESPathValue], JSONDecoded]
