import operator
import re
from typing import Iterable, List, Optional, Union

from ldb.typing import JMESPathValue, JSONBinFunc, JSONDecoded

NumVec = Union[int, float, List[Union[int, float]]]
Arg = Union[JMESPathValue, NumVec]


def regex(string: str, pattern: str) -> bool:
    return re.search(pattern, string) is not None


def regex_match(string: str, pattern: str) -> Optional[str]:
    match = re.search(pattern, string)
    if match is None:
        return None
    return match.group()


def apply_bin_op(
    op: JSONBinFunc,
    x1: JMESPathValue,
    x2: JMESPathValue,
) -> Union[JMESPathValue, NumVec]:
    if isinstance(x1, Iterable):
        if isinstance(x2, Iterable):
            return [op(x1_j, x2_j) for x1_j, x2_j in zip(x1, x2)]
        return [op(x1_j, x2) for x1_j in x1]
    if isinstance(x2, Iterable):
        return [op(x1, x2_j) for x2_j in x2]
    return op(x1, x2)


def add(x1: NumVec, x2: NumVec) -> NumVec:
    return apply_bin_op(operator.add, x1, x2)  # type: ignore[return-value]


def sub(x1: NumVec, x2: NumVec) -> NumVec:
    return apply_bin_op(operator.sub, x1, x2)  # type: ignore[return-value]


def mul(x1: NumVec, x2: NumVec) -> NumVec:
    return apply_bin_op(operator.mul, x1, x2)  # type: ignore[return-value]


def div(x1: NumVec, x2: NumVec) -> NumVec:
    return apply_bin_op(operator.truediv, x1, x2)  # type: ignore[return-value]


def neg(x: NumVec) -> NumVec:
    if isinstance(x, Iterable):
        return [-x_i for x_i in x]
    return -x


def contains_all(
    subject: Union[List[JSONDecoded], str],
    searches: List[JSONDecoded],
) -> bool:
    return all(s in subject for s in searches)


def contains_any(
    subject: Union[List[JSONDecoded], str],
    searches: List[JSONDecoded],
) -> bool:
    return any(s in subject for s in searches)


def parse_identifier_expression(key: str) -> List[str]:
    # TODO: use jmespath.lexer.Lexer().tokenize
    return key.split(".")


def has_keys(value: JSONDecoded, *keys: str) -> bool:
    for key_exp in keys:
        node = value
        for key in parse_identifier_expression(key_exp):
            try:
                node = node[key]  # type: ignore[index,call-overload]
            except (KeyError, TypeError):
                return False
    return True


CUSTOM_FUNCTIONS = {
    "regex": (regex, ["string", "string"]),
    "regex_match": (regex_match, ["string", "string"]),
    "add": (add, ["number|array", "number|array"]),
    "sub": (sub, ["number|array", "number|array"]),
    "mul": (mul, ["number|array", "number|array"]),
    "div": (div, ["number|array", "number|array"]),
    "neg": (neg, ["number|array"]),
    "contains_all": (contains_all, ["array|string", "array"]),
    "contains_any": (contains_any, ["array|string", "array"]),
    "has_keys": (
        has_keys,
        ["boolean|array|object|null|string|number", "string", "*"],
    ),
}
