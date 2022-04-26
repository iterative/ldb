from typing import Dict, List, Optional, Type

from jmespath import exceptions, lexer
from jmespath.parser import ParsedResult, Parser
from jmespath.visitor import Options, TreeInterpreter

from ldb.jmespath.exceptions import (
    IdentifierTypeException,
    InvalidIdentifierExpression,
    MissingIdentifierException,
)
from ldb.typing import JSONDecoded, JSONObject


class FieldValidatedTreeInterpreter(  # pylint: disable=abstract-method
    TreeInterpreter,
):
    def visit_field(
        self,
        node: Dict[str, JSONDecoded],
        value: JSONDecoded,
    ) -> JSONDecoded:
        key: str = node["value"]  # type: ignore[assignment]
        try:
            return value[key]  # type: ignore[index,call-overload]
        except TypeError as exc:
            raise IdentifierTypeException(key, value) from exc
        except KeyError as exc:
            raise MissingIdentifierException(key) from exc


class CustomParser(Parser):
    def __init__(
        self,
        lookahead: int = 2,
        tree_interpreter_class: Type[TreeInterpreter] = TreeInterpreter,
    ) -> None:
        self._tree_interpreter_class = tree_interpreter_class
        super().__init__(lookahead=lookahead)

    def _parse(self, expression: str) -> "CustomParsedResult":
        self.tokenizer = (
            lexer.Lexer().tokenize(  # type: ignore[func-returns-value]
                expression,
            )
        )
        self._tokens = list(self.tokenizer)
        self._index = 0
        parsed = self._expression(  # type: ignore[attr-defined]
            binding_power=0,
        )
        if not self._current_token() == "eof":  # type: ignore[attr-defined]
            t = self._lookahead_token(0)  # type: ignore[attr-defined]
            raise exceptions.ParseError(
                t["start"],
                t["value"],
                t["type"],
                f"Unexpected token: {t['value']!s}",
            )
        return CustomParsedResult(
            expression,
            parsed,
            tree_interpreter_class=self._tree_interpreter_class,
        )


class CustomParsedResult(ParsedResult):
    def __init__(
        self,
        expression: str,
        parsed: JSONObject,
        tree_interpreter_class: Type[TreeInterpreter] = TreeInterpreter,
    ) -> None:
        self._tree_interpreter_class = tree_interpreter_class
        super().__init__(expression, parsed)

    def search(
        self,
        value: JSONDecoded,
        options: Optional[Options] = None,
    ) -> JSONDecoded:
        interpreter = self._tree_interpreter_class(options)
        result: JSONDecoded = interpreter.visit(  # type: ignore[no-untyped-call] # noqa: E501
            self.parsed,
            value,
        )
        return result


def parse_identifier_expression(key: str) -> List[str]:
    tokens = list(
        lexer.Lexer().tokenize(key),  # type: ignore[func-returns-value]
    )
    even = True
    result = []
    for t in tokens[:-1]:
        if even:
            if not t["type"] in ("unquoted_identifier", "quoted_identifier"):
                raise InvalidIdentifierExpression(key)
            result.append(t["value"])
        elif not t["type"] == "dot":
            raise InvalidIdentifierExpression(key)
        even = not even
    return result
