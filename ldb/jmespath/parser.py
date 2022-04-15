from typing import Any, Dict, Optional, Type

from jmespath import exceptions, lexer
from jmespath.parser import ParsedResult, Parser
from jmespath.visitor import Options, TreeInterpreter

from ldb.typing import JSONDecoded, JSONObject


class IdentifierException(exceptions.JMESPathError):
    def __init__(  # pylint: disable=super-init-not-called
        self,
        identifier: str,
    ) -> None:
        self.identifier = identifier

    def __str__(self) -> str:
        return f"{self.identifier}"


class MissingIdentifierException(IdentifierException):
    pass


class IdentifierTypeException(IdentifierException):
    def __init__(self, identifier: str, subject: Any) -> None:
        super().__init__(identifier)
        self.subject = subject

    def __str__(self) -> str:
        return (
            f"Invalid application of identifier {self.identifier}. "
            "Identifier expression subject must be a dict, not "
            f"{type(self.subject)}"
        )


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
