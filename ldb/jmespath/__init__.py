from jmespath.parser import ParsedResult

from ldb.jmespath.parser import CustomParser, FieldValidatedTreeInterpreter


def jp_compile(expression: str) -> ParsedResult:
    return CustomParser().parse(expression)


def custom_compile(expression: str) -> ParsedResult:
    return CustomParser(
        tree_interpreter_class=FieldValidatedTreeInterpreter,
    ).parse(expression)
