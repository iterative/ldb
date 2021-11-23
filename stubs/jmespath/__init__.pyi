from typing import Any

from jmespath import parser as parser
from jmespath.visitor import Options as Options

def compile(expression: Any) -> parser.ParsedResult: ...
def search(expression: str, data: Any, options: Any | None = ...) -> Any: ...
