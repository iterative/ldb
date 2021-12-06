from typing import Any, Dict, Optional

from jmespath.visitor import Options

from ldb.typing import JSONDecoded

class Parser:
    BINDING_POWER: Dict[str, int]
    tokenizer: Any
    def __init__(self, lookahead: int = ...) -> None: ...
    def parse(self, expression: str) -> ParsedResult: ...
    @classmethod
    def purge(cls) -> None: ...

class ParsedResult:
    expression: str
    parsed: Any
    def __init__(self, expression: str, parsed: Any) -> None: ...
    def search(
        self, value: JSONDecoded, options: Optional[Options] = ...
    ) -> JSONDecoded: ...
