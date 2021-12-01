from typing import Optional

from jmespath.parser import ParsedResult
from jmespath.visitor import Options

from ldb.typing import JSONDecoded

def compile(expression: str) -> ParsedResult: ...
def search(
    expression: str, data: JSONDecoded, options: Optional[Options] = ...
) -> JSONDecoded: ...
