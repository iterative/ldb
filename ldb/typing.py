from typing import Any, Dict, List, Union

JSONKey = Union[str, int, float, bool, None]
# possible return values of `json.decoder.JSONDecoder`
# `Any` should be `JSONDecoded`, but mypy doesn't support cyclic definitions
JSONDecoded = Union[Dict[JSONKey, Any], List[Any], str, int, float, bool, None]
