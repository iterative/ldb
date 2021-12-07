import re
from typing import Optional


def regex(string: str, pattern: str) -> bool:
    return re.search(pattern, string) is not None


def regex_match(string: str, pattern: str) -> Optional[str]:
    match = re.search(pattern, string)
    if match is None:
        return None
    return match.group()


CUSTOM_FUNCTIONS = {
    "regex": (regex, ["string", "string"]),
    "regex_match": (regex_match, ["string", "string"]),
}
