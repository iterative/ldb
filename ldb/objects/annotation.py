from dataclasses import dataclass

from ldb.typing import JSONDecoded, JSONObject
from ldb.utils import hash_data, json_dumps


@dataclass
class Annotation:
    value: JSONDecoded
    meta: JSONObject
    value_str: str = ""
    value_bytes: bytes = b""
    meta_bytes: bytes = b""
    bytes: bytes = b""
    oid: str = ""

    def digest(self) -> None:
        self.value_str = json_dumps(self.value)
        self.value_bytes = self.value_str.encode()
        self.meta_bytes = json_dumps(self.meta).encode()
        # TODO use just self.value_bytes
        # self.bytes = self.meta_bytes + self.value_bytes
        # self.bytes = self.value_bytes
        self.bytes = self.meta_bytes + self.value_bytes
        self.oid = hash_data(self.bytes)
