from typing import Tuple

from dvc_objects import db


class ObjectDB(db.ObjectDB):  # type: ignore[misc]
    def _oid_parts(self, oid: str) -> Tuple[str, ...]:
        return oid[:3], oid[3:]
