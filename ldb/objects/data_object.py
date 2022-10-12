from ldb.typing import JSONObject


class DataObjectMeta:
    def __init__(
        self,
        oid: str,
        data: JSONObject,
    ):
        self.oid = oid
        self.data = data


class PairMeta:
    def __init__(
        self,
        oid: str,
        annot_oid: str,
        data: JSONObject,
    ):
        self.oid = oid
        self.annot_oid = annot_oid
        self.data = data
