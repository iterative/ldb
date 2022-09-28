import duckdb

from ldb.db.abstract import AbstractDB
from ldb.objects.annotation import Annotation
from ldb.index.utils import AnnotationMeta, DataObjectMeta as DataObjectMetaT


class DuckDB(AbstractDB):
    def __init__(self, path: str):
        self.path = path
        self.conn = duckdb.connect(path)

    def add_pair(
        self,
        data_object_hash: str,
        data_object_meta: DataObjectMetaT,
        annotation: Annotation = None,
        annotation_meta: AnnotationMeta = None,
    ) -> None:
        pass
