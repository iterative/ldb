from ldb.objects.annotation import Annotation
from ldb.index.utils import AnnotationMeta, DataObjectMeta as DataObjectMetaT


class AbstractDB:
    def add_pair(
        self,
        data_object_hash: str,
        data_object_meta: DataObjectMetaT,
        annotation: Annotation = None,
        annotation_meta: AnnotationMeta = None,
    ) -> None:
        raise NotImplementedError
