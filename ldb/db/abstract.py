from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ldb.index.utils import AnnotationMeta
    from ldb.index.utils import DataObjectMeta as DataObjectMetaT
    from ldb.objects.annotation import Annotation


class AbstractDB:
    def add_pair(
        self,
        data_object_hash: str,
        data_object_meta: "DataObjectMetaT",
        annotation: "Annotation" = None,
        annotation_meta: "AnnotationMeta" = None,
    ) -> None:
        raise NotImplementedError
