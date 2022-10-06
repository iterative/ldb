from abc import abstractmethod
from itertools import tee
from typing import (
    TYPE_CHECKING,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    cast,
)

from ldb.typing import JSONDecoded

if TYPE_CHECKING:
    from ldb.index.utils import AnnotationMeta
    from ldb.index.utils import DataObjectMeta as DataObjectMetaT
    from ldb.objects.annotation import Annotation

DataObjectMetaRecord = Tuple[str, "DataObjectMetaT"]
AnnotationRecord = Tuple[str, JSONDecoded]
DataObjectAnnotationRecord = Tuple[str, str, "AnnotationMeta"]
DatasetRecord = Tuple[int, str]


class AbstractDB:
    def __init__(self, path: str) -> None:
        self.path: str = path
        self.data_object_meta_list: List[DataObjectMetaRecord] = []
        self.annotation_list: List["Annotation"] = []
        self.data_object_annotation_list: List[DataObjectAnnotationRecord] = []
        self.dataset_set: Set[str] = set()
        self.dataset_member_by_name_list: List[Tuple[str, str, str]] = []

    @abstractmethod
    def init(self) -> None:
        ...

    def write_all(self) -> None:
        self.write_data_object_meta()
        self.write_annotation()
        self.write_data_object_annotation()
        self.write_dataset()
        self.write_dataset_member_by_name()

    def add_pair(
        self,
        data_object_hash: str,
        data_object_meta: "DataObjectMetaT",
        annotation: Optional["Annotation"] = None,
        annotation_meta: Optional["AnnotationMeta"] = None,
    ) -> None:
        self.add_data_object_meta(data_object_hash, data_object_meta)
        if annotation is not None:
            if annotation_meta is None:
                raise ValueError(
                    "If annotation is not None, then annotation_meta cannot be None"
                )
            self.add_annotation(annotation)
            self.add_pair_meta(
                data_object_hash,
                annotation.oid,
                annotation_meta,
            )
            self.set_current_annot(data_object_hash, annotation.oid)

    def add_data_object_meta(self, id: str, value: "DataObjectMetaT") -> None:
        self.data_object_meta_list.append((id, value))

    @abstractmethod
    def write_data_object_meta(self) -> None:
        ...

    @abstractmethod
    def get_data_object_meta(self, id: str) -> Optional[DataObjectMetaRecord]:
        ...

    @abstractmethod
    def get_data_object_meta_many(self, ids: Iterable[str]) -> Iterable[DataObjectMetaRecord]:
        ...

    @abstractmethod
    def get_data_object_meta_all(self) -> Iterable[DataObjectMetaRecord]:
        ...

    def add_annotation(self, annotation: "Annotation") -> None:
        if not annotation.oid:
            raise ValueError(f"Invalid Annotation oid: {annotation.oid}")
        self.annotation_list.append(annotation)

    @abstractmethod
    def write_annotation(self) -> None:
        ...

    @abstractmethod
    def get_annotation(self, id: str) -> Optional[AnnotationRecord]:
        ...

    @abstractmethod
    def get_annotation_many(self, ids: Iterable[str]) -> Iterable[AnnotationRecord]:
        ...

    @abstractmethod
    def get_annotation_all(self) -> Iterable[AnnotationRecord]:
        ...

    def add_pair_meta(self, id: str, annot_id: str, value: "AnnotationMeta") -> None:
        self.data_object_annotation_list.append(
            (id, annot_id, value),
        )

    @abstractmethod
    def write_data_object_annotation(self) -> None:
        ...

    @abstractmethod
    def get_pair_meta(self, id: str, annot_id: str) -> Optional[DataObjectAnnotationRecord]:
        ...

    @abstractmethod
    def get_pair_meta_many(
        self,
        collection: Iterable[Tuple[str, Optional[str]]],
    ) -> Iterable[DataObjectAnnotationRecord]:
        ...

    @abstractmethod
    def get_pair_meta_all(self) -> Iterable[DataObjectAnnotationRecord]:
        ...

    @abstractmethod
    def count_pairs(self, id: str) -> int:
        ...

    def jp_search_pair_meta(
        self,
        query: str,
        collection: Iterable[Tuple[str, Optional[str]]],
    ) -> Iterator[Tuple[str, str, JSONDecoded]]:
        from ldb.query.search import get_search_func

        search = get_search_func(query)
        data = self.get_pair_meta_many(collection)
        iter1, iter2 = tee(data)
        values = cast(Iterable[JSONDecoded], (v for _, _, v in iter1))
        for (data_object_id, annotation_id, _), result in zip(
            iter2,
            search(values),
        ):
            yield data_object_id, annotation_id, result

    def add_dataset(self, name: str) -> None:
        self.dataset_set.add(name)

    @abstractmethod
    def write_dataset(self) -> None:
        ...

    def add_dataset_member_by_name(
        self,
        name: str,
        data_object_id: str,
        annotation_id: str,
    ) -> None:
        self.dataset_member_by_name_list.append(
            (name, data_object_id, annotation_id),
        )

    @abstractmethod
    def write_dataset_member_by_name(self) -> None:
        ...

    @abstractmethod
    def get_dataset_member_many(self, dataset_name: str) -> Iterable[Tuple[str, str]]:
        ...

    def get_root_collection(self) -> Iterable[Tuple[str, str]]:
        return self.get_dataset_member_many("root")

    def set_current_annot(self, id: str, annot_id: str) -> None:
        self.add_dataset_member_by_name("root", id, annot_id)

    @abstractmethod
    def ls_collection(
        self, collection: Iterable[Tuple[str, Optional[str]]]
    ) -> Iterable[Tuple[str, str, str, int]]:
        ...
