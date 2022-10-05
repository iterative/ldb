import json
from abc import abstractmethod
from itertools import tee
from typing import TYPE_CHECKING, Dict, Iterable, Iterator, Optional, Tuple

from ldb.typing import JSONDecoded

if TYPE_CHECKING:
    from ldb.index.utils import AnnotationMeta
    from ldb.index.utils import DataObjectMeta as DataObjectMetaT
    from ldb.objects.annotation import Annotation


class AbstractDB:
    def __init__(self, path: str) -> None:
        self.path = path
        self.data_object_meta_list = []
        self.annotation_list = []
        self.data_object_annotation_list = []
        self.dataset_set = set()
        self.dataset_member_by_name_list = []

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
            self.add_annotation(annotation)
            self.add_pair_meta(
                data_object_hash,
                annotation.oid,
                annotation_meta,
            )
            self.set_current_annot(data_object_hash, annotation.oid)

    def add_data_object_meta(self, id: str, value: "DataObjectMetaT") -> None:
        self.data_object_meta_list.append((id, json.dumps(value)))

    @abstractmethod
    def write_data_object_meta(self) -> None:
        ...

    @abstractmethod
    def get_data_object_meta(self, id: str) -> None:
        ...

    @abstractmethod
    def get_data_object_meta_many(self, ids: Iterable[str]):
        ...

    def get_data_object_meta_all(self):
        ...

    def add_annotation(self, annotation: "Annotation") -> None:
        if not annotation.oid:
            raise ValueError(f"Invalid Annotation oid: {annotation.oid}")
        self.annotation_list.append((annotation.oid, annotation.value_str))

    @abstractmethod
    def write_annotation(self) -> None:
        ...

    @abstractmethod
    def get_annotation(self, id: str) -> Tuple[str, JSONDecoded]:
        ...

    @abstractmethod
    def get_annotation_many(self, ids: Iterable[str]) -> Iterator[Tuple[str, JSONDecoded]]:
        ...

    @abstractmethod
    def get_annotation_all(self) -> Iterator[Tuple[str, JSONDecoded]]:
        ...

    def add_pair_meta(self, id: str, annot_id: str, value: JSONDecoded) -> None:
        self.data_object_annotation_list.append(
            (id, annot_id, value),
        )

    @abstractmethod
    def write_data_object_annotation(self) -> None:
        ...

    @abstractmethod
    def get_pair_meta(self, id: str, annot_id: str) -> JSONDecoded:
        ...

    @abstractmethod
    def get_pair_meta_many(
        self,
        collection: Iterable[Tuple[str, Optional[str]]],
    ):
        ...

    @abstractmethod
    def get_pair_meta_all(self) -> Iterator[Tuple[str, str, JSONDecoded]]:
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
        values = (v for _, _, v in iter1)
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
    def get_dataset_member_many(self, dataset_name: str) -> Iterator[Tuple[str, str]]:
        ...

    def get_root_collection(self) -> Dict[str, str]:
        return dict(self.get_dataset_member_many("root"))

    def set_current_annot(self, id: str, annot_id: str) -> None:
        self.add_dataset_member_by_name("root", id, annot_id)

    @abstractmethod
    def ls_collection(
        self, collection: Iterable[Tuple[str, Optional[str]]]
    ) -> Iterator[Tuple[str, str, int, int]]:
        ...
