import json
from itertools import tee
from typing import TYPE_CHECKING, Iterable, Optional, Tuple

import pandas as pd

import duckdb
from ldb.db.abstract import AbstractDB
from ldb.objects.annotation import Annotation
from ldb.utils import json_dumps

if TYPE_CHECKING:
    from ldb.index.utils import AnnotationMeta
    from ldb.index.utils import DataObjectMeta as DataObjectMetaT


class DuckDB(AbstractDB):
    def __init__(self, path: str):
        self.path = path
        self.conn = duckdb.connect(path)

        self.data_object_meta_list = []
        self.annotation_list = []
        self.data_object_annotation_list = []
        self.dataset_set = set()
        self.dataset_member_by_name_list = []

    def init(self):
        self.conn.begin()
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS data_object_meta(id VARCHAR PRIMARY KEY, value JSON)"
        )
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS annotation(id VARCHAR PRIMARY KEY, value JSON)"
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS data_object_annotation(
                data_object_id VARCHAR,
                annotation_id VARCHAR,
                value JSON,
                -- PRIMARY KEY (data_object_id, annotation_id),
                FOREIGN KEY (data_object_id) REFERENCES data_object_meta(id),
                FOREIGN KEY (annotation_id) REFERENCES annotation(id)
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dataset(id INTEGER PRIMARY KEY, name VARCHAR UNIQUE)
            """
        )
        # The primary key is commented out for now due to lack of proper update support
        # https://github.com/duckdb/duckdb/issues/3265#issuecomment-1090012268
        # TODO see if there's a workaround, such as deleting, then inserting
        # https://github.com/duckdb/duckdb/issues/61
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dataset_member(
                dataset_id INTEGER,
                data_object_id VARCHAR,
                annotation_id VARCHAR,
                -- PRIMARY KEY (dataset_id, data_object_id),
                FOREIGN KEY (dataset_id) REFERENCES dataset(id),
                FOREIGN KEY (data_object_id) REFERENCES data_object_meta(id),
                FOREIGN KEY (annotation_id) REFERENCES annotation(id),
            )
            """
        )
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_dataset_id START 1")
        self.conn.commit()

        self.add_dataset("root")
        self.write_all()

    def write_all(self):
        self.write_data_object_meta()
        self.write_annotation()
        self.write_data_object_annotation()
        self.write_dataset()
        self.write_dataset_member_by_name()

    def write_data_object_meta(self):
        if self.data_object_meta_list:
            df = pd.DataFrame(self.data_object_meta_list, columns=["id", "value"])
            self.conn.register("data_object_meta_df", df)

            self.conn.begin()
            self.conn.execute(
                """
                UPDATE data_object_meta
                SET value = data_object_meta_df.value
                FROM data_object_meta_df
                WHERE data_object_meta.id = data_object_meta_df.id
                """
            )
            self.conn.execute(
                """
                INSERT INTO data_object_meta (
                    SELECT * from data_object_meta_df
                    WHERE id not in (SELECT id from data_object_meta)
                )
                """
            )
            self.conn.commit()

            self.conn.unregister("data_object_meta_df")
            self.data_object_meta_list = []

    def write_annotation(self):
        if self.annotation_list:
            df = pd.DataFrame(self.annotation_list, columns=["id", "value"])
            self.conn.register("annotation_df", df)

            self.conn.begin()
            self.conn.execute(
                """
                UPDATE annotation
                SET value = annotation_df.value
                FROM annotation_df
                WHERE annotation.id = annotation_df.id
                """
            )
            self.conn.execute(
                """
                INSERT INTO annotation (
                    SELECT * from annotation_df
                    WHERE id not in (SELECT id from annotation)
                )
                """
            )
            self.conn.commit()

            self.conn.unregister("annotation_df")
            self.annotation_list = []

    def write_data_object_annotation(self):
        if self.data_object_annotation_list:
            df = pd.DataFrame(self.data_object_annotation_list, columns=["data_object_id", "annotation_id", "value"])
            self.conn.register("data_object_annotation_df", df)

            self.conn.begin()
            self.conn.execute(
                """
                UPDATE data_object_annotation
                SET value = data_object_annotation_df.value
                FROM data_object_annotation_df
                WHERE
                    (data_object_annotation.data_object_id, data_object_annotation.annotation_id) = (data_object_annotation_df.data_object_id, data_object_annotation_df.annotation_id)
                """
            )
            self.conn.execute(
                """
                INSERT INTO data_object_annotation (
                    SELECT * from data_object_annotation_df
                    WHERE (data_object_id, annotation_id) not in (
                        SELECT (data_object_id, annotation_id) from data_object_annotation
                    )
                )
                """
            )
            self.conn.commit()

            self.conn.unregister("data_object_annotation_df")
            self.data_object_annotation_list = []

    def write_dataset(self):
        if self.dataset_set:
            self.conn.begin()
            for name in self.dataset_set:
                try:
                    self.conn.execute("INSERT INTO dataset (id, name) VALUES(nextval('seq_dataset_id'), ?)", [name])
                except duckdb.ConstraintException:
                    pass
            self.conn.commit()

            self.datset_set = set()

    def write_dataset_member_by_name(self):
        if self.dataset_member_by_name_list:
            df = pd.DataFrame(
                self.dataset_member_by_name_list,
                columns=["name", "data_object_id", "annotation_id"],
            )
            self.conn.register("dataset_member_by_name_df", df)
            self.conn.begin()
            self.conn.execute(
                """
                UPDATE dataset_member
                SET annotation_id = q.annotation_id
                FROM (
                    SELECT
                        dataset.id as dataset_id,
                        dataset_member_by_name_df.data_object_id as data_object_id,
                        dataset_member_by_name_df.annotation_id as annotation_id
                    FROM dataset_member_by_name_df
                    JOIN dataset ON dataset_member_by_name_df.name = dataset.name
                ) q
                WHERE dataset_member.dataset_id = q.dataset_id
                    AND dataset_member.data_object_id = q.data_object_id
                """
            )
            self.conn.execute(
                """
                INSERT INTO dataset_member (
                    SELECT
                        dataset.id,
                        dataset_member_by_name_df.data_object_id,
                        dataset_member_by_name_df.annotation_id
                    FROM dataset_member_by_name_df
                    JOIN dataset ON dataset_member_by_name_df.name = dataset.name
                    WHERE (dataset.id, dataset_member_by_name_df.data_object_id) NOT IN (
                        SELECT (dataset_id, data_object_id) FROM dataset_member
                    )
                )
                """
            )
            self.conn.commit()

            self.conn.unregister("dataset_member_by_name_df")
            self.dataset_member_by_name_list = []

    def add_pair(
        self,
        data_object_hash: str,
        data_object_meta: "DataObjectMetaT",
        annotation: Optional[Annotation] = None,
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

    def add_data_object_meta(self, id, obj):
        #self.data_object_meta_list.append((id, json_dumps(obj)))
        self.data_object_meta_list.append((id, obj))

    def get_data_object_meta(self, id):
        pass

    def get_data_object_meta_many(self, ids):
        self.conn.register("data_object_meta_id", pd.DataFrame(ids, columns=["id"]))
        result = self.conn.execute(
            """
            SELECT * from data_object_meta
            WHERE id in (SELECT id FROM data_object_meta_id)
            """,
        ).fetchall()
        self.conn.unregister("data_object_meta_id")
        return result

    def get_data_object_meta_all(self):
        return self.conn.execute(
            """
            SELECT * from data_object_meta
            """,
        ).fetchall()

    def add_annotation(self, obj: Annotation):
        assert obj.oid
        self.annotation_list.append((obj.oid, obj.value))

    def get_annotation(self, id: str):
        pass

    def get_annotation_many(self, ids):
        self.conn.register("annotation_id", pd.DataFrame(ids, columns=["id"]))
        result = self.conn.execute(
            """
            SELECT * from annotation
            WHERE id in (SELECT id FROM annotation_id)
            """,
        ).fetchall()
        self.conn.unregister("annotation_ids")
        return result

    def get_annotation_all(self):
        return self.conn.execute(
            """
            SELECT * from annotation
            """,
        ).fetchall()

    def add_pair_meta(self, id, annot_id, obj):
        self.data_object_annotation_list.append((id, annot_id, json.dumps(obj)))

    def get_pair_meta(self, id: str, annot_id: str):
        pass

    def get_pair_meta_many(self, collection: Iterable[Tuple[str, Optional[str]]]):
        df = pd.DataFrame(list(collection), columns=["data_object_id", "annotation_id"])
        self.conn.register("collection_df", df)
        result = self.conn.execute(
            """
            SELECT * FROM data_object_annotation
            WHERE (data_object_id, annotation_id) in (
                SELECT (data_object_id, annotation_id) FROM collection_df
            )
            """
        ).fetchall()
        self.conn.unregister("collection_df")
        for data_object_id, annotation_id, value in result:
            yield data_object_id, annotation_id, json.loads(value)

    def get_pair_meta_all(self):
        result = self.conn.execute(
            """
            SELECT * FROM data_object_annotation
            """
        ).fetchall()
        for data_object_id, annotation_id, value in result:
            yield data_object_id, annotation_id, json.loads(value)

    def jp_search_pair_meta(self, query: str, collection: Iterable[Tuple[str, Optional[str]]]):
        from ldb.query.search import get_search_func

        search = get_search_func(query)
        data = self.get_pair_meta_many(collection)
        iter1, iter2 = tee(data)
        values = (v for _, _, v in iter1)
        for (data_object_id, annotation_id, _), result in zip(iter2, search(values)):
            yield data_object_id, annotation_id, result

    def add_dataset(self, name: str):
        self.dataset_set.add(name)

    def set_current_annot(self, id: str, annot_id: str):
        self.add_dataset_member_by_name("root", id, annot_id)

    def add_dataset_member_by_name(self, name: str, data_object_id: str, annotation_id: str):
        self.dataset_member_by_name_list.append(
            (name, data_object_id, annotation_id)
        )

    def get_root_collection(self):
        return dict(self.get_dataset_member_many("root"))

    def get_dataset_member_many(self, dataset_name: str):
        return self.conn.execute(
            """
            SELECT data_object_id, annotation_id FROM dataset_member WHERE dataset_id = (
                SELECT id FROM dataset WHERE name = (?)
            )
            """,
            [dataset_name],
        ).fetchall()
