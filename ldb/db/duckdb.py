import json
from typing import TYPE_CHECKING, Iterable, Optional, Tuple

import pandas as pd

import duckdb
from ldb.db.abstract import (
    AbstractDB,
    AnnotationRecord,
    DataObjectAnnotationRecord,
    DataObjectMetaRecord,
    DatasetMemberRecord,
)

if TYPE_CHECKING:
    from ldb.index.utils import AnnotationMeta
    from ldb.index.utils import DataObjectMeta as DataObjectMetaT


class DuckDB(AbstractDB):
    def __init__(self, path: str) -> None:
        super().__init__(path)
        self.conn = duckdb.connect(self.path)

    def init(self) -> None:
        self.conn.begin()
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS data_object_meta(id VARCHAR PRIMARY KEY, value JSON)",
        )
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS annotation(id VARCHAR PRIMARY KEY, value JSON)",
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
            """,
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dataset(id INTEGER PRIMARY KEY, name VARCHAR UNIQUE)
            """,
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
            """,
        )
        self.conn.execute(
            "CREATE SEQUENCE IF NOT EXISTS seq_dataset_id START 1",
        )
        self.conn.commit()

        self.add_dataset("root")
        self.write_all()

    def write_data_object_meta(self) -> None:
        if self.data_object_meta_list:
            df = pd.DataFrame(
                self.data_object_meta_list,
                columns=["id", "value"],
            )
            self.conn.register("data_object_meta_df", df)

            self.conn.begin()
            self.conn.execute(
                """
                UPDATE data_object_meta
                SET value = data_object_meta_df.value
                FROM data_object_meta_df
                WHERE data_object_meta.id = data_object_meta_df.id
                """,
            )
            self.conn.execute(
                """
                INSERT INTO data_object_meta (
                    SELECT * from data_object_meta_df
                    WHERE id not in (SELECT id from data_object_meta)
                )
                """,
            )
            self.conn.commit()

            self.conn.unregister("data_object_meta_df")
            self.data_object_meta_list = []

    def get_data_object_meta(self, id: str) -> Optional[DataObjectMetaRecord]:
        result = self.conn.execute(
            """
            SELECT value from data_object_meta
            WHERE id = (?)
            """,
            [id],
        ).fetchone()
        if result is None:
            return None
        return json.loads(result[0])

    def get_data_object_meta_many(self, ids: Iterable[str]) -> Iterable[DataObjectMetaRecord]:
        self.conn.register(
            "data_object_meta_id",
            pd.DataFrame(ids, columns=["id"]),
        )
        result = self.conn.execute(
            """
            SELECT * from data_object_meta
            WHERE id in (SELECT id FROM data_object_meta_id)
            """,
        ).fetchall()
        self.conn.unregister("data_object_meta_id")
        for id, value in result:
            yield id, json.loads(value)

    def get_data_object_meta_all(self) -> Iterable[DataObjectMetaRecord]:
        result = self.conn.execute(
            """
            SELECT * from data_object_meta
            """,
        ).fetchall()
        for id, value in result:
            yield id, json.loads(value)

    def write_annotation(self) -> None:
        if self.annotation_list:
            data = [(a.oid, a.value_str) for a in self.annotation_list]
            df = pd.DataFrame(data, columns=["id", "value"])
            self.conn.register("annotation_df", df)

            self.conn.begin()
            self.conn.execute(
                """
                UPDATE annotation
                SET value = annotation_df.value
                FROM annotation_df
                WHERE annotation.id = annotation_df.id
                """,
            )
            self.conn.execute(
                """
                INSERT INTO annotation (
                    SELECT distinct on (id) id, value from annotation_df
                    WHERE id not in (SELECT id from annotation)
                )
                """,
            )
            self.conn.commit()

            self.conn.unregister("annotation_df")
            self.annotation_list = []

    def get_annotation(self, id: str) -> Optional[AnnotationRecord]:
        result = self.conn.execute(
            """
            SELECT * from annotation
            WHERE id = (?)
            """,
            [id],
        ).fetchone()
        if result is None:
            return None
        id, value = result[0]
        return id, json.loads(value)

    def get_annotation_many(self, ids: Iterable[str]) -> Iterable[AnnotationRecord]:
        self.conn.register("annotation_id", pd.DataFrame(ids, columns=["id"]))
        result = self.conn.execute(
            """
            SELECT * from annotation
            WHERE id in (SELECT id FROM annotation_id)
            """,
        ).fetchall()
        self.conn.unregister("annotation_ids")
        for id, value in result:
            yield id, json.loads(value)

    def get_annotation_all(self) -> Iterable[AnnotationRecord]:
        result = self.conn.execute(
            """
            SELECT * from annotation
            """,
        ).fetchall()
        for id, value in result:
            yield id, json.loads(value)

    def write_data_object_annotation(self) -> None:
        if self.data_object_annotation_list:
            data = [
                (d, a, json.dumps(value)) for d, a, value in self.data_object_annotation_list
            ]
            df = pd.DataFrame(
                data,
                columns=["data_object_id", "annotation_id", "value"],
            )
            self.conn.register("data_object_annotation_df", df)

            self.conn.begin()
            self.conn.execute(
                """
                UPDATE data_object_annotation
                SET value = data_object_annotation_df.value
                FROM data_object_annotation_df
                WHERE
                    (
                        data_object_annotation.data_object_id,
                        data_object_annotation.annotation_id
                    )
                    = (
                        data_object_annotation_df.data_object_id,
                        data_object_annotation_df.annotation_id
                    )
                """,
            )
            self.conn.execute(
                """
                INSERT INTO data_object_annotation (
                    SELECT * from data_object_annotation_df
                    WHERE (data_object_id, annotation_id) not in (
                        SELECT (data_object_id, annotation_id) from data_object_annotation
                    )
                )
                """,
            )
            self.conn.commit()

            self.conn.unregister("data_object_annotation_df")
            self.data_object_annotation_list = []

    def get_pair_meta(self, id: str, annot_id: str) -> Optional[DataObjectAnnotationRecord]:
        result = self.conn.execute(
            """
            SELECT value from data_object_annotation
            WHERE (data_object_id, annotation_id) = (?, ?)
            """,
            [id, annot_id],
        ).fetchone()
        if result is None:
            return None
        return json.loads(result[0])

    def get_pair_meta_many(
        self,
        collection: Iterable[Tuple[str, Optional[str]]],
    ) -> Iterable[DataObjectAnnotationRecord]:
        df = pd.DataFrame(
            list(collection),
            columns=["data_object_id", "annotation_id"],
        )
        self.conn.register("collection_df", df)
        result = self.conn.execute(
            """
            SELECT * FROM data_object_annotation
            WHERE (data_object_id, annotation_id) in (
                SELECT (data_object_id, annotation_id) FROM collection_df
            )
            """,
        ).fetchall()
        self.conn.unregister("collection_df")
        for data_object_id, annotation_id, value in result:
            yield data_object_id, annotation_id, json.loads(value)

    def get_pair_meta_all(self) -> Iterable[DataObjectAnnotationRecord]:
        result = self.conn.execute(
            """
            SELECT * FROM data_object_annotation
            """,
        ).fetchall()
        for data_object_id, annotation_id, value in result:
            yield data_object_id, annotation_id, json.loads(value)

    def count_pairs(self, id: str) -> int:
        return self.conn.execute(
            """
            SELECT COUNT(*)
            FROM data_object_annotation
            WHERE data_object_id = (?)
            """,
            [id],
        ).fetchone()[0]

    def write_dataset(self) -> None:
        if self.dataset_set:
            self.conn.begin()
            for name in self.dataset_set:
                try:
                    self.conn.execute(
                        "INSERT INTO dataset (id, name) VALUES(nextval('seq_dataset_id'), ?)",
                        [name],
                    )
                except duckdb.ConstraintException:
                    pass
            self.conn.commit()

            self.datset_set = set()

    def write_dataset_member_by_name(self) -> None:
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
                """,
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
                """,
            )
            self.conn.commit()

            self.conn.unregister("dataset_member_by_name_df")
            self.dataset_member_by_name_list = []

    def get_dataset_member_many(self, dataset_name: str) -> Iterable[DatasetMemberRecord]:
        yield from self.conn.execute(
            """
            SELECT data_object_id, annotation_id FROM dataset_member WHERE dataset_id = (
                SELECT id FROM dataset WHERE name = (?)
            )
            """,
            [dataset_name],
        ).fetchall()

    def ls_collection(
        self, collection: Iterable[Tuple[str, Optional[str]]]
    ) -> Iterable[Tuple[str, str, int, int]]:
        df = pd.DataFrame(
            list(collection),
            columns=["data_object_id", "annotation_id"],
        )
        self.conn.register("collection_df", df)
        result = self.conn.execute(
            """
            SELECT
                collection_df.data_object_id,
                json_extract_string(data_object_meta.value, '$.fs.path'),
                collection_df.annotation_id,
                json_transform_strict(
                    json_extract(data_object_annotation.value, '$.version'),
                    '"UINTEGER"'
                )
            FROM data_object_meta
            JOIN collection_df on collection_df.data_object_id = data_object_meta.id
            JOIN data_object_annotation
                ON (data_object_annotation.data_object_id, data_object_annotation.annotation_id)
                    = (collection_df.data_object_id, collection_df.annotation_id)
            """,
        ).fetchall()
        self.conn.unregister("collection_df")
        yield from result
