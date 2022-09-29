from typing import TYPE_CHECKING, Optional

import duckdb
import pandas as pd

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
        self.dataset_member_list = []

    def init(self):
        self.conn.begin()
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS data_object_meta(id VARCHAR PRIMARY KEY, value VARCHAR)"
        )
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS annotation(id VARCHAR PRIMARY KEY, value VARCHAR)"
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS data_object_annotation(
                data_object_id VARCHAR,
                annotation_id VARCHAR,
                value VARCHAR,
                PRIMARY KEY (data_object_id, annotation_id),
                FOREIGN KEY (data_object_id) REFERENCES data_object_meta(id),
                FOREIGN KEY (annotation_id) REFERENCES annotation(id)
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dataset(id INTEGER PRIMARY KEY, name VARCHAR)
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dataset_member(
                dataset_id INTEGER,
                data_object_id VARCHAR,
                annotation_id VARCHAR,
                FOREIGN KEY (dataset_id) REFERENCES dataset(id),
                FOREIGN KEY (data_object_id) REFERENCES data_object_meta(id),
                FOREIGN KEY (annotation_id) REFERENCES annotation(id)
            )
            """
        )
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_dataset_id START 1")
        self.conn.execute("INSERT INTO dataset (id, name) VALUES(nextval('seq_dataset_id'), ?)", ["root"])
        self.conn.commit()

    def write_all(self):
        self.write_data_object_meta()
        self.write_annotation()
        self.write_data_object_annotation()
        #self.write_dataset()
        #self.write_dataset_member()

    def write_data_object_meta(self):
        # data_object_meta
        # annotation
        # data_object_annotation
        # update
        if self.data_object_meta_list:
            df = pd.DataFrame(self.data_object_meta_list, columns=["id", "value"])
            self.conn.register("data_object_meta_df",  df)

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

            self.data_object_meta_list = []

    def write_annotation(self):
        if self.annotation_list:
            df = pd.DataFrame(self.annotation_list, columns=["id", "value"])
            self.conn.register("annotation_df",  df)

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

            self.annotation_list = []

    def write_data_object_annotation(self):
        if self.data_object_annotation_list:
            df = pd.DataFrame(self.data_object_annotation_list, columns=["data_object_id", "annotation_id", "value"])
            self.conn.register("data_object_annotation_df",  df)

            self.conn.begin()
            #self.conn.execute(
            #    """
            #    UPDATE data_object_annotation
            #    SET value = data_object_annotation_df.value
            #    FROM data_object_annotation_df
            #    WHERE
            #        data_object_annotation.data_object_id = data_object_annotation_df.data_object_id
            #        AND data_object_annotation.annotation_id = data_object_annotation_df.annotation_id
            #    """
            #)
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
                        SELECT data_object_id, annotation_id from data_object_annotation
                    )
                )
                """
            )
            self.conn.commit()

            self.data_object_annotation_list = []


    def add_pair(
        self,
        data_object_hash: str,
        data_object_meta: "DataObjectMetaT",
        annotation: Optional[Annotation] = None,
        annotation_meta: Optional["AnnotationMeta"] = None,
    ) -> None:
        """
        self.add_data_object_meta(data_object_hash, data_object_meta)
        if annotation is not None:
            self.add_annotation(annotation)
            self.add_pair_meta(
                data_object_hash,
                annotation.oid,
                annotation_meta,
            )
            self.set_current_annot(data_object_hash, annotation.oid)
        """

    def add_data_object_meta(self, id, obj):
        self.data_object_meta_list.append((id, json_dumps(obj)))

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
        self.annotation_list.append(json_dumps(obj))

    def get_annotation(self, id: str):
        pass

    def get_annotation_all(self, ids):
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
        pass

    def get_pair_meta(self, id: str, annot_id: str):
        pass

    def set_current_annot(self, id: str, annot_id: str):
        pass

    def get_root_collection(self):
        pass
