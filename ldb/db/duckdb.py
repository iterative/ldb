import json
from typing import (
    TYPE_CHECKING,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
)

import pandas as pd

import duckdb
from ldb.dataset import CommitInfo, Dataset
from ldb.db.abstract import (
    AbstractDB,
    AnnotationRecord,
    DataObjectAnnotationRecord,
    DataObjectMetaRecord,
)
from ldb.exceptions import (
    CollectionNotFoundError,
    DataObjectNotFoundError,
    DatasetNotFoundError,
    DatasetVersionNotFoundError,
)
from ldb.objects.dataset_version import DatasetVersion
from ldb.utils import DATA_OBJ_ID_PREFIX, normalize_datetime

if TYPE_CHECKING:
    from ldb.index.utils import AnnotationMeta  # noqa: F401
    from ldb.index.utils import DataObjectMeta as DataObjectMetaT  # noqa: F401


class DuckDB(AbstractDB):
    def __init__(self, path: str) -> None:
        super().__init__(path)
        self.dataset_set: Set[str] = set()
        self.dataset_member_by_name_list: List[Tuple[str, str, str]] = []
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
        # dynamic_dataset is for mutable datasets like the root dataset
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dynamic_dataset(id INTEGER PRIMARY KEY, name VARCHAR UNIQUE)
            """,
        )
        self.conn.execute(
            "CREATE SEQUENCE IF NOT EXISTS seq_dynamic_dataset_id START 1",
        )
        # The primary key is commented out for now due to lack of proper update support
        # https://github.com/duckdb/duckdb/issues/3265#issuecomment-1090012268
        # TODO see if there's a workaround, such as deleting, then inserting
        # https://github.com/duckdb/duckdb/issues/61
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dynamic_dataset_member(
                dataset_id INTEGER,
                data_object_id VARCHAR,
                annotation_id VARCHAR,
                -- PRIMARY KEY (dataset_id, data_object_id),
                FOREIGN KEY (dataset_id) REFERENCES dynamic_dataset(id),
                FOREIGN KEY (data_object_id) REFERENCES data_object_meta(id),
                FOREIGN KEY (annotation_id) REFERENCES annotation(id),
            )
            """,
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS collection(id VARCHAR PRIMARY KEY)
            """,
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS collection_member(
                collection_id VARCHAR,
                data_object_id VARCHAR,
                annotation_id VARCHAR,
                -- PRIMARY KEY (collection_id, data_object_id),
                FOREIGN KEY (collection_id) REFERENCES collection(id),
                FOREIGN KEY (data_object_id) REFERENCES data_object_meta(id),
                FOREIGN KEY (annotation_id) REFERENCES annotation(id),
            )
            """,
        )

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dataset_version(
                id VARCHAR PRIMARY KEY,
                version UINTEGER,
                parent VARCHAR,
                collection VARCHAR NOT NULL,
                transform_mapping_id VARCHAR,
                tags VARCHAR[],
                created_by VARCHAR,
                commit_time TIMESTAMPTZ,
                commit_message VARCHAR,
                auto_pull BOOLEAN,
                FOREIGN KEY (parent) REFERENCES dataset_version(id),
                FOREIGN KEY (collection) REFERENCES collection(id),
                -- FOREIGN KEY (transform_mapping_id) REFERENCES transform_mapping(id),
            )
            """,
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dataset(
                id UINTEGER PRIMARY KEY,
                name VARCHAR UNIQUE,
                created_by VARCHAR,
                created TIMESTAMPTZ
            )
            """,
        )
        self.conn.execute(
            "CREATE SEQUENCE IF NOT EXISTS seq_dataset_id START 1",
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dataset_assignment(
                dataset_id UINTEGER NOT NULL,
                dataset_version_id VARCHAR NOT NULL,
                version_number UINTEGER NOT NULL,
                FOREIGN KEY (dataset_id) REFERENCES dataset(id),
                FOREIGN KEY (dataset_version_id) REFERENCES dataset_version(id),
            )
            """,
        )
        self.conn.commit()

        self.add_dataset("root")
        self.write_all()

    def write_all(self) -> None:
        super().write_all()
        self.write_dataset()
        self.write_dataset_member_by_name()

    def write_data_object_meta(self) -> None:
        if self.data_object_meta_list:
            data = [(id, json.dumps(value)) for id, value in self.data_object_meta_list]
            df = pd.DataFrame(data, columns=["id", "value"])
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
        result: Optional[Tuple[str, str]]
        result = self.conn.execute(  # type: ignore[assignment]
            """
            SELECT * from data_object_meta
            WHERE id = (?)
            """,
            [id],
        ).fetchone()
        if result is None:
            return None
        id, value = result
        return id, json.loads(value)

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

    def get_existing_data_object_ids(self, ids: Iterable[str]) -> Set[str]:
        self.conn.register(
            "data_object_meta_id",
            pd.DataFrame(ids, columns=["id"]),
        )
        result = self.conn.execute(
            """
            SELECT id from data_object_meta
            WHERE id in (SELECT id FROM data_object_meta_id)
            """,
        ).fetchall()
        return {r[0] for r in result}

    def write_annotation(self) -> None:
        if self.annotation_map:
            data = [(id, annot.value_str) for id, annot in self.annotation_map.items()]
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
            self.annotation_map = {}

    def get_annotation(self, id: str) -> Optional[AnnotationRecord]:
        result: Optional[Tuple[str, str]]
        result = self.conn.execute(  # type: ignore[assignment]
            """
            SELECT * from annotation
            WHERE id = (?)
            """,
            [id],
        ).fetchone()
        if result is None:
            return None
        id, value = result
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
        result: Optional[Tuple[str, str, str]]
        result = self.conn.execute(  # type: ignore[assignment]
            """
            SELECT * from data_object_annotation
            WHERE (data_object_id, annotation_id) = (?, ?)
            """,
            [id, annot_id],
        ).fetchone()
        if result is None:
            return None
        data_object_id, annotation_id, value = result
        return data_object_id, annotation_id, json.loads(value)

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
        result: Tuple[int]
        result = self.conn.execute(  # type: ignore[assignment]
            """
            SELECT COUNT(*)
            FROM data_object_annotation
            WHERE data_object_id = (?)
            """,
            [id],
        ).fetchone()
        return result[0]

    def write_collection(self) -> None:
        if self.collection_map:
            # TODO handle race condition - gap between checking existing ids and inserting
            existing_collection_ids = self.get_existing_collection_ids(
                self.collection_map.keys()
            )
            ids_to_insert = set(self.collection_map.keys()) - existing_collection_ids
            data = [
                (id, data_object_id, annotation_id)
                for id in ids_to_insert
                for data_object_id, annotation_id in self.collection_map[id].items()
            ]
            df = pd.DataFrame(
                data,
                columns=["collection_id", "data_object_id", "annotation_id"],
            )
            id_df = pd.DataFrame(ids_to_insert, columns=["id"])
            self.conn.register("dataset_member_df", df)
            self.conn.register("collection_df", id_df)
            self.conn.begin()
            self.conn.execute(
                """
                INSERT INTO collection SELECT * FROM collection_df
                """,
            )
            self.conn.execute(
                """
                INSERT INTO collection_member SELECT * FROM dataset_member_df
                """,
            )
            self.conn.commit()
            self.conn.unregister("dataset_member_df")
            self.conn.unregister("collection_df")

    def get_collection(self, id: str) -> Iterable[Tuple[str, str]]:
        # get members
        # if no members check for member set id
        result = self.conn.execute(  # type: ignore[assignment]
            """
            SELECT data_object_id, annotation_id from collection_member
            WHERE collection_id = ?
            """,
            [id],
        ).fetchall()
        if not result:
            id_result = self.conn.execute(  # type: ignore[assignment]
                """
                SELECT EXISTS(SELECT id FROM collection WHERE id = ?)
                """,
                [id],
            ).fetchone()[0]
            if not id_result:
                raise CollectionNotFoundError(f"Collection not found: {id}")
        yield from result

    def get_existing_collection_ids(self, ids: Iterable[str]) -> Set[str]:
        self.conn.register(
            "input_id",
            pd.DataFrame(ids, columns=["id"]),
        )
        result = self.conn.execute(
            """
            SELECT id from collection
            WHERE id in (SELECT id FROM input_id)
            """,
        ).fetchall()
        return {r[0] for r in result}

    def get_collection_id_all(self) -> Iterable[str]:
        result = self.conn.execute(
            """
            SELECT id from collection
            """,
        ).fetchall()
        yield from result

    def add_dataset(self, name: str) -> None:
        self.dataset_set.add(name)

    def add_dataset_member_by_name(
        self,
        name: str,
        data_object_id: str,
        annotation_id: str,
    ) -> None:
        self.dataset_member_by_name_list.append(
            (name, data_object_id, annotation_id),
        )

    def write_dataset(self) -> None:
        if self.dataset_set:
            self.conn.begin()
            for name in self.dataset_set:
                try:
                    self.conn.execute(
                        "INSERT INTO dynamic_dataset (id, name) VALUES(nextval('seq_dynamic_dataset_id'), ?)",
                        [name],
                    )
                except duckdb.ConstraintException:
                    pass
            self.conn.commit()

            self.dataset_set = set()

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
                UPDATE dynamic_dataset_member
                SET annotation_id = q.annotation_id
                FROM (
                    SELECT
                        dynamic_dataset.id as dataset_id,
                        dataset_member_by_name_df.data_object_id as data_object_id,
                        dataset_member_by_name_df.annotation_id as annotation_id
                    FROM dataset_member_by_name_df
                    JOIN dynamic_dataset ON dataset_member_by_name_df.name = dynamic_dataset.name
                ) q
                WHERE dynamic_dataset_member.dataset_id = q.dataset_id
                    AND dynamic_dataset_member.data_object_id = q.data_object_id
                """,
            )
            self.conn.execute(
                """
                INSERT INTO dynamic_dataset_member (
                    SELECT
                        dynamic_dataset.id,
                        dataset_member_by_name_df.data_object_id,
                        dataset_member_by_name_df.annotation_id
                    FROM dataset_member_by_name_df
                    JOIN dynamic_dataset ON dataset_member_by_name_df.name = dynamic_dataset.name
                    WHERE (dynamic_dataset.id, dataset_member_by_name_df.data_object_id) NOT IN (
                        SELECT (dataset_id, data_object_id) FROM dynamic_dataset_member
                    )
                )
                """,
            )
            self.conn.commit()

            self.conn.unregister("dataset_member_by_name_df")
            self.dataset_member_by_name_list = []

    def get_dataset_member_many(self, dataset_name: str) -> Iterable[Tuple[str, str]]:
        yield from self.conn.execute(
            """
            SELECT data_object_id, annotation_id FROM dynamic_dataset_member WHERE dataset_id = (
                SELECT id FROM dynamic_dataset WHERE name = ?
            )
            """,
            [dataset_name],
        ).fetchall()

    def get_root_collection(self) -> Iterable[Tuple[str, str]]:
        return self.get_dataset_member_many("root")

    def set_current_annot(self, id: str, annot_id: str) -> None:
        self.add_dataset_member_by_name("root", id, annot_id)

    def ls_collection(
        self, collection: Iterable[Tuple[str, Optional[str]]]
    ) -> Iterable[Tuple[str, str, str, int]]:
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
            FROM collection_df
            LEFT JOIN data_object_meta on collection_df.data_object_id = data_object_meta.id
            LEFT JOIN data_object_annotation
                ON (data_object_annotation.data_object_id, data_object_annotation.annotation_id)
                    = (collection_df.data_object_id, collection_df.annotation_id)
            """,
        ).fetchall()
        self.conn.unregister("collection_df")
        yield from result

    def write_dataset_version(self) -> None:
        if not self.dataset_version_map:
            return
        data = [
            [
                id,
                d.version,
                d.parent or None,
                d.collection,
                d.transform_mapping_id or None,
                d.tags,
                d.commit_info.created_by,
                d.commit_info.commit_time,
                d.commit_info.commit_message,
                d.auto_pull,
            ]
            for id, d in self.dataset_version_map.items()
        ]
        df = pd.DataFrame(
            data,
            columns=[
                "id",
                "version",
                "parent",
                "collection",
                "transform_mapping_id",
                "tags",
                "created_by",
                "commit_time",
                "commit_message",
                "auto_pull",
            ],
        )
        self.conn.register("dataset_version_df", df)
        self.conn.execute(
            """
            INSERT INTO dataset_version (
                SELECT *
                FROM dataset_version_df
                WHERE dataset_version_df.id NOT IN (SELECT id FROM dataset_version)
            )
            """
        )
        self.conn.unregister("dataset_version_df")

    def get_dataset_version(self, id: str) -> "DatasetVersion":
        return list(self.get_dataset_version_many([id]))[0]
        # result = self.conn.execute(
        #    """
        #    SELECT * FROM dataset_version WHERE id = ?
        #    """,
        #    [id],
        # ).fetchone()
        # if result is None:
        #    raise DatasetVersionNotFoundError(f"DatasetVersion not found {id}")
        # (
        #    id,
        #    version,
        #    parent,
        #    collection,
        #    transform_mapping_id,
        #    tags,
        #    created_by,
        #    commit_time,
        #    commit_message,
        #    auto_pull,
        # ) = result
        # return DatasetVersion(
        #    version=version,
        #    parent=parent or "",
        #    collection=collection or "",
        #    transform_mapping_id=transform_mapping_id,
        #    tags=tags,
        #    commit_info=CommitInfo(
        #        created_by=created_by,
        #        commit_time=normalize_datetime(commit_time),
        #        commit_message=commit_message,
        #    ),
        #    auto_pull=auto_pull,
        #    oid=id,
        # )

    def get_dataset_version_many(self, ids: Iterable[str]) -> Iterator["DatasetVersion"]:
        df = pd.DataFrame(
            list(ids),
            columns=["id"],
        )
        self.conn.register("dataset_version_df", df)
        result = self.conn.execute(
            """
            SELECT dataset_version_df.id as input_id, dataset_version.*
            FROM dataset_version_df
            LEFT JOIN dataset_version ON dataset_version_df.id = dataset_version.id
            """,
        ).fetchdf()
        is_null = result["id"].isnull()
        if is_null.any():
            id = result["input_id"][is_null].iloc[0]
            raise DatasetVersionNotFoundError(f"DatasetVersion not found {id}")
        for d in result.fillna("").itertuples(index=False):
            yield DatasetVersion(
                version=d.version,
                parent=d.parent,
                collection=d.collection,
                transform_mapping_id=d.transform_mapping_id,
                tags=d.tags,
                commit_info=CommitInfo(
                    created_by=d.created_by,
                    commit_time=normalize_datetime(d.commit_time.to_pydatetime()),
                    commit_message=d.commit_message,
                ),
                auto_pull=bool(d.auto_pull),
                oid=d.id,
            )

    def get_dataset_version_id_all(self) -> Iterable[str]:
        result = self.conn.execute(
            """
            SELECT id FROM dataset_version
            """
        ).fetchall()
        for (id,) in result:
            yield id

    def get_dataset_version_by_name(
        self, name: str, version: Optional[int] = None
    ) -> Tuple["DatasetVersion", int]:
        if version is None:
            result = self.conn.execute(
                """
                SELECT dataset_assignment.version_number, dataset_version.*
                FROM dataset_assignment
                JOIN dataset ON dataset.id = dataset_assignment.dataset_id
                JOIN dataset_version on dataset_version.id = dataset_assignment.dataset_version_id
                WHERE dataset.name = ?
                ORDER BY dataset_assignment.version_number DESC
                LIMIT 1
                """,
                [name],
            ).fetchdf()
        else:
            result = self.conn.execute(
                """
                SELECT dataset_assignment.version_number, dataset_version.*
                FROM dataset_assignment
                JOIN dataset ON dataset.id = dataset_assignment.dataset_id
                JOIN dataset_version on dataset_version.id = dataset_assignment.dataset_version_id
                WHERE dataset.name = ? and dataset_assignment.version_number = ?
                """,
                [name, version],
            ).fetchdf()
        if result.empty:
            raise DatasetNotFoundError(f"Dataset {name} does not have version {version}")
        d = result.iloc[0]
        dataset_version = DatasetVersion(
            version=d.version,
            parent=d.parent,
            collection=d.collection,
            transform_mapping_id=d.transform_mapping_id,
            tags=d.tags,
            commit_info=CommitInfo(
                created_by=d.created_by,
                commit_time=normalize_datetime(d.commit_time.to_pydatetime()),
                commit_message=d.commit_message,
            ),
            auto_pull=bool(d.auto_pull),
            oid=d.id,
        )
        return dataset_version, d.version_number

    def write_dataset_assignment(self) -> None:
        for name, dataset_versions in self.dataset_version_assignments.items():
            data = [(name, d.oid) for d in dataset_versions]
            df = pd.DataFrame(
                data,
                columns=["name", "id"],
            )
            print(df)
            print(dataset_versions)
            self.conn.register("dataset_version_df", df)

            created_by = dataset_versions[0].commit_info.created_by
            created = dataset_versions[0].commit_info.commit_time
            try:
                self.conn.execute(
                    """
                    INSERT INTO dataset (id, name, created_by, created)
                    VALUES(nextval('seq_dataset_id'), ?, ?, ?)
                    """,
                    [name, created_by, created],
                )
            except duckdb.ConstraintException:
                pass
            self.conn.execute(
                """
                INSERT INTO dataset_assignment (
                    SELECT
                        dataset.id,
                        dataset_version_df.id,
                        COALESCE((SELECT MAX(dataset_assignment.version_number) FROM dataset_assignment WHERE dataset_assignment.dataset_id = dataset.id), 0) + 1,
                    FROM dataset_version_df
                    JOIN dataset ON dataset.name = dataset_version_df.name
                )
                """
            )
            self.conn.unregister("dataset_version_df")
            self.conn.commit()

    def get_dataset(self, name: str) -> "Dataset":
        result = self.conn.execute(  # type: ignore[assignment]
            """
            SELECT * FROM dataset
            WHERE name = (?)
            """,
            [name],
        ).fetchone()
        result = None
        if result is None:
            raise DatasetNotFoundError(f"Dataset not found: {name}")
        id, name, created_by, created = result
        version_result = self.conn.execute(  # type: ignore[assignment]
            """
            SELECT id FROM dataset_version
            WHERE dataset_id = (?)
            """,
            [id],
        ).fetchall()
        versions = [id for (id,) in version_result]
        return Dataset(
            name=name,
            created_by=created_by,
            created=created,
            versions=versions,
        )

    def get_dataset_many(self, names: Iterable[str]) -> Iterable["Dataset"]:
        for name in names:
            yield self.get_dataset(name)

    def get_dataset_all(self) -> Iterable["Dataset"]:
        for (name,) in self.conn.execute("SELECT name FROM dataset").fetchall():
            yield self.get_dataset(name)

    def write_transform(self) -> None:
        pass

    def get_transform(self, id: str) -> "Transform":
        raise NotImplementedError

    def get_transform_many(self, ids: Iterable[str]) -> Iterable["Transform"]:
        raise NotImplementedError

    def get_transform_all(self) -> Iterable["Transform"]:
        raise NotImplementedError

    def write_transform_mapping(self) -> None:
        pass

    def get_transform_mapping(self, id: str) -> Iterable[Tuple[str, List[str]]]:
        return []

    def get_transform_mapping_id_all(self) -> Iterable[str]:
        raise NotImplementedError

    def check_for_missing_data_object_ids(self, ids: Iterable[str]) -> None:
        df = pd.DataFrame(
            list(ids),
            columns=["id"],
        )
        self.conn.register("data_object_df", df)
        result = self.conn.execute(
            """
            SELECT id from data_object_df
            WHERE id NOT IN (SELECT id FROM data_object_meta)
            LIMIT 1
            """,
        ).fetchone()
        if result is not None:
            id = result[0]
            raise DataObjectNotFoundError(f"Data object not found: {DATA_OBJ_ID_PREFIX}{id}")
        self.conn.unregister("data_object_df")

    def get_current_annotation_hashes(self, data_object_ids: Iterable[str]) -> Iterable[str]:
        df = pd.DataFrame(
            list(data_object_ids),
            columns=["id"],
        )
        self.conn.register("data_object_df", df)
        result = self.conn.execute(
            """
            SELECT data_object_df.id, coalesce(dynamic_dataset_member.annotation_id, '')
            FROM data_object_df
            LEFT JOIN dynamic_dataset_member ON data_object_df.id = dynamic_dataset_member.data_object_id
            """,
            ["root"],
        ).fetchall()
        self.conn.unregister("data_object_df")
        yield from result

    def get_annotation_version_hashes(
        self, data_object_ids: Iterable[str], version: int = -1
    ) -> Iterable[Tuple[str, str]]:
        """
        Return (data_object_id, annotation_id) pairs for every data_object_id given.

        When version is -1, take the latest annotation version. If the version given
        does not exist for a data_object_id, then the annotation_id will be the empty
        string.
        """
        df = pd.DataFrame(
            list(data_object_ids),
            columns=["id"],
        )
        self.conn.register("data_object_df", df)
        pair_version = """
        SELECT
            data_object_annotation.data_object_id as data_object_id,
            data_object_annotation.annotation_id as annotation_id,
            json_transform_strict(
                json_extract(data_object_annotation.value, '$.version'),
                '"UINTEGER"'
            ) as annotation_version
        FROM data_object_annotation
        WHERE data_object_id in (SELECT id FROM data_object_df)
        """
        if version == -1:
            result = self.conn.execute(
                f"""WITH pair_version as ({pair_version}), latest_version as (
                    SELECT data_object_id, arg_max(annotation_id, annotation_version) as annotation_id
                    FROM pair_version
                    GROUP BY data_object_id
                )
                SELECT data_object_df.id, coalesce(latest_version.annotation_id, '')
                FROM data_object_df
                LEFT JOIN latest_version ON data_object_df.id = latest_version.data_object_id
                """,
            ).fetchall()
        else:
            result = self.conn.execute(
                f"""WITH pair_version as ({pair_version})
                SELECT data_object_df.id, coalesce(pair_version.annotation_id, '')
                FROM data_object_df
                LEFT JOIN pair_version
                    ON (
                        data_object_df.id = pair_version.data_object_id
                        AND pair_version.annotation_version = ?
                    )
                """,
                [version],
            ).fetchall()
        self.conn.unregister("data_object_id_df")
        yield from result
