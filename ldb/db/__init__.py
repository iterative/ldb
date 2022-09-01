import os

LDB_BACKEND = os.getenv("LDB_BACKEND", "fs")

if LDB_BACKEND == "fs":
    from ldb.db.annotation import AnnotationFileSystemDB as AnnotationDB
    from ldb.db.collection import CollectionFileSystemDB as CollectionDB
    from ldb.db.data_object import DataObjectFileSystemDB as DataObjectDB
elif LDB_BACKEND == "duckdb":
    from ldb.db.duckdb.annotation import AnnotationDuckDB as AnnotationDB
    #from ldb.db.duckdb.collection import CollectionDuckDB as CollectionDB
    #from ldb.db.duckdb.data_object import (
    #    DataObjectFileSystemDB as DataObjectDB,
    #)
    from ldb.db.collection import CollectionFileSystemDB as CollectionDB
    from ldb.db.data_object import DataObjectFileSystemDB as DataObjectDB
elif LDB_BACKEND == "sqlite":
    from ldb.db.sqlite.annotation import AnnotationSqliteDB as AnnotationDB
    from ldb.db.sqlite.collection import CollectionSqliteDB as CollectionDB
    from ldb.db.sqlite.data_object import DataObjectSqliteDB as DataObjectDB
else:
    raise ValueError("Invalid LDB_BACKEND value: {LDB_BACKEND!r}")
