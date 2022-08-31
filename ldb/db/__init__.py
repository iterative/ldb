import os

LDB_BACKEND = os.getenv("LDB_BACKEND", "fs")

if LDB_BACKEND == "fs":
    from ldb.db.annotation import AnnotationFileSystemDB as AnnotationDB
elif LDB_BACKEND == "duckdb":
    from ldb.db.duckdb.annotation import AnnotationDuckDB as AnnotationDB
elif LDB_BACKEND == "sqlite":
    from ldb.db.sqlite.annotation import AnnotationSqliteDB as AnnotationDB

    print("fix backend")
else:
    raise ValueError("Invalid LDB_BACKEND value: {LDB_BACKEND!r}")
