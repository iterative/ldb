import os

LDB_BACKEND = os.getenv("LDB_BACKEND", "fs")

if LDB_BACKEND == "fs":
    pass
elif LDB_BACKEND == "duckdb":
    pass
    #from ldb.db.duckdb.collection import CollectionDuckDB as CollectionDB
    #from ldb.db.duckdb.data_object import (
    #    DataObjectFileSystemDB as DataObjectDB,
    #)
elif LDB_BACKEND == "sqlite":
    pass
else:
    raise ValueError("Invalid LDB_BACKEND value: {LDB_BACKEND!r}")
