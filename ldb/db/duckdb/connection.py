import os

from sqlalchemy import Column, Integer, Sequence, String, create_engine, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import Session

from ldb.db.sql.models import Annotation, Base


def get_db_path(ldb_dir: str) -> str:
    return os.path.join(ldb_dir, "duckdb", "index.duckdb")


def path_to_db_url(path: str) -> str:
    return f"duckdb:///{path}"


def init_db(ldb_dir: str):
    db_path = get_db_path(ldb_dir)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    eng = get_engine(db_path)
    create_tables(eng)
    return eng


def create_tables(engine):
    Base.metadata.create_all(engine)


def get_engine(path: str):
    return create_engine(
        path_to_db_url(path),
        connect_args={
            'preload_extensions': ['json'],
        }
    )


def get_session(path: str):
    return Session(bind=get_engine(path))
