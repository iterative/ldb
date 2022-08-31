import os

from sqlalchemy import JSON, Column, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import sessionmaker

Base = declarative_base()
Session = sessionmaker()
# TODO: setup better config mechanism instead of using global state
_SESSION = {}
_ALLOW_RECONFIG = True


def get_session(path: str):
    url = path_to_db_url(path)
    if not _SESSION:
        configure_db(url)
        session = Session()
        _SESSION[url] = session
    else:
        ((configured_url, session),) = _SESSION.items()
        if url != configured_url:
            if _ALLOW_RECONFIG:
                _SESSION.popitem()
                session.close()
                configure_db(url)
                session = Session()
                _SESSION[url] = session
            else:
                raise ValueError(
                    "Cannot configure database {url}. Already configured database {configured_url}",
                )
    return session


def get_db_path(ldb_dir: str) -> str:
    from ldb.db import LDB_BACKEND

    if LDB_BACKEND == "duckdb":
        return os.path.join(ldb_dir, "duckdb", "index.duckdb")
    elif LDB_BACKEND == "sqlite":
        return os.path.join(ldb_dir, "sqlite", "index.db")
    else:
        raise ValueError("backend")


def path_to_db_url(path: str) -> str:
    from ldb.db import LDB_BACKEND

    if LDB_BACKEND == "duckdb":
        return f"duckdb:///{path}"
    elif LDB_BACKEND == "sqlite":
        return f"sqlite:///{path}"
    else:
        raise ValueError("backend")


def init_db(ldb_dir: str):
    db_path = get_db_path(ldb_dir)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    url = path_to_db_url(db_path)
    configure_db(url)


def configure_db(db_url: str):
    engine = create_engine(db_url)
    Session.configure(bind=engine)
    Base.metadata.bind = engine
    Base.metadata.create_all()


class Annotation(Base):
    __tablename__ = "annotation"

    id = Column(String, primary_key=True)
    value = Column(JSON)
    meta = Column(JSON)


class DataObjectMeta(Base):
    __tablename__ = "data_object_meta"

    id = Column(String, primary_key=True)
    meta = Column(JSON)


class DataObjectCurrentAnnot(Base):
    __tablename__ = "data_object_current_annot"

    id = Column(String, primary_key=True)
    current_annotation = Column(String)


class DataObjectAnnotation(Base):
    __tablename__ = "data_object_annotations"

    id = Column(String, primary_key=True)
    annot_id = Column(String, primary_key=True)
    value = Column(JSON)


# def get_engine(path: str):
#    return create_engine(
#        path_to_db_url(path),
#        connect_args={
#            'preload_extensions': ['json'],
#        }
#    )
