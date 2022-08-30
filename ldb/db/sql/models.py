from sqlalchemy import Column, Integer, Sequence, String, create_engine, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import Session

Base = declarative_base()

class Annotation(Base):
    __tablename__ = "annotation"

    value = Column(JSON)
    meta = Column(JSON)
    id = Column(String, primary_key=True)
