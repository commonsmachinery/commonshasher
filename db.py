import config

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, DateTime, Enum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.types import TypeDecorator, CHAR
from sqlalchemy.dialects.postgresql import UUID
import uuid

Base = declarative_base()

# Backend agnostic GUID type, from the SQLalchemy docs
class GUID(TypeDecorator):
    """Platform-independent GUID type.

    Uses Postgresql's UUID type, otherwise uses
    CHAR(32), storing as stringified hex values.

    """
    impl = CHAR

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(UUID())
        else:
            return dialect.type_descriptor(CHAR(32))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return str(value)
        else:
            if not isinstance(value, uuid.UUID):
                return "%.32x" % uuid.UUID(value)
            else:
                # hexstring
                return "%.32x" % value

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            return uuid.UUID(value)

class Work(Base):
    __tablename__ = 'works'

    id = Column(Integer, primary_key=True)
    handler = Column(String)

    # Never process an URL more than once
    url = Column(String, unique=True)

    # loaded -> queued -> processing -> done/error
    status = Column(Enum('loaded', 'queued', 'processing', 'done', 'error', name='status'))

    # Current processing task
    task_id = Column(GUID)
    process_start = Column(DateTime)

    apidata = Column(String)
    hash = Column(String)
    updated = Column(DateTime)

    def __init__(self, handler, url):
        self.handler = handler
        self.url = url
        self.status = 'loaded'


def open_session():
    engine = create_engine(config.SQLALCHEMY_URL, echo=False)
    Base.metadata.create_all(bind=engine)
    session = scoped_session(sessionmaker(autocommit=False, autoflush=False, expire_on_commit=False, bind=engine))
    return session
