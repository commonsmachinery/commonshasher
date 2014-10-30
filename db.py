import config

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

Base = declarative_base()

class Work(Base):
    __tablename__ = 'works'

    id = Column(Integer, primary_key=True)
    handler = Column(String)

    # Never process an URL more than once
    url = Column(String, unique=True)

    # loaded -> queued -> processing -> done/error
    status = Column(String, index=True)

    # Current processing task
    task_id = Column(String)
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
