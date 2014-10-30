from celery import Task

import db

class DatabaseTask(Task):
    abstract = True
    _db = None

    @property
    def db(self):
        if self._db is None:
            self._db = db.open_session()
        return self._db
