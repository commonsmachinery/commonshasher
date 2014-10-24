#! /usr/bin/env python3

import celery
from celery import Celery
from celery.exceptions import WorkerShutdown
from celery.signals import worker_ready

import db
import wmc
from common import DatabaseTask

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

app = Celery('commonshasher', broker='amqp://guest@localhost//')

app.conf.update(
    #CELERY_ALWAYS_EAGER=True,
    CELERYD_CONCURRENCY=1,
)

@app.task(bind=True, base=DatabaseTask)
def start_batch(self, *args):
    logger.info('starting batch')

@app.task(bind=True, base=DatabaseTask)
def setup_for_processing(self, *args):
    works_apidata = self.db.query(db.Work).filter(db.Work.apidata_status == 'queued').limit(50).all()
    works_hash = self.db.query(db.Work).filter(db.Work.hash_status == 'queued').limit(50).all()

    # TODO: select 50 rows per handler instead
    ids_apidata = [w.id for w in works_apidata if w.handler == 'wmc']
    ids_hash = [w.id for w in works_hash if w.handler == 'wmc']

    if len(ids_apidata) == 0: #and len(ids_hash) == 0:
        logger.info('nothing to do, shutting down')
        #raise WorkerShutdown()

    try:
        self.db.query(db.Work).filter(db.Work.id.in_(ids_apidata)).update({
            'apidata_status': 'processing',
        }, synchronize_session='fetch')

        self.db.query(db.Work).filter(db.Work.id.in_(ids_hash)).update({
            'hash_status': 'processing',
        }, synchronize_session='fetch')

        self.db.commit()
        return (works_apidata, works_hash)
    except:
        self.db.rollback()
        # TODO: should we actually stop here?
        # raise RevokeChainRequested(False)
    return [], []

@app.task(bind=True, base=DatabaseTask)
def finish(self, *args):
    print('finishing batch')
    try:
        self.db.commit()
    except:
        self.db.rollback()

def restart_workflow():
    workflow.apply_async()

@app.task(bind=True, base=DatabaseTask)
def restart_workflow_task(self, *args):
    restart_workflow()

@app.task(bind=True, base=DatabaseTask)
def workflow(self, *args):
    logger.info('starting batch chain')
    chain = (
        start_batch.s() |
        setup_for_processing.s() |
        wmc.process.s() |
        finish.s() |
        restart_workflow_task.s ()
    )

    #return celery.chord([chain])(restart_workflow)
    chain()

@worker_ready.connect
def main(sender=None, conf=None, **kwargs):
    logger.info('starting workflow')
    workflow.apply_async()
