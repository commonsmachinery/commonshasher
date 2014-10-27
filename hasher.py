#! /usr/bin/env python3

import celery
from celery import Celery
from celery.exceptions import WorkerShutdown
from celery.signals import worker_ready

import db
import wmc
import config
from common import DatabaseTask

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

app = Celery('commonshasher', broker='amqp://guest@localhost//')

app.conf.update(
    BROKER_URL=config.BROKER_URL,
    #CELERY_ALWAYS_EAGER=True,
    #CELERYD_CONCURRENCY=1,
)

