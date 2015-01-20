
import celery
from celery import Celery

import config

app = Celery('commonshasher', broker=config.BROKER_URL)

app.conf.update(
    CELERY_ROUTES = {
        'wmc.process': { 'queue': 'wmc.apidata' },
        'wmc.update_hash': { 'queue': 'wmc.hash' },
        'wmc.export': { 'queue': 'wmc.export' },
    },
)
