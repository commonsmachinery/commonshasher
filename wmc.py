import urllib
import json
import tempfile
import requests
from lxml import etree
import subprocess
from datetime import datetime

from sqlalchemy.sql import bindparam

import celery

from app import app
from common import DatabaseTask
import config
import db

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


APIBASE = 'http://commons.wikimedia.org/w/api.php?format=xml&action=query&prop=imageinfo&iiprop=sha1|url|thumbmime|extmetadata|archivename&iiurlwidth=640&iilimit=1&maxlag=5'

def login():
    baseurl = "http://commons.wikimedia.org/w/"
    params  = '?action=login&lgname=%s&lgpassword=%s&format=json'% (config.WMC_USER, config.WMC_PASSWORD)

    r = requests.post(baseurl + 'api.php' + params, data={
        'lgname': config.WMC_USER,
        'lgpassword': config.WMC_PASSWORD
    })
    try:
        result = r.json()
        token = result['login']['token']
        login_params = urllib.parse.urlencode({'lgname': config.WMC_USER, 'lgpassword': config.WMC_PASSWORD, 'lgtoken': token})
    except KeyError:
        login_params = ''

    print('setting login params to...', login_params)
    return login_params


def get_metadata(filelist):
    quotedfiles = [urllib.parse.quote(x) for x in filelist]
    apirequest = '%s&titles=%s' % (APIBASE, '|'.join(quotedfiles))
    logger.debug('Requesting %s' % apirequest)

    try:
        r = requests.get(apirequest)
        apidata = etree.fromstring(r.text)
    except (etree.XMLSyntaxError, requests.exceptions.RequestException):
        return None

    filedata = {}
    for filename in filelist:
        logger.debug('Traversing API output for %s' % filename)
        filedata[filename] = {}
        try:
            if '"' in filename:
                if "'" in filename:
                    logger.warning("Xpath can't (easily) handle strings with both ' and \", ignoring: {}".format(filename))
                    continue
                                   
                node = apidata.find(".//page[@title='%s']//ii" % filename)
            else:
                node = apidata.find('.//page[@title="%s"]//ii' % filename)
                
        except SyntaxError:
            logger.warning('Syntax error on filename %s' % filename)
            continue

        if node is None:
            logger.warning('Returned invalid API data on %s' % filename)
            continue

        filedata[filename]['thumburl'] = apidata.find('.//page[@title="%s"]//ii' % filename).get('thumburl')
        if filedata[filename]['thumburl'] is None:
            logger.warning('Missing thumbnail URL')
            continue

        filedata[filename]['url'] = apidata.find('.//page[@title="%s"]//ii' % filename).get('url')

        filedata[filename]['identifier'] = apidata.find('.//page[@title="%s"]//ii' % filename).get('descriptionurl')
        filedata[filename]['sha1'] = apidata.find('.//page[@title="%s"]//ii' % filename).get('sha1')

        values = {'licenseurl': 'LicenseUrl',
            'licenseshort': 'LicenseShortName',
            'copyrighted': 'Copyrighted',
            'artist': 'Artist',
            'description': 'ImageDescription'}
        for k in values:
            rawnode = apidata.find('.//page[@title="%s"]//ii//%s' % (filename, values[k]))
            if rawnode is not None:
                filedata[filename][k] = rawnode.get('value')

        ''' Check if the file has actually been updated since last time
        we retrieved it or not. We retrieve the file if we don't
        have it in the DB or if the sha1 differ from what we have. '''

    return filedata

@app.task(name='wmc.process', bind=True, base=DatabaseTask,
          track_started=True, ignore_result=True,
          max_retries=5)
def process(self, work_ids):
    """Process a list of WMC works, given by their database IDs.  This should
    be a suitably large batch for calling the metadata API, e.g. 50 records.
    """

    if not work_ids:
        logger.warning('called without any works')
        return

    logger.info('processing: {}'.format(work_ids))

    # Set ourselves as processing these tasks
    stmt = db.Work.__table__.update().where(
        db.Work.id.in_(work_ids)
    ).where(
        db.Work.task_id == None
    ).where(
        db.Work.status == 'queued'
    ).values(task_id=self.request.id, process_start=datetime.now(), status='processing')

    # logger.info('executing: {} with {}'.format(stmt, stmt.compile().params))
    self.db.execute(stmt)
    self.db.commit()

    # Then load the work objects we got hold of
    works = self.db.query(db.Work).filter_by(
        task_id=self.request.id, status='processing'
    ).all()

    if not works:
        logger.warning('did not find any works to process')
        return

    logger.info('working on {} objects'.format(len(works)))

    filelist = [work.url for work in works]
    apidata = get_metadata(filelist)

    if not apidata:
        logger.warning('error getting apidata for {}, retrying'.format(work_ids))
        # Failed getting works, clean up and retry later
        stmt = db.Work.__table__.update().where(
            db.Work.task_id == self.request.id
        ).where(
            db.Work.status == 'processing'
        ).values(task_id=None, status='queued')
        self.db.execute(stmt)
        self.db.commit()

        raise self.retry()


    for work in works:
        data = apidata[work.url]
        thumburl = data.get('thumburl', None)

        work.apidata = json.dumps(data)
        self.db.commit()

        if thumburl and not work.hash:
            # queue a hashing task for this work
            update_hash.apply_async((work.id, thumburl))
        else:
            # nothing to hash, so we are done
            work.status = 'done'
            self.db.commit()


@app.task(name='wmc.update_hash', bind=True, base=DatabaseTask, rate_limit=config.WMC_RATE_LIMIT,
          track_started=True, ignore_result=True)
def update_hash(self, work_id, image_url):
    work = self.db.query(db.Work).filter_by(id=work_id).one()

    tfile = tempfile.NamedTemporaryFile()
    logger.debug('Retrieving %s to %s' % (image_url, tfile.name))

    try:
        r = requests.get(image_url)
        tfile.write(r.content)
    except requests.exceptions.RequestException:
        logger.warning('Unable to retrieve %s' % image_url)
        work.status = 'error'
        self.db.commit()
        return

    try:
        retval = subprocess.check_output([config.BLOCKHASH_COMMAND, tfile.name], universal_newlines=True)
    except (subprocess.CalledProcessError, BlockingIOError):
        logger.debug('%s not supported by blockhash.py' % image_url)
        work.status = 'error'
        self.db.commit()
        return

    hash = retval.partition(' ')[2]
    hash = hash.strip()
    logger.debug('Blockhash from external cmd: %s' % hash)

    tfile.close()

    if hash is not None:
        work.hash = hash

    work.status = 'done'
    self.db.commit()
