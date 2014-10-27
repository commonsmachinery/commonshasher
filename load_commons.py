#! /usr/bin/python3

import argparse
import bz2
from lxml import etree
import db

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def main():
    argparser = argparse.ArgumentParser()

    argparser.add_argument('-v', '--verbose', action='store_true')
    argparser.add_argument('-i', '--input', dest="file", required=True, help='A Commons dump file (bz2 compressed)')
    argparser.add_argument('-c', '--count', dest="count", type=int, help='Maximum number of object to add to database')

    args = argparser.parse_args()

    filename = args.file

    if args.count:
        maxworks = args.count
    else:
        maxworks = -1

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    reader = bz2.BZ2File(filename, 'r')
    dump = etree.iterparse(reader, events=('end',))

    db_session = db.open_session()

    count = 0

    for event, elem in dump:
        if elem.tag == '{http://www.mediawiki.org/xml/export-0.9/}page' and elem.findtext('.//{http://www.mediawiki.org/xml/export-0.9/}title').startswith('File:'):
            filename = elem.findtext('.//{http://www.mediawiki.org/xml/export-0.9/}title')

            # TODO: query last_updated
            work_record = db.Work("wmc", filename)
            db_session.add(work_record)

            count += 1
            if count == maxworks:
                break

            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

    db_session.commit()

if __name__ == "__main__":
    main()
