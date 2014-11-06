#! /usr/bin/env python3

import json
import argparse

from sqlalchemy.sql import select

import wmc
import db

def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-c', '--count', dest="count", type=int, help='Maximum number of works to export')
    argparser.add_argument('outfile')
    args = argparser.parse_args()

    filename = args.outfile
    outfile = open(filename, 'w')

    if args.count:
        maxworks = args.count
    else:
        maxworks = -1

    db_session = db.open_session()

    count = 0

    # Find the next bunch of finished works
    stmt = select([db.Work]).where(
        db.Work.status=='done'
    )
    works = db_session.execute(stmt).fetchall()

    for work in works:
        if count == maxworks:
            break

        if work.handler == 'wmc':
            pkg = wmc.export_work(work)
        else:
            raise RuntimeError('Unknown work handler: %s' % work.handler)

        outfile.write(json.dumps(pkg))
        outfile.write("\n")

        count += 1

    print('processed records: {}'.format(count))

if __name__ == "__main__":
    main()
