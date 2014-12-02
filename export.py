#! /usr/bin/env python3

import json
import argparse
import lzma
from datetime import datetime

from sqlalchemy.sql import select

import wmc
import db

def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-l', '--limit', dest="limit", type=int)
    argparser.add_argument('-o', '--offset', dest="offset", type=int)
    argparser.add_argument('--compress', dest='compress', action='store_true', help='Compress output using LZMA')
    argparser.add_argument('outfile')
    args = argparser.parse_args()

    filename = args.outfile

    if args.compress:
        outfile = lzma.open(filename, 'w')
    else:
        outfile = open(filename, 'w')

    error_filename = 'errors_export_' + datetime.now().isoformat() + '.txt'
    error_file = None

    db_session = db.open_session()

    # Find the next bunch of finished works
    stmt = select([db.Work]).where(
        (db.Work.status == 'done') &
        (db.Work.hashm4 != None) # TODO: use 'hash' when hashm4 is renamed back
    )

    if args.limit:
        stmt = stmt.limit(args.limit)
    if args.offset:
        stmt = stmt.offset(args.offset)
    works = db_session.connection().execution_options(stream_results=True).execute(stmt)

    count = 0

    for work in works:
        if work.handler == 'wmc':
            try:
                pkg = wmc.export_work(work)
            except RuntimeError as e:
                print('Error exporting work {0}. Work written to error file'.format(count))
                if error_file is None:
                    error_file = open(error_filename, 'w')
                error_file.write(str(work))
                error_file.write('\n')
                continue
        else:
            raise RuntimeError('Unknown work handler: %s' % work.handler)

        if args.compress:
            outfile.write(bytes(json.dumps(pkg), 'utf-8'))
            outfile.write(bytes('\n', 'utf-8'))
        else:
            outfile.write(json.dumps(pkg))
            outfile.write('\n')

        count += 1

    print('processed records: {}'.format(count))

    outfile.close()
    if error_file is not None:
        error_file.close()

if __name__ == "__main__":
    main()
