#! /usr/bin/env python3

import sys

from sqlalchemy.sql import select

import hasher
import wmc
import db

def main():
    try:
        max_tasks = int(sys.argv[1])
    except (IndexError, TypeError):
        sys.exit('Usage: {} NUM_TASKS'.format(sys.argv[0]))

    session = db.open_session()

    while max_tasks > 0:
        # Find the next bunch of unqueued works,
        stmt = select([db.Work.id]).where(
            db.Work.status=='loaded'
        ).limit(min(max_tasks, 50))
        work_ids = [row[0] for row in session.execute(stmt).fetchall()]

        if not work_ids:
            print('No more unqueued works in the database')
            return

        max_tasks -= len(work_ids)

        print('Queuing {} works'.format(len(work_ids)))
        print(work_ids)

        # We expect that this job is not run in parallel, so we don't
        # have to worry about the status changing under our feet
        try:
            stmt = db.Work.__table__.update().where(
                db.Work.id.in_(work_ids)
            ).values(status='queued')
            session.execute(stmt)

            wmc.process.apply_async((work_ids, ))
        except:
            session.rollback()
            raise
        else:
            session.commit()

if __name__ == '__main__':
    main()

