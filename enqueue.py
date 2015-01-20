#! /usr/bin/env python3

import sys

from sqlalchemy.sql import select

import hasher
import wmc
import db

def main(old_status, new_status, task, max_tasks):
    """
    Flag works for processing and call appropriate task.

    Arguments:
    old_status -- status to select works
    new_status -- status to update works
    task -- task to pass work ids for further processing (process or export)
    max_tasks -- maximum number of tasks to enqueue
    """

    # TODO: choose handlers according to handler column

    session = db.open_session()

    count = 0
    diff = 0
    num_tasks = max_tasks

    while num_tasks > 0:
        # Find the next bunch of unqueued works,
        stmt = select([db.Work.id]).where(
            db.Work.status==old_status
        ).limit(min(num_tasks, 50))
        work_ids = [row[0] for row in session.execute(stmt).fetchall()]

        if not work_ids:
            print('No more unqueued works in the database')
            return

        num_tasks -= len(work_ids)
        count += len(work_ids)
        diff += len(work_ids)

        if diff >= 10000:
            print('Queued works: {}'.format(count))
            diff = 0

        # We expect that this job is not run in parallel, so we don't
        # have to worry about the status changing under our feet
        try:
            stmt = db.Work.__table__.update().where(
                db.Work.id.in_(work_ids)
            ).values(status=new_status)
            session.execute(stmt)

            task.apply_async((work_ids, ))
        except:
            session.rollback()
            raise
        else:
            session.commit()

if __name__ == '__main__':
    try:
        max_tasks = int(sys.argv[1])
    except (IndexError, TypeError):
        sys.exit('Usage: {} NUM_TASKS'.format(sys.argv[0]))

    main('loaded', 'queued', wmc.process, max_tasks)
