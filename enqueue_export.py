#! /usr/bin/env python3

import sys

import enqueue
import wmc

if __name__ == '__main__':
    try:
        max_tasks = int(sys.argv[1])
    except (IndexError, TypeError):
        sys.exit('Usage: {} NUM_TASKS'.format(sys.argv[0]))

    enqueue.main('done', 'queued_export', wmc.export, max_tasks)
