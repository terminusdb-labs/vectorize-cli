#!/usr/bin/env python
import argparse
import etcd3
import json
import threading
import os
from queue import Queue

etcd = None
CLAIMS = '/services/claims/'
TASKS = '/services/tasks/'
QUEUE = '/services/queue/'

def task_to_claim(task):
    task_id = task[len(TASKS):]
    return f'{CLAIMS}{task_id}'

def task_to_queue(task):
    task_id = task[len(TASKS):]
    return f'{QUEUE}{task_id}'

def claim_to_task(claim):
    task_id = claim[len(CLAIMS):]
    return f'{TASKS}{task_id}'

def runnable_status(status):
    return status in ['pending', 'running']

def iterator_to_queue(it, q):
    for v in it:
        q.put(v)

def enqueue(task_key):
    claim = task_to_claim(task_key)
    queue = task_to_queue(task_key)
    # requeue stuff
    print(etcd.transaction(
        compare=[
            etcd.transactions.version(claim) == 0,
            etcd.transactions.version(queue) == 0,
        ],
        success=[
            etcd.transactions.put(queue, '')
        ],
        failure=[]
    ))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--etcd', help='hostname of etcd server')
    args = parser.parse_args()
    host = args.etcd
    if host is None:
        host = os.getenv('ETCD_HOST')
    if host is None:
        etcd = etcd3.client()
    else:
        etcd = etcd3.client(host=host)

    (tasks_watch, tasks_watch_cancel) = etcd.watch_prefix(TASKS)
    (claims_watch, claims_watch_cancel) = etcd.watch_prefix(CLAIMS)
    q = Queue()
    tasks_watch_thread = threading.Thread(target=iterator_to_queue, args=(tasks_watch, q))
    claims_watch_thread = threading.Thread(target=iterator_to_queue, args=(claims_watch, q))
    tasks_watch_thread.start()
    claims_watch_thread.start()
    try:
        result = etcd.get_prefix(TASKS, sort_order='ascend', sort_target='create')
        for (v, kv) in result:
            state = json.loads(v)
            print(state)
            if not runnable_status(state['status']):
                continue

            task_key = kv.key.decode('utf-8')
            enqueue(task_key)

        # Now that any stragglers are cleared up, it is time to start relying on the watch
        print('time for work')
        while True:
            event = q.get()
            print(event)
            # is it a disappearing claim?
            if isinstance(event, etcd3.events.DeleteEvent):
                key = event.key.decode('utf-8')
                if key.startswith(CLAIMS):
                    task_key = claim_to_task(key)
                    (v,_) = etcd.get(task_key)
                    state = json.loads(v)
                    if runnable_status(state['status']):
                        # claim disappeared, but this task is still runnable. requeue!
                        enqueue(task_key)
            # is it a new task?
            elif isinstance(event, etcd3.events.PutEvent):
                key = event.key.decode('utf-8')
                if key.startswith(TASKS):
                    state = json.loads(event.value)
                    if runnable_status(state['status']):
                        enqueue(key)

    finally:
        # todo proper cleanup here
        pass