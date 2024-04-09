#!/usr/bin/env python
import argparse
import os
import sys
import etcd3
import json

etcd = None

def process(args):
    input_file = args.input
    output_file = args.output
    task_name = args.task_name if args.task_name is not None else f'{input_file}->{output_file}'

    task_key = f'/services/tasks/vectorizer/{task_name}'
    task_data = {'status': 'pending', 'init': {'input_file': input_file, 'output_file': output_file}}

    etcd.put(task_key, json.dumps(task_data))

    print(f'created task: `{task_name}`')

def status_line(key, state):
    task_id = key[len('/services/tasks/vectorizer/'):]
    status_line = f'{task_id} ({state["init"]["input_file"]}->{state["init"]["output_file"]}): {state["status"]}'
    progress = state.get('progress')
    if progress:
        rate = 'unknown'
        avg_rate = 'unknown'
        if 'rate' in progress:
            rate = f'{progress["rate"]:.2f}'
        if 'avg_rate' in progress:
            avg_rate = f'{progress["avg_rate"]:.2f}'
        status_line += f', progress: {progress["count"]}/{progress["total"]}, rate: {rate} (avg {avg_rate})'

    return status_line

def status(args):
    task_name = args.task_name
    task_key = f'/services/tasks/vectorizer/{task_name}'
    (v,_) = etcd.get(task_key)

    task_data = json.loads(v)
    if args.raw:
        print(json.dumps(task_data, indent=4))
    elif task_data['status'] == 'error':
        print(status_line(task_key, task_data))
        print(task_data['error'])
    else:
        print(status_line(task_data, v))

def list_tasks(args):
    for (v,kv) in etcd.get_prefix('/services/tasks/vectorizer/'):
        key = kv.key.decode('utf-8')
        task_data = json.loads(v)
        print(status_line(key, task_data))

def pause(args):
    task_name = args.task_name
    task_key = f'/services/tasks/vectorizer/{task_name}'
    queue = f'/services/queue/vectorizer/{task_name}'
    claim = f'/services/claims/vectorizer/{task_name}'
    interrupt = f'/services/interrupts/vectorizer/{task_name}'
    (task_data_bytes,_) = etcd.get(task_key)
    task_data = json.loads(task_data_bytes)
    if task_data['status'] == 'running':
        # this is a live interrupt
        interrupt_key = f'/services/interrupt/vectorizer/{task_name}'
        etcd.put(interrupt_key, 'pause')
    elif task_data['status'] == 'resuming':
        # we're trying to resume but changed our mind. lets pause again (as long as nothing changed)
        task_data['status'] = 'paused'
        (success, _) = etcd.transaction(
            compare=[
                etcd.transactions.value(task_key) == task_data_bytes,
                etcd.transactions.version(claim) == 0, # this should always be true if the above is true, but let's check anyway
            ],
            success=[
                etcd.transactions.put(task_key, json.dumps(task_data)),
                etcd.transactions.delete(interrupt), # these can't be any good
                etcd.transactions.delete(queue) # don't want to get this from queue anyway
            ],
            failure=[]
        )
        if not success:
            print(f'pausing a resuming task failed')
            sys.exit(1)
    else:
        print(f'cannot pause task in state {task_data["state"]}')
        sys.exit(1)

def resume(args):
    task_name = args.task_name
    task_key = f'/services/tasks/vectorizer/{task_name}'
    (state_bytes, _) = etcd.get(task_key)
    state = json.loads(state_bytes)
    if state['status']  != 'paused':
        print('task is not paused')
        sys.exit(1)

    state['status'] = 'resuming'
    if not etcd.replace(task_key, state_bytes, json.dumps(state)):
        print('resume failed')
        sys.exit(1)


def retry(args):
    task_name = args.task_name
    task_key = f'/services/tasks/vectorizer/{task_name}'
    (state_bytes, _) = etcd.get(task_key)
    state = json.loads(state_bytes)
    if state['status']  != 'error':
        print('task is not in an error state')
        sys.exit(1)

    del state['error']

    state['status'] = 'resuming'
    if not etcd.replace(task_key, state_bytes, json.dumps(state)):
        print('retry failed')
        sys.exit(1)



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--etcd', help='hostname of etcd server')
    subparsers = parser.add_subparsers(dest='subcommand')

    process_parser = subparsers.add_parser('process', help='process a json-lines file into a vectors file')
    process_parser.add_argument('input', type=str, help='Input file')
    process_parser.add_argument('output', type=str, help='Output file')
    process_parser.add_argument('--task-name', type=str, help='Task name')

    status_parser = subparsers.add_parser('status', help='retrieve the status of a task')
    status_parser.add_argument('task_name', type=str, help='task name to query')
    status_parser.add_argument('--raw', action='store_true', help='raw output')

    list_parser = subparsers.add_parser('list', help='list all tasks')

    pause_parser = subparsers.add_parser('pause', help='pause task')
    pause_parser.add_argument('task_name', type=str, help='task name to pause')

    resume_parser = subparsers.add_parser('resume', help='resume task')
    resume_parser.add_argument('task_name', type=str, help='task name to resume')

    retry_parser = subparsers.add_parser('retry', help='retry errored task')
    retry_parser.add_argument('task_name', type=str, help='task name to retry')

    args = parser.parse_args()
    host = args.etcd
    if host is None:
        host = os.getenv('ETCD_HOST')
    if host is None:
        etcd = etcd3.client()
    else:
        etcd = etcd3.client(host=host)

    match args.subcommand:
        case 'process':
            process(args)
        case 'status':
            status(args)
        case 'list':
            list_tasks(args)
        case 'pause':
            pause(args)
        case 'resume':
            resume(args)
        case 'retry':
            retry(args)
        case _:
            parser.print_help()
