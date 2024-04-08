#!/usr/bin/env python
import argparse
import os
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

def status_line(key, val):
    state = json.loads(val)

    task_id = key[len('/services/tasks/vectorizer/'):]
    status_line = f'{task_id}: {state["status"]}'
    progress = state.get('progress')
    if progress:
        status_line += f' progress: {str(progress)}'

    return status_line

def status(args):
    task_name = args.task_name
    task_key = f'/services/tasks/vectorizer/{task_name}'
    (v,_) = etcd.get(task_key)

    if args.raw:
        task_data = json.loads(v)
        print(json.dumps(task_data, indent=4))
    else:
        print(status_line(task_key, v))

def list_tasks(args):
    for (v,kv) in etcd.get_prefix('/services/tasks/vectorizer/'):
        key = kv.key.decode('utf-8')
        print(status_line(key, v))

def pause(args):
    task_name = args.task_name
    interrupt_key = f'/services/interrupt/vectorizer/{task_name}'
    etcd.put(interrupt_key, 'pause')

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

    pause_parser = subparsers.add_parser('pause', help='list all tasks')
    pause_parser.add_argument('task_name', type=str, help='task name to query')

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
        case _:
            parser.print_help()
