from etcd_task import TaskQueue, TaskInterrupted
import vectorize
import sys
import json
import socket
import argparse
import os
import traceback

identity = None
directory = None

def retrieve_identity():
    from_env = os.getenv('VECTORIZER_IDENTITY')
    return from_env if from_env is not None else socket.getfqdn()

def resolve_path(path):
    rootdir = os.path.abspath(directory)
    normalized = os.path.normpath(f'{rootdir}/{path}')
    if not normalized.startswith(rootdir):
        raise ValueError(f'path {path} is invalid')

    return normalized

def start_(task, truncate=0, skip=0):
    init = task.init()
    input_file = resolve_path(init['input_file'])
    output_file = resolve_path(init['output_file'])

    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")

    chunk = []
    count = skip
    with open(output_file, 'a+') as output_fp:
        # truncate to a safe known size
        output_fp.truncate(truncate)
        output_fp.seek(0, os.SEEK_END)

        with open(input_file, 'r') as input_fp:
            for line in input_fp:
                # skip already processed lines
                if skip != 0:
                    skip -= 1
                    continue

                json_str = json.loads(line)
                chunk.append(json_str)
                if len(chunk) == 100:
                    task.alive()
                    vectorize.process_chunk(chunk, output_fp)
                    count += len(chunk)
                    task.set_progress(count)
                    chunk = []
        if len(chunk) != 0:
              vectorize.process_chunk(chunk, output_fp)
              output_fp.flush()
              count += len(chunk)
              task.set_progress(count)

        os.fsync(output_fp.fileno())
        task.finish(count)

def start(task):
    task.start()
    try:
        start_(task)
    except TaskInterrupted as e:
        pass
    except Exception as e:
        stack_trace = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        task.finish_error(stack_trace)

def resume(task):
    # We have to figure out where we left off
    # This is determined by the current file size. rounding that down
    # to the nearest multiple of the vector size gets us a reliable
    # count. This might be lower than the number in progress!
    if task.status() == 'resuming':
        task.resume()
    init = task.init()
    size = os.path.getsize(resolve_path(init['output_file']))
    count = size // 4096
    truncate_to = count * 4096

    print(f'resuming after having already vectorized {count}')

    task.set_progress(count)

    try:
        start_(task, truncate=truncate_to, skip=count)
    except TaskInterrupted as e:
        pass
    except Exception as e:
        task.finish_error(str(e))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--etcd', help='hostname of etcd server')
    parser.add_argument('--identity', help='the identity this worker will use when claiming tasks')
    parser.add_argument('--directory', help='the directory where files are to be found')
    args = parser.parse_args()
    identity = args.identity if args.identity is not None else retrieve_identity()

    directory = args.directory
    if directory is None:
        directory = os.getenv('VECTORIZER_DIRECTORY')

    etcd = args.etcd
    if etcd is None:
        etcd = os.getenv('ETCD_HOST')

    if etcd is not None:
        queue = TaskQueue('vectorizer', identity, host=etcd)
    else:
        queue = TaskQueue('vectorizer', identity)

    while True:
        task = queue.next_task()
        print('wow a task: ' + task.status())
        match task.status():
            case 'pending':
                print('starting..')
                start(task)
            case 'running':
                resume(task)
            case 'resuming':
                resume(task)
            case _:
                sys.stderr.write(f'cannot process task with status {task.status()}\n')
