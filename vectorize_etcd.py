from etcd_task import TaskQueue, TaskInterrupted
import vectorize
import sys
import json
import socket
import argparse
import os

def retrieve_identity():
    return socket.getfqdn()

def start_(task, truncate=None, skip=0):
    init = task.init()
    input_file = init['input_file']
    output_file = init['output_file']

    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")

    chunk = []
    count = 0
    with open(output_file, 'w') as output_fp:
        # truncate to a safe known size
        if truncate:
            output_fp.truncate(truncate)

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
        task.finish_error(str(e))

def resume(task):
    # We have to figure out where we left off
    # This is determined by the current file size. rounding that down
    # to the nearest multiple of the vector size gets us a reliable
    # count. This might be lower than the number in progress!
    if task.status() == 'paused':
        task.resume()
    init = task.init()
    size = os.path.getsize(init['output_file'])
    print(f'size of {init["output_file"]}: {size}')
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
    args = parser.parse_args()
    if args.identity is None:
        identity = retrieve_identity()
    else:
        identity = args.identity

    if args.etcd:
        queue = TaskQueue('vectorizer', identity, host=args.etcd)
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
            case 'paused':
                resume(task)
            case _:
                sys.stderr.write(f'cannot process task with status {task.status()}\n')
