from etcd_task import TaskQueue
import vectorize
import sys
import json

def retrieve_identity():
    return 'dummy'

def start_(task):
    print('really gonna start now!')
    task.start()
    print('started!')
    init = task.init()
    input_file = init['input_file']
    output_file = init['output_file']

    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")

    chunk = []
    count = 0
    with open(output_file, 'w') as output_fp:
        with open(input_file, 'r') as input_fp:
            for line in input_fp:
                json_str = json.loads(line)
                chunk.append(json_str)
                if len(chunk) == 100:
                    task.alive()
                    vectorize.process_chunk(chunk, output_fp)
                    count += len(chunk)
                    print(f'setting progress to {count}')
                    task.set_progress(count)
                    chunk = []
        if len(chunk) != 0:
              vectorize.process_chunk(chunk, output_fp)
              count += len(chunk)
              task.set_progress(count)
        task.finish(count)

def start(task):
    try:
        start_(task)
    except Exception as e:
        task.finish_error(str(e))


def resume(task):
    pass

if __name__ == '__main__':
    identity = retrieve_identity()
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
            case _:
                sys.stderr.write(f'cannot process task with status {task.status()}\n')
