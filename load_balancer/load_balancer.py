import json
import logging
import random
from rich import print
import socket
import threading
import time

WORKER_SELECTION_MODE = 'roundrobin'
WORKERS = [
    ('127.0.0.1', 6868),
    ('127.0.0.1', 6869),
    ('127.0.0.1', 6870),
    ('127.0.0.1', 6871),
]

job_list = []

last_used_worker_index_lock = threading.Lock()
last_used_worker_index = -1

job_list: list[object] = []

jobs_completed_lock = threading.Lock()
jobs_completed = 0

def generate_jobs():
    with open('weights.csv', 'r') as f:
        weights = f.readline().split(',')
        for i in range(len(weights)):
            job_list.append({
                'id': i + 1,
                'weight': int(weights[i]),
            })

def distribute_jobs():
    waits = []
    with open('waits.csv', 'r') as f:
        waits = f.readline().split(',')

    for (i, job) in enumerate(job_list):
        thread = threading.Thread(
            target = threaded_pass_to_worker,
            args = (job,),
            daemon = True
        )
        thread.start()
        time.sleep(int(waits[i]) / 1000)

    # wait for outstanding jobs to complete
    while len(job_list) > jobs_completed:
        time.sleep(1)

def threaded_pass_to_worker(job: object):
    global jobs_completed
    (host, port) = select_worker()
    print(f'[yellow]Sending job {job["id"]} with weight {job["weight"]} to worker {host}:{port}...[/yellow]')
    encoded_job = json.dumps(job).encode('utf-8')
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((host, port))
            sock.sendall(bytes([len(encoded_job)]))
            sock.sendall(encoded_job)
            send_time = time.time_ns()
            print(f'[blue]Waiting for response for job {job["id"]}...[/blue]')
            response = sock.recv(1)
            print(f'[blue]Got response {"SUCCESS" if response == b"1" else "FAILURE"} for job {job["id"]}.[/blue]')
            recv_time = time.time_ns()
            sock.close()
            logging.debug(f'{job["id"]};{job["weight"]};{send_time};{recv_time}')
            with jobs_completed_lock:
                jobs_completed += 1
    except ConnectionError:
        print(f'[red]Worker {host}:{port} is offline[/red]')

def select_worker():
    global last_used_worker_index
    if WORKER_SELECTION_MODE == 'random':
        return random.choice(WORKERS)
    elif WORKER_SELECTION_MODE == 'roundrobin':
        with last_used_worker_index_lock:
            last_used_worker_index = last_used_worker_index + 1 if last_used_worker_index + 1 < len(WORKERS) else 0
            return WORKERS[last_used_worker_index]

if __name__ == '__main__':
    logging.basicConfig(
        filename = f'{time.strftime("%Y-%m-%d-%H-%M-%S")}-{WORKER_SELECTION_MODE}.log',
        format = '%(asctime)s;%(message)s',
        level = logging.DEBUG,
    )
    print(f'[blue bold]Load Balancer ({WORKER_SELECTION_MODE})[/blue bold]')
    generate_jobs()
    distribute_jobs()
