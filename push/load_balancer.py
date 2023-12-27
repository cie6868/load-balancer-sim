#!/usr/bin/env python3

# Usage: ./load_balancer.py [algorithm (roundrobin/random)] [mean processing time of jobs (ms)] [mean interval between jobs (ms)]

import json
import logging
import os
from pathlib import Path
import random
from rich import print
import socket
import sys
import threading
import time

sys.path.insert(1, os.path.join(sys.path[0], '..'))

import config
from job_generator import generate_jobs

job_list = []

last_used_worker_index_var_lock = threading.Lock()
last_used_worker_index = -1

jobs_completed_var_lock = threading.Lock()
jobs_completed = 0

def load_jobs():
    with open('job_weights.csv', 'r') as f:
        job_weights = f.readline().split(',')
        for i in range(len(job_weights)):
            job_list.append({
                'id': i + 1,
                'weight': int(job_weights[i]),
            })

def distribute_jobs():
    job_intervals = []
    with open('job_intervals.csv', 'r') as f:
        job_intervals = f.readline().split(',')

    logging.debug('job_id,job_weight,worker,queued_time,response_time')

    for (i, job) in enumerate(job_list):
        thread = threading.Thread(
            target = threaded_pass_to_worker,
            args = (job,),
            daemon = True
        )
        thread.start()
        time.sleep(int(job_intervals[i]) / 1000)

    # wait for outstanding jobs to complete
    while len(job_list) > jobs_completed:
        time.sleep(1)

def threaded_pass_to_worker(job: object):
    global jobs_completed
    (host, port) = select_worker()
    print(f'[yellow]Sending job {job["id"]} with weight {job["weight"]} to worker {port}...[/yellow]')
    encoded_job = json.dumps(job).encode('utf-8')
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((host, port))
            sock.sendall(bytes([len(encoded_job)]))
            sock.sendall(encoded_job)
            queued_time = time.time_ns()

            response = sock.recv(1)
            recv_time = time.time_ns()
            print(f'[yellow]Got response {"SUCCESS" if response == b"1" else "FAILURE"} for job {job["id"]}.[/yellow]')

            sock.close()

            logging.debug(f'{job["id"]},{job["weight"]},{port},{queued_time},{recv_time}')

            with jobs_completed_var_lock:
                jobs_completed += 1
    except ConnectionError:
        print('[red bold]ConnectionError[/red bold]')
        print(f'[red]Worker {port} is offline[/red]')

def select_worker():
    global last_used_worker_index

    algo = sys.argv[1]
    if algo == 'random':
        return random.choice(config.LB_WORKERS)
    elif algo == 'roundrobin':
        with last_used_worker_index_var_lock:
            last_used_worker_index = last_used_worker_index + 1 if last_used_worker_index + 1 < len(config.LB_WORKERS) else 0
            return config.LB_WORKERS[last_used_worker_index]

if __name__ == '__main__':
    algo = sys.argv[1]
    job_weight_mean = int(sys.argv[2])
    job_interval_mean = int(sys.argv[3])

    Path(config.LB_OUTPUT_FOLDER).mkdir(parents=True, exist_ok=True)

    time_str = time.strftime("%Y-%m-%d-%H-%M-%S")
    log_filename = f'{config.LB_OUTPUT_FOLDER}/{time_str}-{algo}-{job_weight_mean}-{job_interval_mean}.csv'
    logging.basicConfig(
        filename = log_filename,
        format = '%(message)s',
        level = logging.DEBUG,
    )
    print(f'[blue bold]Load Balancer ({algo})[/blue bold]')

    num_jobs = int(config.LB_RUNTIME_SECONDS * 1000 / job_interval_mean)
    print(f'[blue]Will dispatch {num_jobs} over {config.LB_RUNTIME_SECONDS} seconds.[/blue]')

    generate_jobs(num_jobs, job_weight_mean, job_interval_mean)

    load_jobs()

    distribute_jobs()

    print(f'[blue]Data written to {log_filename}[/blue]')
