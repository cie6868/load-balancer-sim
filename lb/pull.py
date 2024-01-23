#!/usr/bin/env python3

# Usage: ./pull.py [runtime (min)] [mean processing time of jobs (ms)] [mean interval between jobs (ms)] [lb_port]

import json
import logging
from pathlib import Path
import queue
from rich import print
import socket
import sys
import threading
import time

from job_generator import generate_jobs

LB_OUTPUT_FOLDER = 'outputs'
LB_QUEUE_MAX_SIZE = 1000

job_list = []
job_queue = queue.Queue(LB_QUEUE_MAX_SIZE)

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

def distribute_jobs(lb_port: int):
    job_intervals = []
    with open('job_intervals.csv', 'r') as f:
        job_intervals = f.readline().split(',')

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(('0.0.0.0', lb_port))

        logging.debug('job_id,job_weight,worker,queued_time,response_time')

        print(f'[yellow]Listening for job requests on port {lb_port}...[/yellow]')
        sock.listen()

        thread = threading.Thread(
            target = threaded_listen_for_job_requests,
            args = (sock,),
            daemon = True
        )
        thread.start()

        for (i, job) in enumerate(job_list):
            queued_time = time.time_ns()
            job_queue.put((job, queued_time))

            print(f'[yellow]Queued job ID {job["id"]} and weight {job["weight"]}.[/yellow]')

            time.sleep(int(job_intervals[i]) / 1000)

        # wait for outstanding jobs to complete
        while len(job_list) > jobs_completed:
            time.sleep(1)

        thread.join()

def threaded_listen_for_job_requests(sock: socket.socket):
    try:
        while len(job_list) > jobs_completed:
            (con, con_addr) = sock.accept()
            print(f'[blue]Connection from {con_addr}.[/blue]')

            thread = threading.Thread(
                target = threaded_pass_to_worker,
                args = (con, con_addr,),
                daemon = True
            )
            thread.start()

    except KeyboardInterrupt:
        print('[red bold]KeyboardInterrupt[/red bold]')
        sock.close()

    print('[yellow]Stopped listening for job requests.[/yellow]')
    print(f'Jobs completed: {jobs_completed}')

def threaded_pass_to_worker(con: socket.socket, worker_addr):
    global jobs_completed

    job, queued_time = job_queue.get(block = True)
    encoded_job = json.dumps(job).encode('utf-8')

    try:
        print(f'[blue]Sending job {job["id"]} with weight {job["weight"]} to worker {worker_id}...[/blue]')

        con.sendall(bytes([len(encoded_job)]))
        con.sendall(encoded_job)

        worker_id = int.from_bytes(con.recv(2), 'big')
        response = con.recv(1)
        recv_time = time.time_ns()

        print(f'[blue]Got response {"SUCCESS" if response == b"1" else "FAILURE"} for job {job["id"]}.[/blue]')

        con.close()

        logging.debug(f'{job["id"]},{job["weight"]},{worker_id},{queued_time},{recv_time}')

        with jobs_completed_var_lock:
            jobs_completed += 1

    except ConnectionError:
        print('[red bold]ConnectionError[/red bold]')
        print(f'[red]Worker {worker_addr} is offline[/red]')

    except KeyboardInterrupt:
        print('[red bold]KeyboardInterrupt[/red bold]')
        con.close()

if __name__ == '__main__':
    if len(sys.argv) > 4 and sys.argv[2].isnumeric() and sys.argv[3].isnumeric() and sys.argv[4].isnumeric():
        runtime_min = float(sys.argv[1])
        job_weight_mean = int(sys.argv[2])
        job_interval_mean = int(sys.argv[3])
        lb_port = int(sys.argv[4])

        Path(LB_OUTPUT_FOLDER).mkdir(parents=True, exist_ok=True)

        time_str = time.strftime("%Y%m%d-%H%M%S")
        log_filename = f'{LB_OUTPUT_FOLDER}/{time_str}-pull-{runtime_min}m-{job_weight_mean}-{job_interval_mean}.csv'
        logging.basicConfig(
            filename = log_filename,
            format = '%(message)s',
            level = logging.DEBUG,
        )
        print(f'[blue bold]Load Balancer (pull)[/blue bold]')

        num_jobs = int(runtime_min * 60 * 1000 / job_interval_mean)
        print(f'[blue]Will dispatch {num_jobs} over {runtime_min} minutes.[/blue]')

        generate_jobs(num_jobs, job_weight_mean, job_interval_mean)

        load_jobs()

        distribute_jobs(lb_port)

        print(f'[blue]Data written to {log_filename}[/blue]')
    else:
        print('Invalid arugments.')
