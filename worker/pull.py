#!/usr/bin/env python3

# Usage: ./pull.py [worker_id] [power] [lb_hostname] [lb_port]

import json
from rich import print
import socket
import sys
import threading
import time

WORKER_THREAD_COUNT = 1

jobs_completed_lock = threading.Lock()
jobs_completed = 0

def start_processing_threads(worker_id: int, power: int, lb_hostname: str, lb_port: int):
    print(f'[blue]Thread count is {WORKER_THREAD_COUNT}.[/blue]')
    print(f'[blue]Power is {power}.[/blue]')

    threads = []
    for i in range(WORKER_THREAD_COUNT):
        thread = threading.Thread(
            target = processing_thread,
            args = (i + 1, worker_id, power, lb_hostname, lb_port,),
            daemon = True
        )
        thread.start()
        threads.append(thread)

    for t in threads:
        t.join()

def processing_thread(thread_id: int, worker_id: int, power: int, lb_hostname: str, lb_port: int):
    global jobs_completed

    print(f'[blue]Started processing thread {thread_id}...[/blue]')
    try:
        while True:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as con:
                    con.connect((lb_hostname, lb_port))

                    con.sendall(int.to_bytes(worker_id, 2, 'big'))

                    message_length = int.from_bytes(con.recv(1), 'big')
                    encoded_job_bytes = con.recv(message_length)
                    job = json.loads(encoded_job_bytes.decode('utf-8'))

                    process_job(job, power)

                    print(f'[blue]Responding to job {job["id"]}.[/blue]')

                    con.sendall(b'1')
                    con.close()

                    with jobs_completed_lock:
                        jobs_completed += 1

                print(f'[blue]Completed job {job["id"]}.[/blue]')
            except json.JSONDecodeError:
                print('[red bold]JSONDecodeError[/red bold]')
                print(f'[red]Load Balancer returned invalid job.[/red]')

                time.sleep(0.5)

            except ConnectionError:
                time.sleep(0.5)

    except (KeyboardInterrupt, SystemExit):
        print('[red bold]KeyboardInterrupt / SystemExit[/red bold]')
        print(f'[blue]Stopped processing on thread {thread_id}.[/blue]')

def process_job(job: object, power: int):
    print(f'[blue]Processing job {job["id"]} with weight {job["weight"]}...[/blue]')

    sleep_seconds = (job['weight'] / 1000)
    time.sleep(sleep_seconds / (power / 100))

if __name__ == '__main__':
    if len(sys.argv) > 4 and sys.argv[1].isnumeric() and sys.argv[2].isnumeric() and sys.argv[4].isnumeric():
        worker_id = int(sys.argv[1])
        power = int(sys.argv[2])
        lb_hostname = sys.argv[3]
        lb_port = int(sys.argv[4])

        with jobs_completed_lock:
            jobs_completed = 0

        print(f'[blue bold]Worker (pull) - ID {worker_id}[/blue bold]')

        start_processing_threads(worker_id, power, lb_hostname, lb_port)
    else:
        print('Invalid arugments.')
