#!/usr/bin/env python3

# Usage: ./worker.py [port] [power]

import json
import queue
from rich import print
import socket
import sys
import threading
import time

HOST_ADDR = '127.0.0.1'

THREAD_COUNT = 1
QUEUE_MAX_SIZE = 1000

job_queue = queue.Queue(QUEUE_MAX_SIZE)

jobs_received_lock = threading.Lock()
jobs_received = 0
jobs_completed_lock = threading.Lock()
jobs_completed = 0

def start_processing_thread(power: int):
    print(f'[blue]Queue maximum size is {QUEUE_MAX_SIZE}.[/blue]')
    print(f'[blue]Thread count is {QUEUE_MAX_SIZE}.[/blue]')
    print(f'[blue]Power is {power}.[/blue]')

    for i in range(THREAD_COUNT):
        thread = threading.Thread(
            target = processing_thread,
            args = (i + 1, power,),
            daemon = True
        )
        thread.start()

def processing_thread(thread_id: int, power: int):
    global jobs_completed

    print(f'[blue]Started processing thread {thread_id}...[/blue]')
    try:
        while True:
            (job, con) = job_queue.get(block = True)

            print(f'[blue]Processing job {job["id"]} with weight {job["weight"]}...[/blue]')
            sleep_seconds = (job['weight'] / 1000)
            time.sleep(sleep_seconds / (power / 100))

            print(f'[blue]Responding to job {job["id"]}.[/blue]')
            con.sendall(b'1')
            con.close()
            job_queue.task_done()
            with jobs_completed_lock:
                jobs_completed += 1

            print(f'[blue]Completed job {job["id"]}.[/blue]')

    except (KeyboardInterrupt, SystemExit):
        print(f'[blue]Stopped processing on thread {thread_id}.[/blue]')

def start_listening(address: str, port: int):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind((address, port))
            print(f'[yellow]Listening for jobs on port {port}...[/yellow]')
            sock.listen()
            while True:
                (con, con_addr) = sock.accept()
                print(f'[yellow]Connection from {con_addr}.[/yellow]')
                queue_incoming_job(con)
        except KeyboardInterrupt:
            sock.close()
        print('Stopped listening.')
        print(f'Jobs recevied: {jobs_received}')
        print(f'Jobs completed: {jobs_completed}')

def queue_incoming_job(con: socket.socket):
    global jobs_received

    message_length = int.from_bytes(con.recv(1), 'big')
    encoded_job_bytes = con.recv(message_length)
    job = json.loads(encoded_job_bytes.decode('utf-8'))
    job_queue.put((job, con))
    with jobs_received_lock:
        jobs_received += 1

    print(f'[yellow]Queued job ID {job["id"]} and weight {job["weight"]}.[/yellow]')

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1].isnumeric() and sys.argv[2].isnumeric():
        with jobs_received_lock:
            jobs_received = 0
        with jobs_completed_lock:
            jobs_completed = 0

        print('[blue bold]Worker[/blue bold]')

        start_processing_thread(int(sys.argv[2]))

        start_listening(HOST_ADDR, int(sys.argv[1]))
