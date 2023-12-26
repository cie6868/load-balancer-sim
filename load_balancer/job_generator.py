#!/usr/bin/env python3

# Usage: ./job_generator.py [number of jobs] [mean processing time of jobs (ms)] [mean interval between jobs (ms)]

from numpy import char
from pathlib import Path
from rich import print
from scipy.stats import poisson
import sys

def generate_jobs(num_jobs, job_weight_mean, job_interval_mean):
    Path("inputs").mkdir(parents=True, exist_ok=True)

    with open('inputs/job_weights.csv', 'w') as f:
        print(f'Populating {num_jobs} jobs.')
        # job_weights = poisson.rvs(mu = job_weight_mean, size = num_jobs)
        job_weights = [job_weight_mean] * num_jobs
        job_weights_str = char.mod('%d', job_weights)
        f.write(','.join(job_weights_str))

    with open('inputs/job_intervals.csv', 'w') as f:
        print(f'Populating {num_jobs} wait times between jobs.')
        # job_intervals = poisson.rvs(mu = job_interval_mean, size = num_jobs)
        job_intervals = [job_interval_mean] * num_jobs
        job_intervals_str = char.mod('%d', job_intervals)
        f.write(','.join(job_intervals_str))

if __name__ == '__main__':
    num_jobs = int(sys.argv[1])
    job_weight_mean = int(sys.argv[2])
    job_interval_mean = int(sys.argv[3])

    generate_jobs(num_jobs, job_weight_mean, job_interval_mean)
