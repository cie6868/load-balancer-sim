from numpy import char
from rich import print
from scipy.stats import poisson
import sys

num_jobs = int(sys.argv[1])
weight_mean = int(sys.argv[2])
wait_mean = int(sys.argv[3])

job_list = []

with open('weights.csv', 'w') as f:
    print(f'Populating {num_jobs} jobs.')
    weights = poisson.rvs(mu = weight_mean, size = num_jobs)
    weights_str = char.mod('%d', weights)
    f.write(','.join(weights_str))

with open('waits.csv', 'w') as f:
    print(f'Populating {num_jobs} wait times between jobs.')
    waits = poisson.rvs(mu = wait_mean, size = num_jobs)
    waits_str = char.mod('%d', waits)
    f.write(','.join(waits_str))
