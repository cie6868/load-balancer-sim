#!/usr/bin/env python3

# Usage: ./lb_graph.py

from matplotlib import ticker, pyplot
import numpy
import sys

fig, axs = pyplot.subplots(2)

for i in range(len(sys.argv) - 1):
    filename = sys.argv[i + 1]
    with open(filename, 'r') as f:
        job_weights = []
        worker_assigned = []
        start_times = []
        end_times = []
        response_times = []
        response_times_norm = []

        next(f)
        for line in f:
            parts = line.split(',')
            job_weights.append(int(parts[1]))
            worker_assigned.append(parts[2])
            start_times.append(int(parts[3]))
            end_times.append(int(parts[4]))
            response_times.append((end_times[-1] - start_times[-1]) / 1E6)
            response_times_norm.append(response_times[-1] / job_weights[-1])

        print(max(response_times_norm))
        print(min(response_times_norm))
        bins = numpy.linspace(numpy.floor(min(response_times_norm)), numpy.ceil(max(response_times_norm)), 500)
        weights = numpy.ones(len(response_times_norm)) / len(response_times_norm)
        axs[0].hist(response_times_norm, bins = bins, weights = weights, histtype = 'step', label = filename)

        axs[1].hist(worker_assigned, histtype = 'step', label = filename)

axs[0].yaxis.set_major_formatter(ticker.PercentFormatter(1))
axs[0].legend()

axs[1].legend()

pyplot.show()
