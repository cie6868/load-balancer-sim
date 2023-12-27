#!/usr/bin/env python3

# Usage: ./lb_graph.py

from matplotlib import pyplot
import numpy
import pandas
import seaborn
import sys

fig, axs = pyplot.subplots(2)

df = pandas.DataFrame({
    'Dataset': [],
    'Job Weight': [],
    'Worker Assigned': [],
    'Start Time': [],
    'End Time': []
})
# df_response_times = pandas.DataFrame({
#     'Dataset': [],
#     'Job ID': [],
#     'Response Time': []
# })
# df_job_counts = pandas.DataFrame({
#     'Dataset': [],
#     'Worker': [],
#     'Jobs': []
# })

for i in range(len(sys.argv) - 1):
    filename = sys.argv[i + 1]
    print(filename)

    with open(filename, 'r') as f:
        # job_weights = []
        # worker_assigned = []
        # start_times = []
        # end_times = []
        # response_times = []
        # response_times_norm = []

        next(f)
        for line in f:
            parts = line.split(',')
            # job_weights.append(int(parts[1]))
            # worker_assigned.append(parts[2])
            # start_times.append(int(parts[3]))
            # end_times.append(int(parts[4]))
            # response_times.append((end_times[-1] - start_times[-1]) / 1E6)
            # response_times_norm.append(response_times[-1] / job_weights[-1])

            df.loc[len(df)] = {
                'Dataset': filename,
                'Job Weight': int(parts[1]),
                'Worker Assigned': int(parts[2]),
                'Start Time': int(parts[3]),
                'End Time': int(parts[4])
            }

df['Response Time'] = (df['End Time'] - df['Start Time']) / 1E6

job_counts_by_worker = df.groupby('Dataset')['Worker Assigned'].value_counts().to_frame('Jobs')

print(df.groupby('Dataset')['Response Time'].quantile(q = [0.5, 0.9, 0.95, 0.99]))

dfx = df[df['Response Time'] <= 1000]
p1 = seaborn.histplot(ax = axs[0], data = dfx, x = 'Response Time', hue = 'Dataset', binwidth = 1, stat = 'percent')

p2 = seaborn.barplot(ax = axs[1], data = job_counts_by_worker, x = 'Worker Assigned', y = 'Jobs', hue = 'Dataset')
for container in axs[1].containers:
    axs[1].bar_label(container, label_type = 'center')

pyplot.show()
