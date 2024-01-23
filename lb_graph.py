#!/usr/bin/env python3

# Usage: ./lb_graph.py

from matplotlib import pyplot
import pandas
import seaborn
import sys

pyplot.figure(figsize = (10, 6))
seaborn.set_theme(style = 'whitegrid')

df = pandas.DataFrame({
    'Dataset': [],
    'Job Weight': [],
    'Worker Assigned': [],
    'Start Time': [],
    'End Time': []
})

for i in range(len(sys.argv) - 1):
    filename = sys.argv[i + 1]
    print(filename)

    with open(filename, 'r') as f:
        next(f) # skip title line

        for line in f:
            parts = line.split(',')

            df.loc[len(df)] = {
                'Dataset': i + 1,
                'Job Weight': int(parts[1]),
                'Worker Assigned': int(parts[2]),
                'Start Time': int(parts[3]),
                'End Time': int(parts[4])
            }

df['Response Time'] = (df['End Time'] - df['Start Time']) / 1E6

job_counts_by_worker = df.groupby('Dataset')['Worker Assigned'].value_counts().to_frame('Jobs')

percentiles = pandas.DataFrame({
    'Dataset': [],
    'Percentile': [],
    'Response Time': []
})

for d in df['Dataset'].unique():
    for p in [0, 25, 50, 75, 90, 95, 99]:
        percentiles.loc[len(percentiles)] = {
            'Dataset': d,
            'Percentile': f'p{p}',
            'Response Time': df[df['Dataset'] == d]['Response Time'].quantile(p / 100)
        }

# quantiles = df.groupby('Dataset')['Response Time'].quantile(q = [0.5, 0.9, 0.95, 0.99])
print(percentiles)

# dfx = df[df['Response Time'] <= 1000]
# p1 = seaborn.histplot(data = dfx, x = 'Response Time', hue = 'Dataset', binwidth = 1, stat = 'percent')

p2 = seaborn.barplot(data = job_counts_by_worker, x = 'Worker Assigned', y = 'Jobs', hue = 'Dataset', palette = 'colorblind')
p2.legend(loc='center left', bbox_to_anchor=(1, 0.5))

# p3 = seaborn.barplot(data = percentiles, x = 'Percentile', y = 'Response Time', hue = 'Dataset', palette = 'colorblind')

# print(df[df['Response Time'] <= 0])

pyplot.show()
