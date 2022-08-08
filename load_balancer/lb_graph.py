from matplotlib import pyplot
import sys

for i in range(len(sys.argv) - 1):
    filename = sys.argv[i + 1]
    with open(filename, 'r') as f:
        weights = []
        response_times = []
        for line in f:
            parts = line.split(';')
            weights.append(int(parts[2]))
            response_times.append((int(parts[4]) - int(parts[3])) / 1E9)
        response_times.sort()
        pyplot.plot(response_times)
        # response_time_mean = sum(response_times) / len(response_times)
        # response_times_normalized = [rt / response_time_mean for rt in response_times]
        # pyplot.plot(response_times_normalized)

pyplot.show()
