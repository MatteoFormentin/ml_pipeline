from sys import path
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


file_path = "/Users/matteo/Desktop/varie tesi/results/kafka_latency/5000/5000.csv"


def reject_outliers(data, m=5.5):
    return data[abs(data - np.mean(data)) < m * np.std(data)]


data = pd.read_csv(
    file_path, sep=',', index_col=0)

to_plot = data["kafka_latency"].to_numpy()

to_plot = reject_outliers(data["kafka_latency"].to_numpy())

plt.locator_params(axis="x", nbins=10)
plt.hist(to_plot, bins=20, density=False,
         weights=np.ones(len(to_plot)) / len(to_plot))
plt.gca().set(title='Kafka latency distribution - 5000 links', xlabel='Kafka latency (ms)', ylabel='Frequency')
plt.xlim(np.min(to_plot), np.max(to_plot))

print("Mean: %f\nStd: %f" % (np.mean(to_plot),  np.std(to_plot)))

plt.show()
