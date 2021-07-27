import numpy as np
import matplotlib.pyplot as plt

plt.style.use('seaborn-whitegrid')

x = [10,
     100,
     500,
     1000,
     2000,
     3000,
     4000,
     5000,
     6000,
     7000,
     8000,
     9000,
     10000]

y = [11,
     120,
     201,
     256,
     288,
     331,
     333,
     341,
     329,
     354,
     374,
     383,
     372]

var = [12,
       18,
       95,
       159,
       215,
       279,
       288,
       311,
       279,
       328,
       345,
       353,
       363]
plt.locator_params(axis="x", nbins=20)

plt.errorbar(x, y, yerr=var, fmt='-o', capsize=5)
plt.gca().set(title='Average Kafka latency', xlabel='Number of links', ylabel='Kafka latency (ms)')


plt.show()
