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

y = [1.679-0.110-0.0035,
     2.3-0.1200-0.0035,
     4.025-0.2010-0.0035,
     5.372-0.2560-0.0035,
     8.9-0.2880-0.0035,
     11.4-0.3310-0.0035,
     14.939-0.3330-0.0035,
     17.696-0.3410-0.0035,
     21.708-0.3290-0.0035,
     23.561-0.3540-0.0035,
     27.56-0.3740-0.0035,
     32.336-0.3830-0.0035,
     35.018-0.3720-0.0035]

plt.locator_params(axis="x", nbins=20)

plt.errorbar(x, y, fmt='-o', color='orange', capsize=5)
plt.gca().set(title='Average Spark latency',
              xlabel='Number of links', ylabel='Spark latency (ms)')


plt.show()
