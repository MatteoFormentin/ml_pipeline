#
#   logs_csv_splitter.py
#   Split a CSV files that contains many log producer in multiple CSV file foreach producer
#

import pandas as pd
import os.path


INPUT_CSV = "/Volumes/HDD/data/dataIveritas_from17052019.csv"
INDEX_COLUMN = "idlink"
OUT_FOLDER = "out/"

chunksize = 10000  # Amount of line of CSV loaded in memory

# Since CSV inputs are large use chunck processing
with pd.read_csv(INPUT_CSV, chunksize=chunksize) as reader:
    for chunk in reader:
        # List unique index in current chunck
        indexes = chunk.idlink.unique()
        for index in indexes:
            # If file not exist create it with csv header
            if not os.path.isfile(str(index)+'.csv'):
                # Filter chunck rows by current unique index
                chunk[chunk[INDEX_COLUMN] == index].to_csv(
                    OUT_FOLDER + str(index)+'.csv', index=False)
            else:
                # If file exist append to it
                chunk[chunk[INDEX_COLUMN] == index].to_csv(OUT_FOLDER + str(index)+'.csv',
                                                           mode='a', header=False, index=False)
