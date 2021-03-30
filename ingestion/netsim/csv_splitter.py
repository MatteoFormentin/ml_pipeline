import pandas as pd
import os.path


chunksize = 10000  # Amount of line of CSV loaded in memory


def split_csv(input_csv, index_column, out_folder):
    # Since CSV inputs are large use chunck processing
    with pd.read_csv(input_csv, chunksize=chunksize) as reader:
        for chunk in reader:
            # List unique index in current chunck
            indexes = chunk.idlink.unique()
            for index in indexes:
                # If file not exist create it with csv header
                if not os.path.isfile(os.path.join(out_folder, str(index) +
                                                   '.csv')):
                    # Filter chunck rows by current unique index
                    chunk[chunk[index_column] == index].to_csv(
                        os.path.join(out_folder, str(index) +
                                    '.csv'), index=False)
                else:
                    # If file exist append to it
                    chunk[chunk[index_column] == index].to_csv(os.path.join(out_folder, str(index)+'.csv'),
                                                               mode='a', header=False, index=False)
