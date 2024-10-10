import os
import sys

import pandas as pd

if __name__ == '__main__':
    folder = sys.argv[1]
    data_filename = sys.argv[2]
    new_filename = sys.argv[3]
    n = int(sys.argv[4])
    sub_df = pd.read_csv(f"{os.path.join(folder, data_filename)}", nrows=n)
    sub_df.to_csv(f"{os.path.join(folder, new_filename)}", index=False)
