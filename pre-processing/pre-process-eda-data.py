import os
import sys

import pandas as pd


if __name__ == '__main__':
    folder = sys.argv[1]
    data_filepath = sys.argv[2]
    df = pd.read_csv(f"{os.path.join(folder, data_filepath)}")
    df_phenotypic = df[["id", "age", "gender", "smoking_status"]]
    df_samples = df[["id", "bmi", "blood_pressure", "glucose_levels"]]
    df_condition = df[["id", "condition"]]

    # for samples only, we add a sample id column
    df_samples["sid"] = [f"s{i}" for i in range(1, len(df_samples)+1)]

    df_phenotypic.to_csv(os.path.join(folder, "preprocessed", "phenotypic.csv"), index=False)
    df_samples.to_csv(os.path.join(folder, "preprocessed", "samples.csv"), index=False)
    df_condition.to_csv(os.path.join(folder, "preprocessed", "conditions.csv"), index=False)
