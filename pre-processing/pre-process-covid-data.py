import json
import os
import sys

import pandas as pd
from pandas import DataFrame


if __name__ == '__main__':
    folder = sys.argv[1]
    dynamic_filepath = sys.argv[2]
    demographic_filepath = sys.argv[3]

    # process phenotypic data
    demo_df = pd.read_csv(os.path.join(folder, demographic_filepath))
    demo_df.drop("patient_id", axis=1, inplace=True)
    demo_df.drop("outcome", axis=1, inplace=True)
    demo_df.to_csv(os.path.join(folder, "preprocessed", "phenotypic.csv"), index=False)

    # preprocess disease data
    demo_df = pd.read_csv(os.path.join(folder, demographic_filepath))
    disease_df = demo_df[["id", "outcome"]]
    disease_df.to_csv(os.path.join(folder, "preprocessed", "diseases.csv"), index=False)

    # process samples data to transpose them
    dyn_df = pd.read_csv(os.path.join(folder, dynamic_filepath))
    dyn_df.drop(["hospital", "interpolated", "time_start", "time_end"], axis=1, inplace=True)
    new_df_as_json = {}
    columns = list(dyn_df["test"].unique())
    all_ids = list(dyn_df["id"].unique())
    one_patient = {}
    all_patients = []

    if "id" not in new_df_as_json:
        new_df_as_json["id"] = []
    for column in columns:
        new_df_as_json[column] = []

    for pid in all_ids:
        # print(f"****{pid}****")
        tests_for_patient = DataFrame(dyn_df.loc[dyn_df["id"] == pid]["test"])
        tests_for_patient = tests_for_patient.reset_index(drop=True)
        values_for_patient = DataFrame(dyn_df.loc[dyn_df["id"] == pid]["value"])
        values_for_patient = values_for_patient.reset_index(drop=True)
        max_occurrence = max(tests_for_patient.value_counts())

        one_patient["id"] = [pid for _ in range(max_occurrence)]
        for column in columns:
            one_patient[column] = [None for _ in range(max_occurrence)]
        # print(f"one_patient = {one_patient}")
        # print(f"tests_for_patient = {tests_for_patient}")
        # print(f"values_for_patient = {values_for_patient}")
        for i in range(len(values_for_patient)):
            the_test = tests_for_patient.iloc[i].iloc[0]
            the_value = values_for_patient.iloc[i].iloc[0]
            # print(f"value={the_value}; ")
            # print(f"tests_for_patient[i]={the_test}")
            # print(f"index {i%max_occurrence} in array: {one_patient[the_test]}")
            one_patient[the_test][i%max_occurrence] = the_value
            # print(f"one_patient = {one_patient}")
        all_patients.append(one_patient)
        one_patient = {}

    final_json = {}
    for patient_as_json in all_patients:
        for key in patient_as_json:
            if key not in final_json:
                final_json[key] = []
            final_json[key].extend(patient_as_json[key])
    # print(json.dumps(all_patients))
    # print(json.dumps(final_json))
    samples_df = pd.DataFrame(final_json)
    samples_df["sid"] = [f"s{i}" for i in range(1, len(samples_df)+1)]
    samples_df.to_csv(f"{os.path.join(folder, "preprocessed", "samples.csv")}", index=False)
