import pandas as pd


def get_sample_number(x):
    extracted_number = x[str(x).rfind("_") + 1:len(str(x))]
    try:
        return int(extracted_number)
    except:
        return 0


if __name__ == '__main__':
    w1_metadata = pd.read_csv("../datasets/expes/covid-kidney/w1_metadata.csv", index_col=False)
    print(w1_metadata)
    w2_metadata = pd.read_csv("../datasets/expes/covid-kidney/w2_metadata.csv", index_col=False)
    print(w2_metadata)

    w1_metadata = w1_metadata.drop(
            ["cause_eskd", "WHO_severity", "WHO_temp_severity", "fatal_disease", "case_control",
             "radiology_evidence_covid", "time_from_first_symptoms", "time_from_first_positive_swab"], axis=1)
    # after selecting columns of interest, we compute the latest sample id of the patient and keep associated data
    w1_metadata["sample_number"] = w1_metadata["sample_id"].apply(get_sample_number)
    w1_metadata["sample_max"] = w1_metadata.groupby(["individual_id"])["sample_number"].transform("max")
    w1_metadata = w1_metadata[w1_metadata["sample_number"] == w1_metadata["sample_max"]]
    w1_metadata = w1_metadata.drop(["sample_id", "sample_number", "sample_max"], axis="columns")
    w1_metadata = w1_metadata.reset_index()

    w2_metadata = w2_metadata.drop(
        ["cause_eskd", "WHO_severity", "WHO_temp_severity", "fatal_disease", "case_control",
         "radiology_evidence_covid", "time_from_first_symptoms", "time_from_first_positive_swab"], axis=1)
    # after selecting columns of interest, we compute the latest sample id of the patient and keep associated data
    w2_metadata["sample_number"] = w2_metadata["sample_id"].apply(get_sample_number)
    w2_metadata["sample_max"] = w2_metadata.groupby(["individual_id"])["sample_number"].transform("max")
    w2_metadata = w2_metadata[w2_metadata["sample_number"] == w2_metadata["sample_max"]]
    w2_metadata = w2_metadata.drop(["sample_id", "sample_number", "sample_max"], axis="columns")
    w2_metadata = w2_metadata.reset_index()
    print(w2_metadata)

    print(f"{len(w1_metadata["individual_id"])} vs. {len(set(w1_metadata["individual_id"]))}")
    print(f"{len(w2_metadata["individual_id"])} vs. {len(set(w2_metadata["individual_id"]))}")

