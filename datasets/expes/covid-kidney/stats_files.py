import pandas as pd
from pandas import DataFrame


if __name__ == '__main__':
    df_metadata_w1 = pd.read_csv("w1_metadata.csv", index_col=False)
    df_metadata_w2 = pd.read_csv("w2_metadata.csv", index_col=False)
    df_htseq_counts = pd.read_csv("htseq_counts.csv", index_col=False)
    df_panel = pd.read_csv("general_panel.csv", index_col=False)
    patients_w1_htseq_count = []
    for row_index, row in df_metadata_w1.iterrows():
        if row["sample_id"] in df_htseq_counts.columns:
            if row["individual_id"] not in patients_w1_htseq_count:
                patients_w1_htseq_count.append(row["individual_id"])
    patients_w2_htseq_count = []
    for row_index, row in df_metadata_w2.iterrows():
        if row["sample_id"] in df_htseq_counts.columns:
            if row["individual_id"] not in patients_w2_htseq_count:
                patients_w2_htseq_count.append(row["individual_id"])

    nb_samples_htseq_patients_w1 = 0
    for row_index, row in df_metadata_w1.iterrows():
        if row["individual_id"] in patients_w1_htseq_count:
            nb_samples_htseq_patients_w1 += 1
    nb_samples_htseq_patients_w2 = 0
    for row_index, row in df_metadata_w2.iterrows():
        if row["individual_id"] in patients_w2_htseq_count:
            nb_samples_htseq_patients_w2 += 1

    print(len(df_panel["Sample_ID"]))

    patients_w1_panel = []
    for row_index, row in df_metadata_w1.iterrows():
        print(f"{row["sample_id"]} in {list(df_panel["Sample_ID"])}")
        if row["sample_id"] in list(df_panel["Sample_ID"]):
            if row["individual_id"] not in patients_w1_panel:
                patients_w1_panel.append(row["individual_id"])
    print(patients_w1_panel)
    patients_w2_panel = []
    for row_index, row in df_metadata_w2.iterrows():
        if row["sample_id"] in list(df_panel["Sample_ID"]):
            if row["individual_id"] not in patients_w2_panel:
                patients_w2_panel.append(row["individual_id"])

    nb_samples_panel_patients_w1 = 0
    for row_index, row in df_metadata_w1.iterrows():
        if row["individual_id"] in patients_w1_panel:
            nb_samples_panel_patients_w1 += 1
    nb_samples_panel_patients_w2 = 0
    for row_index, row in df_metadata_w2.iterrows():
        if row["individual_id"] in patients_w2_panel:
            nb_samples_panel_patients_w2 += 1

    print(len(df_metadata_w1))
    print(len(df_metadata_w2))
    print(len(pd.concat([df_metadata_w1, df_metadata_w2], axis=0)))
    print(f"nb distinct patients in overall metadata: {len(pd.unique(pd.concat([df_metadata_w1, df_metadata_w2], axis=0)["individual_id"]))}")
    print(f"nb distinct patient metadata w1: {len(pd.unique(df_metadata_w1["individual_id"]))}")
    print(f"nb distinct patient metadata w2: {len(pd.unique(df_metadata_w2["individual_id"]))}")
    print(f"total nb of samples for w1: {len(df_metadata_w1["sample_id"])}")
    print(f"total nb of samples for w2: {len(df_metadata_w2["sample_id"])}")
    print(f"nb w1 patients having sample in htseq counts: {len(patients_w1_htseq_count)}")
    print(f"nb w2 patients having sample in htseq counts: {len(patients_w2_htseq_count)}")
    print(f"nb samples for w1 patients having sample in htseq counts: {nb_samples_htseq_patients_w1}")
    print(f"nb samples for w2 patients having sample in htseq counts: {nb_samples_htseq_patients_w2}")
    print(f"nb w1 patients having sample in panel: {len(patients_w1_panel)}")
    print(f"nb w2 patients having sample in panel: {len(patients_w2_panel)}")
    print(f"nb samples for w1 patients having sample in panel: {nb_samples_panel_patients_w1}")
    print(f"nb samples for w2 patients having sample in panel: {nb_samples_panel_patients_w2}")
