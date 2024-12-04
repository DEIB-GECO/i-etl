import pandas as pd


if __name__ == '__main__':
    panel = pd.read_csv("htseq_counts.csv", index_col=False)
    df_barcode_to_patient = pd.read_csv("mapping_patient_sample.csv")
    df_barcode_to_patient = df_barcode_to_patient[["sample_id", "individual_id"]]
    all_sample_ids = list(panel.columns)
    all_sample_ids.remove("gencode_id")

    print(panel)
    panel.set_index("gencode_id", inplace=True)
    print(panel)
    panel = panel.transpose()
    print(panel)
    panel["sample_id"] = all_sample_ids
    print(panel)
    print(panel.index)
    panel = panel.loc[:, (panel != "0").any(axis=0)]  # drop columns with only 0
    print(panel)
    panel = panel.reindex()
    panel.reset_index(inplace=True)
    # rename the first column to indicate that this contains the sample id now that the DF is transposed
    print(panel)
    panel = pd.merge(panel, df_barcode_to_patient, on="sample_id", how="left")
    print(panel)
    print("genecode_id" in list(panel.columns))
    print("sample_id" in list(panel.columns))
    print("individual_id" in list(panel.columns))
