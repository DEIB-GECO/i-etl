import pandas as pd


def get_sample_number(x):
    extracted_number = x[str(x).rfind("_") + 1:len(str(x))]
    try:
        return int(extracted_number)
    except:
        return 0


if __name__ == '__main__':
    df_barcode_patient = pd.read_csv("../datasets/expes/covid-kidney/mapping_patient_sample.csv")
    df_barcode_patient = df_barcode_patient[["sample_id", "individual_id"]]
    df_barcode_patient = df_barcode_patient.drop_duplicates()
    print(df_barcode_patient)

    htseq = pd.read_csv("../datasets/expes/covid-kidney/htseq_counts_original.csv", index_col=False)
    htseq.set_index('gencode_id', inplace=True)
    htseq = htseq.transpose()
    print(htseq)
    htseq = htseq.reset_index()
    htseq = htseq.rename({"index": "sample_id"}, axis="columns")
    htseq = pd.merge(htseq, df_barcode_patient, on="sample_id", how="left")
    htseq["sample_number"] = htseq["sample_id"].apply(get_sample_number)
    htseq["sample_max"] = htseq.groupby(["individual_id"])["sample_number"].transform(max)
    print(htseq)
    htseq_patient = htseq[htseq["sample_number"] == htseq["sample_max"]]
    htseq_patient = htseq_patient.reset_index()
    print(htseq_patient)
    print("individual_id" in htseq_patient)

    htseq_numbers = htseq_patient.drop(["sample_id", "individual_id", "sample_number", "sample_max", "index"], axis="columns")
    top = 1000
    top_columns = [str(i) for i in range(1, top+1)]
    htseq_top = pd.DataFrame(htseq_numbers.apply(lambda x: x.nlargest(top).index.tolist(), axis=1).tolist(), columns=top_columns)
    gene_tops = []
    for column in htseq_top.columns:
        gene_tops.extend(list(htseq_top[column]))
    print(len(gene_tops))
    gene_tops_set = set(gene_tops)
    print(len(gene_tops_set))
    print(gene_tops_set)

    print("individual_id" in htseq_patient)
    columns_to_keep = list(gene_tops_set)
    columns_to_keep.append("individual_id")
    htseq_patient = htseq_patient[columns_to_keep]  # keep only 2382 genes of interest + individual_id
    print(htseq_patient)

    htseq_patient.to_csv("../datasets/expes/covid-kidney/htseq_counts.csv", index=False)
