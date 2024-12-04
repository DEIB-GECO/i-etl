import pandas as pd
from pandas import DataFrame, Series
from utils.api_utils import send_query_to_api, parse_xml_response, parse_json_response, parse_html_response
from enums.AccessTypes import AccessTypes
import gget

if __name__ == '__main__':
    htseq = pd.read_csv("htseq_counts_original.csv", index_col=False)
    htseq.set_index('gencode_id', inplace=True)
    print(htseq)
    htseq = htseq.transpose()
    htseq_filtered = htseq.loc[:, (htseq != 0).any(axis=0)]  # drop columns with only 0
    htseq_filtered.reindex()

    # 1. get only ensembl IDs
    ensembl_codes = list(set(htseq_filtered.columns))
    print(len(ensembl_codes))

    # 2. for each of them get their OMIM id using the correspondence table (downloaded from the OMIM website)
    # https://omim.org/downloads/, then https://omim.org/static/omim/data/mim2gene.txt
    ontology_list = ["hgnc" for ensembl_code in ensembl_codes]
    code_list = [ensembl_code[0:ensembl_code.index('.')] for ensembl_code in ensembl_codes]
    profile_list = ["GENOMIC" for ensembl_code in ensembl_codes]
    dataset_list = ["htseq_counts.csv" for ensembl_code in ensembl_codes]
    name_list = [ensembl_code for ensembl_code in ensembl_codes]
    description_list = ["" for ensembl_code in ensembl_codes]
    datatype_list = ["integer" for ensembl_code in ensembl_codes]
    visibility_list = ["PUBLIC" for ensembl_code in ensembl_codes]

    final_df = pd.DataFrame.from_dict({
        "ontology": ontology_list,
        "ontology_code": code_list,
        "profile": profile_list,
        "dataset": dataset_list,
        "name": name_list,
        "description": description_list,
        "visibility": visibility_list,
        "ETL_type": datatype_list
    })
    print(len(final_df))
    final_df.to_csv("ensembl_metadata.csv", index=False)  # write metadata in a file
    htseq_filtered.transpose().to_csv("htseq_counts.csv")  # write filtered dataset back


