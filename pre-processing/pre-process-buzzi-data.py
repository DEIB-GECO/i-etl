import os
import sys

from pandas import DataFrame

from enums.MetadataColumns import MetadataColumns
from utils.assertion_utils import is_not_nan
from utils.file_utils import read_tabular_file_as_string
from utils.setup_logger import log

if __name__ == '__main__':
    buzzi_folder = sys.argv[1]
    screening_filepath = sys.argv[2]
    diagnosis_filepath = sys.argv[3]
    df = read_tabular_file_as_string(filepath=f"{os.path.join(buzzi_folder, screening_filepath)}", read_as_string=False)
    df.rename(columns=lambda x: MetadataColumns.normalize_name(column_name=x), inplace=True)

    # 1. split initial screening in sample data and screening (phenotypic) data
    lab_columns = ["DateOfBirth", "Sex", "City", "GestationalAge", "Ethnicity", "Weight", "Twins", "AntibioticsBaby", "AntibioticsMother", "Meconium", "CortisoneBaby", "CortisoneMother", "TyroidMother", "Premature", "BabyFed", "HUFeed", "MIXFeed", "ARTFeed", "TPNFeed", "ENFeed", "TPNCARNFeed", "TPNMCTFeed", "Hospital", "HospitalCode1", "HospitalCode2", "BirthMethod"]
    lab_columns = [MetadataColumns.normalize_name(column_name) for column_name in lab_columns]

    # a. get sample data: no lab columns
    # (except patient id, that is kept to be able to know which sample is for which patient)
    df_samples = DataFrame(df)
    df_samples = df_samples.drop(lab_columns, axis=1)

    # b. get lab data: only lab columns + patient id
    lab_columns.append(MetadataColumns.normalize_name("id"))
    df_lab = df[lab_columns]
    df_lab = df_lab.drop_duplicates(subset=["id"], keep=False)

    df_lab.to_csv(os.path.join(buzzi_folder, "preprocessed", "screening.csv"), index=False)
    df_samples.to_csv(os.path.join(buzzi_folder, "preprocessed", "samples.csv"), index=False)

    # 2. pre-process diagnosis data to:
    diag_df = read_tabular_file_as_string(filepath=os.path.join(buzzi_folder, diagnosis_filepath), read_as_string=False)

    # a. replace the "affetto" and "carrier" columns by the columns "acronym" and "affected"
    diagnosis = [row["affetto"] if is_not_nan(row["affetto"]) else row["carrier"] if is_not_nan(row["carrier"]) else None for index, row in diag_df.iterrows()]
    affected_booleans = [True if is_not_nan(row["affetto"]) else False for index, row in diag_df.iterrows()]
    diag_df["acronym"] = diagnosis
    diag_df["affected"] = affected_booleans
    diag_df.drop("carrier", axis=1, inplace=True)
    diag_df.drop("affetto", axis=1, inplace=True)

    # b. replace the SampleBarCode by the patient id
    mapping = {row["sample_barcode"]: row["id"] for index, row in df.iterrows()}
    diag_df["SampleBarcode"] = diag_df["SampleBarcode"].replace(mapping)
    diag_df.rename(columns={"SampleBarcode": "id"}, inplace=True)

    diag_df.to_csv(os.path.join(buzzi_folder, "preprocessed", "diagnoses.csv"), index=False)
