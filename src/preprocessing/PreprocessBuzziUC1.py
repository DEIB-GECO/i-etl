import os
import sys

from argparse import FileType
from pandas import DataFrame

from constants.structure import DOCKER_FOLDER_PREPROCESSED, DOCKER_FOLDER_DATA, DOCKER_FOLDER_PHENOTYPIC, \
    DOCKER_FOLDER_SAMPLE, DOCKER_FOLDER_DIAGNOSIS
from database.Execution import Execution
from enums.FileTypes import FileTypes
from enums.MetadataColumns import MetadataColumns
from enums.ParameterKeys import ParameterKeys
from preprocessing.Preprocess import Preprocess
from utils.assertion_utils import is_not_nan
from utils.file_utils import read_tabular_file_as_string
from utils.setup_logger import log


class PreprocessBuzziUC1(Preprocess):
    def __init__(self, execution: Execution):
        super().__init__(execution=execution)

    def run(self):
        screening_filepaths = os.getenv(ParameterKeys.PHENOTYPIC_PATHS)
        if screening_filepaths is not None:
            log.info(screening_filepaths)
            for screening_filepath in screening_filepaths.split(","):
                df = read_tabular_file_as_string(filepath=f"{os.path.join(DOCKER_FOLDER_DATA, screening_filepath)}", read_as_string=False)
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

                self.save_preprocessed_file(df=df_lab, file_type=FileTypes.PHENOTYPIC, filename="phenotypic.csv")
                self.save_preprocessed_file(df=df_samples, file_type=FileTypes.SAMPLE, filename="samples.csv")

        diagnosis_filepaths = os.getenv(ParameterKeys.DIAGNOSIS_PATHS)
        if diagnosis_filepaths is not None:
            for diagnosis_filepath in diagnosis_filepaths.split(","):
                # 2. pre-process diagnosis data to:
                df_diag = read_tabular_file_as_string(filepath=f"{os.path.join(DOCKER_FOLDER_DATA, diagnosis_filepath)}", read_as_string=False)

                # a. replace the "affetto" and "carrier" columns by the columns "acronym" and "affected"
                diagnosis = [row["affetto"] if is_not_nan(row["affetto"]) else row["carrier"] if is_not_nan(row["carrier"]) else None for index, row in df_diag.iterrows()]
                affected_booleans = [True if is_not_nan(row["affetto"]) else False for index, row in df_diag.iterrows()]
                df_diag["acronym"] = diagnosis
                df_diag["affected"] = affected_booleans
                df_diag.drop("carrier", axis=1, inplace=True)
                df_diag.drop("affetto", axis=1, inplace=True)

                # b. replace the SampleBarCode by the patient id
                mapping = {row["sample_barcode"]: row["id"] for index, row in df_diag.iterrows()}
                df_diag["SampleBarcode"] = df_diag["SampleBarcode"].replace(mapping)
                df_diag.rename(columns={"SampleBarcode": "id"}, inplace=True)

                self.save_preprocessed_file(df=df_diag, file_type=FileTypes.DIAGNOSIS, filename="diagnoses.csv")
