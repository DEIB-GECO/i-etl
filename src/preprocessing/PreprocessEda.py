import os

import pandas as pd

from database.Execution import Execution
from enums.FileTypes import FileTypes
from enums.ParameterKeys import ParameterKeys
from preprocessing.Preprocess import Preprocess
from utils.setup_logger import log


class PreprocessEda(Preprocess):
    def __init__(self, execution: Execution):
        super().__init__(execution=execution)

    def run(self):
        log.info("I will preprocess data")
        medical_filepaths = os.getenv(ParameterKeys.PHENOTYPIC_PATHS)
        if medical_filepaths is not None:
            log.info(medical_filepaths)
            prefix = FileTypes.get_prefix_for_path(filetype=FileTypes.PHENOTYPIC)
            for data_filepath in medical_filepaths.split(","):
                df = pd.read_csv(f"{os.path.join(prefix, data_filepath)}")
                df_phenotypic = df[["id", "age", "gender", "smoking_status"]]
                df_samples = df[["id", "bmi", "blood_pressure", "glucose_levels"]]
                df_condition = df[["id", "condition"]]

                # for samples only, we add a sample id column
                df_samples["sid"] = [f"s{i}" for i in range(1, len(df_samples)+1)]

                # those filenames HAVE TO exactly match the names given in the metadata
                self.save_preprocessed_file(df=df_phenotypic, file_type=FileTypes.PHENOTYPIC, filename="phenotypic.csv")
                self.save_preprocessed_file(df=df_samples, file_type=FileTypes.SAMPLE, filename="samples.csv")
                self.save_preprocessed_file(df=df_condition, file_type=FileTypes.DIAGNOSIS, filename="conditions.csv")
        log.info("Done with preprocessing data")
