import os

import pandas as pd
from pandas import DataFrame

from database.Execution import Execution
from enums.Profile import Profile
from enums.ParameterKeys import ParameterKeys
from preprocessing.Preprocess import Preprocess
from utils.assertion_utils import is_not_nan
from utils.file_utils import read_tabular_file_as_string
from utils.setup_logger import log


class PreprocessKidneyCovid(Preprocess):
    def __init__(self, execution: Execution, data: DataFrame, profile: Profile):
        super().__init__(execution=execution, data=data, profile=profile)

    def run(self):
        if self.profile == Profile.CLINICAL or self.profile == Profile.GENOMIC:
            df_barcode_to_patient = read_tabular_file_as_string(self.execution.diagnosis_regexes_filepath)
            df_barcode_to_patient = df_barcode_to_patient[["sample_id", "individual_id"]]

        if self.profile == Profile.PHENOTYPIC:
            # process samples data to transpose them
            self.data = self.data.drop(["sample_id", "cause_eskd", "WHO_severity", "WHO_temp_severity", "fatal_disease", "case_control", "radiology_evidence_covid", "time_from_first_symptoms", "time_from_first_positive_swab"], axis=1)
            self.data = self.data.drop_duplicates()  # there were several lines for each patient due to sample bar code, we drop that

        elif self.profile == Profile.IMAGING:
            self.data = self.data[["individual_id", "radiology_evidence_covid"]]
            self.data = self.data.drop_duplicates()

        elif self.profile == Profile.DIAGNOSIS:
            self.data = self.data[["individual_id", "cause_eskd", "WHO_severity", "fatal_disease"]]
            self.data = self.data.drop_duplicates()

        elif self.profile == Profile.CLINICAL:
            # we need to associate each sample barcode to the corresponding patient ID
            self.data = self.data.rename({"Sample_ID": "sample_id"}, axis="columns")
            self.data = pd.merge(self.data, df_barcode_to_patient, on="sample_id", how="left")

        elif self.profile == Profile.GENOMIC:
            # the htseq_counts.csv file has been processed beforehand to select a smaller subset of genes and
            # to attach the individual id to each sequence count
            pass
