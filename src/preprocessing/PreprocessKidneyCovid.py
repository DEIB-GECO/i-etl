import pandas as pd
from pandas import DataFrame

from database.Execution import Execution
from enums.Profile import Profile
from preprocessing.Preprocess import Preprocess
from utils.file_utils import read_tabular_file_as_string
from utils.setup_logger import log


def get_sample_number(x):
    extracted_number = x[str(x).rfind("_") + 1:len(str(x))]
    try:
        return int(extracted_number)
    except:
        return 0


class PreprocessKidneyCovid(Preprocess):
    def __init__(self, execution: Execution, data: DataFrame, profile: Profile):
        super().__init__(execution=execution, data=data, profile=profile)

    def run(self):
        log.info(self.profile)
        log.info(self.data)
        log.info(self.data.columns)
        if self.profile == Profile.CLINICAL or self.profile == Profile.GENOMIC:
            df_barcode_to_patient = read_tabular_file_as_string(self.execution.diagnosis_regexes_filepath)
            df_barcode_to_patient = df_barcode_to_patient[["sample_id", "individual_id"]]
            df_barcode_to_patient = df_barcode_to_patient.drop_duplicates()

        if self.profile in [Profile.PHENOTYPIC, Profile.IMAGING, Profile.DIAGNOSIS]:
            # for metadata_w1.csv
            if self.profile == Profile.PHENOTYPIC:
                # process samples data to transpose them
                self.data = self.data.drop(["cause_eskd", "WHO_severity", "WHO_temp_severity", "fatal_disease", "case_control", "radiology_evidence_covid", "time_from_first_symptoms", "time_from_first_positive_swab"], axis=1)
            elif self.profile == Profile.IMAGING:
                self.data = self.data[["individual_id", "sample_id", "radiology_evidence_covid"]]
            elif self.profile == Profile.DIAGNOSIS:
                self.data = self.data[["individual_id", "sample_id", "cause_eskd", "WHO_severity", "fatal_disease"]]
            # after selecting columns of interest, we compute the latest sample id of the patient and keep associated data
            self.data["sample_number"] = self.data["sample_id"].apply(get_sample_number)
            self.data["sample_max"] = self.data.groupby(["individual_id"])["sample_number"].transform("max")
            self.data = self.data[self.data["sample_number"] == self.data["sample_max"]]
            self.data = self.data.drop(["sample_id", "sample_number", "sample_max"], axis="columns")
            self.data = self.data.reset_index()

        elif self.profile == Profile.CLINICAL:
            # for general_panel.csv
            # we need to associate each sample barcode to the corresponding patient ID
            self.data = self.data.rename({"Sample_ID": "sample_id"}, axis="columns")
            self.data = pd.merge(self.data, df_barcode_to_patient, on="sample_id", how="left")

        elif self.profile == Profile.GENOMIC:
            # the htseq_counts.csv file has been processed beforehand to select a smaller subset of genes and
            # to attach the individual id to each sequence count
            pass

