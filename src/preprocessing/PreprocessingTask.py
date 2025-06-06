from pandas import DataFrame

from database.Execution import Execution
from enums.HospitalNames import HospitalNames
from enums.Profile import Profile
from preprocessing.Preprocess import Preprocess
from preprocessing.PreprocessBuzziUC1 import PreprocessBuzziUC1
from preprocessing.PreprocessCovid import PreprocessCovid
from preprocessing.PreprocessHsjd import PreprocessHsjd
from preprocessing.PreprocessImgge import PreprocessImgge
from preprocessing.PreprocessKidneyCovid import PreprocessKidneyCovid
from preprocessing.PreprocessLafe import PreprocessLafe
from utils.setup_logger import log


class PreprocessingTask:
    def __init__(self, execution: Execution, data: DataFrame, metadata: DataFrame, profile: str, all_columns_dataset: list):
        self.execution = execution
        self.data = data
        self.metadata = metadata
        self.profile = profile
        self.all_columns_dataset = all_columns_dataset

    def run(self):
        if self.execution.hospital_name == HospitalNames.IT_BUZZI_UC1:
            pp = PreprocessBuzziUC1(execution=self.execution, data=self.data, metadata=self.metadata, profile=self.profile)
            pp.run()
        elif self.execution.hospital_name == HospitalNames.EXPES_COVID:
            pp = PreprocessCovid(execution=self.execution, data=self.data, metadata=self.metadata, profile=self.profile)
            pp.run()
        elif self.execution.hospital_name == HospitalNames.EXPES_KIDNEY:
            pp = PreprocessKidneyCovid(execution=self.execution, data=self.data, metadata=self.metadata, profile=self.profile)
            pp.run()
        elif self.execution.hospital_name == HospitalNames.RS_IMGGE:
            pp = PreprocessImgge(execution=self.execution, data=self.data, metadata=self.metadata, profile=self.profile, all_columns_dataset=self.all_columns_dataset)
            pp.run()
        elif self.execution.hospital_name == HospitalNames.ES_HSJD:
            pp = PreprocessHsjd(execution=self.execution, data=self.data, metadata=self.metadata, profile=self.profile)
            pp.run()
        elif self.execution.hospital_name == HospitalNames.ES_LAFE:
            pp = PreprocessLafe(execution=self.execution, data=self.data, metadata=self.metadata, profile=self.profile, all_columns_dataset=self.all_columns_dataset)
            pp.run()
        else:
            pp = Preprocess(execution=self.execution, data=self.data, metadata=self.metadata, profile=self.profile)
            pp.run()

        # save the new data
        self.data = pp.data
        self.metadata = pp.metadata
        if hasattr(pp, "all_columns_dataset"):
            # only IMGGE pre-processing class has this attribute
            self.all_columns_dataset = pp.all_columns_dataset
