import os

from pandas import DataFrame

from database.Execution import Execution
from enums.HospitalNames import HospitalNames
from enums.Profile import Profile
from preprocessing.PreprocessBuzziUC1 import PreprocessBuzziUC1
from preprocessing.PreprocessCovid import PreprocessCovid
from utils.setup_logger import log


class PreprocessingTask:
    def __init__(self, execution: Execution, metadata: DataFrame, data: DataFrame, profile: Profile):
        self.execution = execution
        self.metadata = metadata
        self.data = data
        self.profile = profile

    def run(self):
        if self.execution.hospital_name == HospitalNames.IT_BUZZI_UC1:
            pp = PreprocessBuzziUC1(execution=self.execution, metadata=self.metadata, data=self.data, profile=self.profile)
            pp.run()
            self.data = pp.data
        elif self.execution.hospital_name == HospitalNames.KAGGLE_COVID:
            pp = PreprocessCovid(execution=self.execution, metadata=self.metadata, data=self.data, profile=self.profile)
            pp.run()
        else:
            pass
