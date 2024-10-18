import os

from pandas import DataFrame

from constants.structure import DOCKER_FOLDER_PREPROCESSED
from database.Execution import Execution
from enums.FileTypes import FileTypes
from enums.HospitalNames import HospitalNames
from preprocessing.PreprocessBuzziUC1 import PreprocessBuzziUC1
from preprocessing.PreprocessCovid import PreprocessCovid
from preprocessing.PreprocessDefault import PreprocessDefault
from preprocessing.PreprocessEda import PreprocessEda


class PreprocessingTask:
    def __init__(self, execution: Execution):
        self.execution = execution

    def run(self):
        # 0. preprocess all the files given by the user
        # this task is specific to each hospital
        pp = None
        if self.execution.hospital_name == HospitalNames.IT_BUZZI_UC1:
            pp = PreprocessBuzziUC1(execution=self.execution)
        elif self.execution.hospital_name == HospitalNames.KAGGLE_COVID:
            pp = PreprocessCovid(execution=self.execution)
        elif self.execution.hospital_name == HospitalNames.KAGGLE_EDA:
            pp = PreprocessEda(execution=self.execution)
        else:
            pp = PreprocessDefault(execution=self.execution)
        # this writes clean data in the "preprocessed" folder according to the Docker internal structure
        pp.run()
