import os

from constants.structure import DOCKER_FOLDER_PREPROCESSED
from database.Execution import Execution
import shutil

from enums.FileTypes import FileTypes
from enums.ParameterKeys import ParameterKeys
from preprocessing.Preprocess import Preprocess


class PreprocessDefault(Preprocess):
    def __init__(self, execution: Execution):
        super().__init__(execution)

    def run(self):
        pairs = [
            (ParameterKeys.PHENOTYPIC_PATHS, FileTypes.PHENOTYPIC),
            (ParameterKeys.SAMPLE_PATHS, FileTypes.SAMPLE),
            (ParameterKeys.DIAGNOSIS_PATHS, FileTypes.DIAGNOSIS),
            (ParameterKeys.IMAGING_PATHS, FileTypes.IMAGING),
            (ParameterKeys.MEDICINE_PATHS, FileTypes.MEDICINE),
            (ParameterKeys.GENOMIC_PATHS, FileTypes.GENOMIC)
        ]

        for pair in pairs:
            parameter = pair[0]
            file_type = pair[1]
            filepaths = os.getenv(parameter)
            if filepaths is not None:
                prefix = FileTypes.get_prefix_for_path(file_type)
                for data_filepath in filepaths.split(","):
                    current_location = os.path.join(prefix, data_filepath)
                    preprocessed_location = os.path.join(prefix, DOCKER_FOLDER_PREPROCESSED, data_filepath)
                    shutil.copy(current_location, preprocessed_location)
