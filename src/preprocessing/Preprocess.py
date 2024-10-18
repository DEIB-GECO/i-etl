import os

from pandas import DataFrame

from constants.structure import DOCKER_FOLDER_PREPROCESSED
from database.Execution import Execution
from enums.FileTypes import FileTypes


class Preprocess:
    def __init__(self, execution: Execution):
        self.execution = execution

    def run(self):
        raise NotImplementedError("This method should be implemented in every Preprocessing child class.")

    def save_preprocessed_file(self, df: DataFrame, file_type: str, filename: str) -> None:
        docker_prefix = FileTypes.get_prefix_for_path(file_type)
        docker_path = os.path.join(docker_prefix, DOCKER_FOLDER_PREPROCESSED)
        if not os.path.exists(docker_path):
            os.makedirs(docker_path)
        filepath_processed = os.path.join(docker_path, filename)
        df.to_csv(filepath_processed, index=False)
        self.execution.phenotypic_filepaths.append(filepath_processed)
