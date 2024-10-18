import os

from pandas import DataFrame

from constants.structure import DOCKER_FOLDER_GENERATED_DATA
from database.Execution import Execution


class DataGenerator:
    def __init__(self, execution: Execution):
        self.execution = execution

    def generate(self):
        raise NotImplementedError("This method should be implemented in every child class.")

    def save_generated_file(self, df: DataFrame, filename: str) -> None:
        if not os.path.exists(os.path.join(DOCKER_FOLDER_GENERATED_DATA, self.execution.hospital_name)):
            os.makedirs(os.path.join(DOCKER_FOLDER_GENERATED_DATA, self.execution.hospital_name))
        last_dot = filename.rfind(".")  # rfind starts from the end of the string to find occurrences
        filename_with_nb_rows = f"{filename[0:last_dot]}_{self.execution.nb_rows}{filename[last_dot:]}"
        filepath_generated = f"{os.path.join(DOCKER_FOLDER_GENERATED_DATA, self.execution.hospital_name, filename_with_nb_rows)}"
        df.to_csv(filepath_generated, index=False)
