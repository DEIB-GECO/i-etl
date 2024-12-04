import os

from pandas import DataFrame

from constants.structure import DOCKER_FOLDER_GENERATED_DATA
from database.Execution import Execution
from utils.setup_logger import log


class DataGenerator:
    def __init__(self, execution: Execution):
        self.execution = execution

    def generate(self):
        raise NotImplementedError("This method should be implemented in every child class.")

    def save_generated_file(self, df: DataFrame, filename: str) -> None:
        if not os.path.exists(os.path.join(DOCKER_FOLDER_GENERATED_DATA, str(self.execution.nb_rows))):
            os.makedirs(os.path.join(DOCKER_FOLDER_GENERATED_DATA, str(self.execution.nb_rows)))
        filepath_generated = f"{os.path.join(DOCKER_FOLDER_GENERATED_DATA, str(self.execution.nb_rows), filename)}"
        if filepath_generated.endswith(".csv"):
            df.to_csv(filepath_generated, index=False)
        else:
            df.to_excel(filepath_generated, index=False)
