from pandas import DataFrame

from database.Execution import Execution
from enums.MetadataColumns import MetadataColumns
from enums.Profile import Profile
from preprocessing.Preprocess import Preprocess
from utils.setup_logger import log


class PreprocessHsjd(Preprocess):
    def __init__(self, execution: Execution, data: DataFrame, metadata: DataFrame, profile: str):
        super().__init__(execution=execution, data=data, profile=profile)

        self.metadata = metadata
        self.mapping_full_name_to_var_name = {}

    def preprocess(self):
        log.info("pre-process HSJD data")
        log.info(self.data)
        log.info(self.data.columns)
        # 1. add patient IDs for the data file "Phenotypic_Table.xlsx"
        # (other files containing phenotypic data do already contain patient IDs, starting from 1)
        normalized_columns = [MetadataColumns.normalize_name(column_name=column) for column in self.data.columns]
        if self.profile == Profile.PHENOTYPIC and "Phenotypic_Table" in self.execution.current_filepath and self.execution.patient_id_column_name not in normalized_columns:
            self.data[self.execution.patient_id_column_name] = [i for i in range(1, len(self.data)+1)]
        log.info(self.data.columns)
        log.info(self.data)
