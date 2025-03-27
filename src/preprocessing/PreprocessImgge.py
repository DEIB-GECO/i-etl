import pandas as pd
from pandas import DataFrame

from database.Execution import Execution
from enums.MetadataColumns import MetadataColumns
from preprocessing.Preprocess import Preprocess
from utils.setup_logger import log


class PreprocessImgge(Preprocess):
    def __init__(self, execution: Execution, data: DataFrame, metadata: DataFrame, profile: str):
        super().__init__(execution=execution, data=data, profile=profile)

        self.metadata = metadata
        self.mapping_full_name_to_var_name = {}

    def run(self):
        log.info("pre-process IMGGE data")
        log.info(self.data)
        log.info(self.data.columns)
        # 1. remove the first two columns containing the data type (clinical, genetic, etc)
        self.data = self.data[[column for column in self.data.columns if not column.startswith("Unnamed")]]
        # 2. transpose the data to get patients in rows, variables in columns
        self.data.set_index("Columns", inplace=True)
        self.data = self.data.transpose()
        self.data.reset_index()
        log.info(self.data.columns)
        # 3. Rename columns with new names
        # index is the map key, the other is the value
        self.mapping_full_name_to_var_name = pd.Series(self.metadata.loc[self.metadata[MetadataColumns.PROFILE] == self.profile]["name"].values, index=self.metadata.loc[self.metadata[MetadataColumns.PROFILE] == self.profile]["old_name_(historical_purposes,_do_not_use)"]).to_dict()
        log.info(self.mapping_full_name_to_var_name)
        self.data.rename(columns=self.mapping_full_name_to_var_name, inplace=True)
        log.info(self.data.columns)
        log.info([value for value in self.mapping_full_name_to_var_name.values()])
        log.info([value for value in self.mapping_full_name_to_var_name.values() if value in self.data.columns])
        self.data = self.data[[value for value in self.mapping_full_name_to_var_name.values() if value in self.data.columns]]
        log.info(self.data.columns)
        # 4. Add the column with patient ids for the data kinds that do not have it
        if "record_id" not in self.data.columns:
            self.data["record_id"] = self.data.index

        log.info(self.data.columns)
        log.info(self.data)
