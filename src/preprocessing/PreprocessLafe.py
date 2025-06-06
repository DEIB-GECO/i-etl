import os

import pandas as pd
from pandas import DataFrame

from database.Execution import Execution
from enums.DataTypes import DataTypes
from enums.MetadataColumns import MetadataColumns
from enums.Ontologies import Ontologies
from enums.Profile import Profile
from enums.Visibility import Visibility
from preprocessing.Preprocess import Preprocess
from utils.setup_logger import log


class PreprocessLafe(Preprocess):
    def __init__(self, execution: Execution, data: DataFrame, metadata: DataFrame, profile: str, all_columns_dataset: list):
        super().__init__(execution=execution, data=data, metadata=metadata, profile=profile)

        self.metadata = metadata
        self.all_columns_dataset = all_columns_dataset

    def preprocess(self):
        log.info("pre-process LAFE data")
        log.info(self.data)
        log.info(self.data.columns)
        normalized_columns = [MetadataColumns.normalize_name(column_name=column) for column in self.data.columns]
        log.info(self.execution.current_filepath)
        log.info(self.profile)
        log.info(self.execution.sample_id_column_name)
        if self.profile == Profile.CLINICAL and "Dynamic_Clinical_Table" in self.execution.current_filepath and self.execution.sample_id_column_name not in normalized_columns:
            # 1. add sample IDs for the data file "Dynamic_Clinical_Table.xlsx"
            self.execution.sample_id_column_name = "sample_id"
            self.data[self.execution.sample_id_column_name] = [i for i in range(1, len(self.data)+1)]
            log.info(self.data.columns)
            log.info(self.data)

            # 2. update the metadata to make the sample ID column appear
            df_line_sid = DataFrame({
                MetadataColumns.ONTO_NAME: [Ontologies.LOINC["name"]],
                MetadataColumns.ONTO_CODE: ["57723-9"],
                MetadataColumns.COLUMN_NAME: [self.execution.sample_id_column_name],
                MetadataColumns.PROFILE: [self.profile],
                MetadataColumns.DATASET_NAME: [os.path.basename(self.execution.current_filepath)],
                MetadataColumns.ETL_TYPE: [DataTypes.STRING],
                MetadataColumns.VISIBILITY: [Visibility.PUBLIC]
            })
            log.info(df_line_sid)
            log.info(self.metadata)
            self.metadata = pd.concat([self.metadata, df_line_sid])
            log.info(self.metadata)
            log.info(self.all_columns_dataset)
            self.all_columns_dataset = pd.concat([self.all_columns_dataset, pd.Series(data=[self.execution.sample_id_column_name])])
            log.info(self.all_columns_dataset)
