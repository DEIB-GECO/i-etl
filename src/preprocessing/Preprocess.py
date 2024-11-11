import os

import pandas as pd
from pandas import DataFrame

from constants.structure import DOCKER_FOLDER_PREPROCESSED
from database.Execution import Execution
from enums.Profile import Profile
from enums.MetadataColumns import MetadataColumns
from utils.setup_logger import log


class Preprocess:
    def __init__(self, execution: Execution, metadata: DataFrame, data: DataFrame, profile: Profile):
        self.execution = execution
        self.metadata = metadata
        self.data = data
        self.profile = profile

    def run(self):
        raise NotImplementedError("This method should be implemented in every Preprocessing child class.")

    @classmethod
    def get_subset_of_columns_in_df(cls, df: DataFrame, file_type: Profile, metadata: DataFrame) -> DataFrame:
        profile_filename = Profile.get_preprocess_data_filename(filetype=file_type)
        columns = metadata[metadata[MetadataColumns.PROFILE] == profile_filename][MetadataColumns.COLUMN_NAME]
        columns = [MetadataColumns.normalize_name(column_name) for column_name in columns]
        # df_samples = df_samples[sample_columns]  # nope, this raises an error if some of the columns to keep do not exist in the data
        return df.loc[:, pd.Index(columns).intersection(df.columns)]

    @classmethod
    def save_preprocessed_file(cls, df: DataFrame, file_type: str) -> None:
        docker_prefix = Profile.get_prefix_for_path(file_type)
        docker_path = os.path.join(docker_prefix, DOCKER_FOLDER_PREPROCESSED)
        if not os.path.exists(docker_path):
            os.makedirs(docker_path)
        filename = Profile.get_preprocess_data_filename(filetype=file_type)
        filepath_processed = os.path.join(docker_path, filename)
        df.to_csv(filepath_processed, index=False)
