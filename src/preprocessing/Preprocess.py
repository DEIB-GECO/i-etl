import pandas as pd
from pandas import DataFrame

from database.Execution import Execution
from enums.MetadataColumns import MetadataColumns
from enums.Profile import Profile


class Preprocess:
    def __init__(self, execution: Execution, data: DataFrame, profile: Profile):
        self.execution = execution
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
