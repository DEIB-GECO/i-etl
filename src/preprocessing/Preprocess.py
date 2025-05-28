import os

import pandas as pd
from pandas import DataFrame

from constants.structure import DOCKER_FOLDER_DATA
from database.Execution import Execution
from enums.MetadataColumns import MetadataColumns
from enums.Profile import Profile
from enums.VcfColumns import vcf_columns
from utils.setup_logger import log


class Preprocess:
    def __init__(self, execution: Execution, data: DataFrame, profile: str):
        self.execution = execution
        self.data = data
        self.profile = profile

    def run(self):
        self.preprocess()
        self.add_vcf_files_in_data()

    def preprocess(self):
        pass

    def add_vcf_files_in_data(self) -> None:
        if self.profile == Profile.GENOMIC:
            mapping_pid_vcf = []
            pid_column_name = os.getenv("PATIENT_ID")  # nope: the data is not normalized yet, so we keep the non)normalized column name self.execution.patient_id_column_name
            filepath_column_name = vcf_columns[self.execution.hospital_name]
            if filepath_column_name is not None:
                for entry in os.getenv("DATA_FILES").split(","):
                    if "*.vcf" in entry:
                        # this is the directory which contains all the VCF files
                        the_entry = entry.replace("*.vcf", "")  # os.listdir requires the folder name (VCF-FILES/) without the specification
                        log.info(the_entry)
                        if the_entry == "":
                            the_entry = "."  # the VCF files are next to the data files
                        for vcf_file in os.listdir(os.path.join(DOCKER_FOLDER_DATA, the_entry)):
                            if ".vcf" in vcf_file:
                                pid = vcf_file.replace(".vcf", "")
                                mapping_pid_vcf.append({pid_column_name: pid, filepath_column_name: os.path.join(os.getenv("SERVER_FOLDER_DATA"), vcf_file)})
                            else:
                                log.info(f"skip non VCF file {vcf_file}")
                    else:
                        # log.info(f"Skip {entry}")
                        pass
                pid_vcf_df = DataFrame(mapping_pid_vcf)
                self.data = self.data.merge(pid_vcf_df, on=pid_column_name, how="outer")
                log.info(self.data.columns)

    @classmethod
    def get_subset_of_columns_in_df(cls, df: DataFrame, file_type: Profile, metadata: DataFrame) -> DataFrame:
        profile_filename = Profile.get_preprocess_data_filename(filetype=file_type)
        columns = metadata[metadata[MetadataColumns.PROFILE] == profile_filename][MetadataColumns.COLUMN_NAME]
        columns = [MetadataColumns.normalize_name(column_name) for column_name in columns]
        # df_samples = df_samples[sample_columns]  # nope, this raises an error if some of the columns to keep do not exist in the data
        return df.loc[:, pd.Index(columns).intersection(df.columns)]
