import os

import pandas as pd
from pandas import DataFrame

from constants.structure import DOCKER_FOLDER_DATA, VCF_MOUNTED_DOCKER
from database.Execution import Execution
from enums.MetadataColumns import MetadataColumns
from enums.Profile import Profile
from enums.VcfColumns import get_vcf_column
from utils.setup_logger import log


class Preprocess:
    def __init__(self, execution: Execution, data: DataFrame, metadata: DataFrame, profile: str):
        self.execution = execution
        self.data = data
        self.metadata = metadata
        self.profile = profile

    def run(self):
        self.preprocess()
        if ".vcf" in os.getenv("DATA_FILES"):
            # do not add VCF if not VCF data has been given as input (even if there may be VCF data later)
            self.add_vcf_files_in_data()

    def preprocess(self):
        pass

    def add_vcf_files_in_data(self) -> None:
        if self.profile == Profile.GENOMIC:
            mapping_pid_vcf = []
            pid_column_name = os.getenv("PATIENT_ID")  # the data is not normalized yet, so we keep the non-normalized column name (not self.execution.patient_id_column_name)
            filepath_column_name = get_vcf_column(self.execution.hospital_name)
            for entry in os.getenv("DATA_FILES").split(","):
                if "*.vcf" not in entry:
                    continue
                # this is the directory which contains all the VCF files
                the_entry = os.path.dirname(entry)  # works for both "VCF-FILES/*.vcf" and "VCF-FILES/*.vcf.gz"
                log.info(the_entry)
                if the_entry == "":
                    the_entry = "."  # the VCF files are next to the data files
                for vcf_file in os.listdir(os.path.join(DOCKER_FOLDER_DATA, the_entry)):
                    if vcf_file.endswith(".vcf.gz"):
                        pid = vcf_file[:-len(".vcf.gz")]
                    elif vcf_file.endswith(".vcf"):
                        pid = vcf_file[:-len(".vcf")]
                    else:
                        log.info(f"skip non VCF file {vcf_file}")
                        continue
                    mapping_pid_vcf.append({pid_column_name: pid, filepath_column_name: os.path.join(VCF_MOUNTED_DOCKER, vcf_file)})
            if mapping_pid_vcf:
                pid_vcf_df = DataFrame(mapping_pid_vcf)
                # left-merge: keep every patient row; drop VCFs whose filename PID has no matching patient
                self.data = self.data.merge(pid_vcf_df, on=pid_column_name, how="left")

    @classmethod
    def get_subset_of_columns_in_df(cls, df: DataFrame, file_type: Profile, metadata: DataFrame) -> DataFrame:
        profile_filename = Profile.get_preprocess_data_filename(filetype=file_type)
        columns = metadata[metadata[MetadataColumns.PROFILE] == profile_filename][MetadataColumns.COLUMN_NAME]
        columns = [MetadataColumns.normalize_name(column_name) for column_name in columns]
        # df_samples = df_samples[sample_columns]  # nope, this raises an error if some of the columns to keep do not exist in the data
        return df.loc[:, pd.Index(columns).intersection(df.columns)]
