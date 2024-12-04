import locale
import os

import pandas as pd

from constants.locales import DATASET_LOCALES
from constants.structure import DOCKER_FOLDER_DATA
from database.Database import Database
from database.Execution import Execution
from enums.MetadataColumns import MetadataColumns
from enums.Profile import Profile
from etl.Extract import Extract
from etl.Load import Load
from etl.Reporting import Reporting
from etl.Transform import Transform
from statistics.DatabaseStatistics import DatabaseStatistics
from statistics.QualityStatistics import QualityStatistics
from statistics.TimeStatistics import TimeStatistics
from utils.file_utils import read_tabular_file_as_string
from utils.setup_logger import log


class ETL:
    def __init__(self, execution: Execution, database: Database):
        self.execution = execution
        self.database = database

        # set the locale
        if self.execution.use_en_locale:
            # this user explicitly asked for loading data with en_US locale
            log.debug(f"default locale: en_US")
            locale.setlocale(category=locale.LC_NUMERIC, locale="en_US")
        else:
            # we use the default locale assigned to each center based on their country
            log.debug(f"custom locale: {DATASET_LOCALES[self.execution.hospital_name]}")
            locale.setlocale(category=locale.LC_NUMERIC, locale=DATASET_LOCALES[self.execution.hospital_name])

        log.info(f"Current locale is: {locale.getlocale(locale.LC_NUMERIC)}")

        # init ETL steps
        self.extract = None
        self.transform = None
        self.load = None
        self.reporting = None

    def run(self) -> None:
        time_stats = TimeStatistics(record_stats=True)
        time_stats.start_total_execution_timer()
        compute_indexes = False
        dataset_number = 0
        file_counter = 0

        quality_stats = QualityStatistics(record_stats=True)
        all_filenames = os.getenv("DATA_FILES").split(",")
        log.info(all_filenames)

        self.transform = Transform(database=self.database, execution=self.execution, data=None, metadata=None,
                                   mapping_categorical_value_to_onto_resource=None, mapping_column_to_unit=None,
                                   mapping_column_to_categorical_value=None, patient_ids_mapping=None, profile=None,
                                   dataset_number=dataset_number, file_counter=file_counter,
                                   quality_stats=quality_stats, time_stats=time_stats)

        log.info("********** create hospital")
        self.transform.set_resource_counter_id()
        self.transform.create_hospital()
        file_counter = self.transform.file_counter

        all_metadata = read_tabular_file_as_string(self.execution.metadata_filepath)  # keep all metadata as str
        log.info(all_metadata)
        for one_filename in all_filenames:
            log.info(one_filename)
            dataset_number = dataset_number + 1
            self.transform.dataset_number = dataset_number
            # set the current filepath
            self.execution.current_filepath = os.path.join(DOCKER_FOLDER_DATA, one_filename)
            log.info(self.execution.current_filepath)

            # get metadata of file
            log.info(one_filename)
            if one_filename not in all_metadata[MetadataColumns.DATASET_NAME].unique():
                raise ValueError(f"The current dataset ({one_filename}) is not described in the provided metadata file.")
            else:
                log.info(f"--- Extract metadata for file '{self.execution.current_filepath}', with number {self.execution.current_file_number}")
                metadata = pd.DataFrame(all_metadata[all_metadata[MetadataColumns.DATASET_NAME] == one_filename])

                log.info(f"--- Starting to transform file '{self.execution.current_filepath}', with number {self.execution.current_file_number}")
                # we have to iterate over all profiles associated to the dataset because we cannot do it in the Extract
                # because the transform and load have to be applied on each pair (ds, profile)
                # the Extract class will take care of setting the profile if it is associated to the current dataset
                unique_profiles_of_current_dataset = pd.unique(metadata[MetadataColumns.PROFILE])
                count_profiles = 0
                for profile in unique_profiles_of_current_dataset:
                    count_profiles += 1
                    profile = Profile.normalize(profile)
                    log.info(f"using profile {profile}")

                    # check whether this is the last profile of the last dataset
                    # to know whether we should compute indexes
                    if dataset_number == len(all_filenames) and count_profiles == len(unique_profiles_of_current_dataset):
                        compute_indexes = True

                    #### EXTRACT
                    time_stats.start_total_extract_timer()
                    self.extract = Extract(metadata=metadata, profile=profile, database=self.database, execution=self.execution, quality_stats=quality_stats, time_stats=time_stats)
                    self.extract.run()
                    time_stats.stop_total_extract_timer()

                    if self.extract.metadata is not None:
                        log.info(f"running transform on dataset {self.execution.current_filepath} with profile {profile}")
                        #### TRANSFORM
                        time_stats.start_total_transform_timer()
                        self.transform = Transform(database=self.database, execution=self.execution, data=self.extract.data,
                                                   metadata=self.extract.metadata,
                                                   mapping_categorical_value_to_onto_resource=self.extract.mapping_categorical_value_to_onto_resource,
                                                   mapping_column_to_categorical_value=self.extract.mapping_column_to_categorical_value,
                                                   mapping_column_to_unit=self.extract.mapping_column_to_unit,
                                                   patient_ids_mapping=self.transform.patient_ids_mapping,
                                                   profile=profile,
                                                   dataset_number=dataset_number, file_counter=file_counter,
                                                   quality_stats=quality_stats, time_stats=time_stats)
                        self.transform.run()
                        time_stats.stop_total_transform_timer()
                        file_counter = self.transform.file_counter

                        #### LOAD
                        time_stats.start_total_load_timer()
                        log.info(f"{one_filename} -> {compute_indexes}")
                        # create indexes only if this is the last file (otherwise, we would create useless intermediate indexes)
                        self.load = Load(database=self.database, execution=self.execution, create_indexes=compute_indexes,
                                         dataset_number=dataset_number, profile=profile,
                                         quality_stats=quality_stats, time_stats=time_stats)
                        self.load.run()
                        time_stats.stop_total_load_timer()
                self.execution.current_file_number += 1
        # finally, compute DB stats and give all (time, quality and db) stats to the report
        time_stats.stop_total_execution_timer()
        db_stats = DatabaseStatistics(record_stats=True)
        db_stats.compute_stats(database=self.database)
        self.reporting = Reporting(database=self.database, execution=self.execution, quality_stats=quality_stats, time_stats=time_stats, db_stats=db_stats)
        self.reporting.run()
