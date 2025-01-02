import locale
import os

import pandas as pd

from catalogue.FeatureProfileComputation import FeatureProfileComputation
from constants.structure import DOCKER_FOLDER_DATA
from database.Counter import Counter
from database.Database import Database
from database.Dataset import Dataset
from database.Execution import Execution
from entities.Hospital import Hospital
from enums.MetadataColumns import MetadataColumns
from enums.Profile import Profile
from enums.TableNames import TableNames
from etl.Extract import Extract
from etl.Load import Load
from etl.Reporting import Reporting
from etl.Transform import Transform
from statistics.DatabaseStatistics import DatabaseStatistics
from statistics.QualityStatistics import QualityStatistics
from statistics.TimeStatistics import TimeStatistics
from utils.file_utils import read_tabular_file_as_string, write_in_file
from utils.setup_logger import log


class ETL:
    def __init__(self, execution: Execution, database: Database):
        self.execution = execution
        self.database = database
        self.datasets = []

        # set the locale
        log.debug(f"use locale: {self.execution.use_locale}")
        locale.setlocale(category=locale.LC_NUMERIC, locale=self.execution.use_locale)
        log.info(f"Current locale is: {locale.getlocale(locale.LC_NUMERIC)}")

        # init ETL steps
        self.extract = None
        self.transform = None
        self.load = None
        self.reporting = None
        self.profile_computation = None

    def run(self) -> None:
        time_stats = TimeStatistics(record_stats=True)
        time_stats.start_total_execution_timer()
        compute_indexes = False
        dataset_number = 0
        file_counter = 0

        quality_stats = QualityStatistics(record_stats=True)
        all_filenames = os.getenv("DATA_FILES").split(",")
        log.info(all_filenames)

        log.info("********** create hospital")
        counter = Counter()
        counter.set_with_database(database=self.database)
        file_counter = self.create_hospital(counter=counter, dataset_number=dataset_number, file_counter=file_counter)

        all_metadata = read_tabular_file_as_string(self.execution.metadata_filepath)  # keep all metadata as str
        log.info(all_metadata)
        for one_filename in all_filenames:
            if one_filename != "":
                log.info(one_filename)
                dataset_number = dataset_number + 1

                # set the current filepath
                self.execution.current_filepath = os.path.join(DOCKER_FOLDER_DATA, one_filename)
                log.info(self.execution.current_filepath)

                # create a new Dataset instance
                counter.set_with_database(database=self.database)
                current_dataset_instance = Dataset(database=self.database, docker_path=self.execution.current_filepath, version_notes=None, license=None, counter=counter)
                self.datasets.append(current_dataset_instance)
                self.execution.current_dataset_identifier = current_dataset_instance.global_identifier

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

                        # EXTRACT
                        time_stats.start_total_extract_timer()
                        self.extract = Extract(metadata=metadata, profile=profile, database=self.database, execution=self.execution, quality_stats=quality_stats, time_stats=time_stats)
                        self.extract.run()
                        time_stats.stop_total_extract_timer()

                        if self.extract.metadata is not None:
                            log.info(f"running transform on dataset {self.execution.current_filepath} with profile {profile}")
                            # TRANSFORM
                            time_stats.start_total_transform_timer()
                            self.transform = Transform(database=self.database, execution=self.execution, data=self.extract.data,
                                                       metadata=self.extract.metadata,
                                                       mapping_categorical_value_to_onto_resource=self.extract.mapping_categorical_value_to_onto_resource,
                                                       mapping_column_to_categorical_value=self.extract.mapping_column_to_categorical_value,
                                                       mapping_column_to_unit=self.extract.mapping_column_to_unit,
                                                       profile=profile, load_patients=count_profiles == 1,
                                                       dataset_number=dataset_number, file_counter=file_counter, dataset_instance=current_dataset_instance,
                                                       quality_stats=quality_stats, time_stats=time_stats)
                            self.transform.run()
                            time_stats.stop_total_transform_timer()
                            file_counter = self.transform.file_counter

                            # LOAD
                            time_stats.start_total_load_timer()
                            log.info(f"{one_filename} -> {compute_indexes}")
                            # create indexes only if this is the last file (otherwise, we would create useless intermediate indexes)
                            self.load = Load(database=self.database, execution=self.execution, create_indexes=compute_indexes,
                                             dataset_number=dataset_number, profile=profile,
                                             quality_stats=quality_stats, time_stats=time_stats)
                            self.load.run()
                            time_stats.stop_total_load_timer()
                self.execution.current_file_number += 1

        # save the datasets in the DB
        if self.database is not None and len(self.datasets) > 0:
            log.info([dataset.to_json() for dataset in self.datasets])
            self.database.upsert_one_batch_of_tuples(table_name=TableNames.DATASET, unique_variables=["docker_path"], the_batch=[dataset.to_json() for dataset in self.datasets], ordered=False)
        # compute their profiles
        self.profile_computation = FeatureProfileComputation(database=self.database)
        self.profile_computation.compute_features_profiles()
        # finally, compute DB stats and give all (time, quality and db) stats to the report
        time_stats.stop_total_execution_timer()
        db_stats = DatabaseStatistics(record_stats=True)
        db_stats.compute_stats(database=self.database)
        self.reporting = Reporting(database=self.database, execution=self.execution, quality_stats=quality_stats, time_stats=time_stats, db_stats=db_stats)
        self.reporting.run()

    def create_hospital(self, counter: Counter, dataset_number: int, file_counter: int) -> int:
        log.info(f"create hospital instance in memory")
        cursor = self.database.find_operation(table_name=TableNames.HOSPITAL, filter_dict={"name": self.execution.hospital_name}, projection={})
        hospital_exists = False
        for _ in cursor:
            # the hospital already exists within the database, we do nothing
            # the ETL will take care of retrieving the existing hospital ID while creating records
            hospital_exists = True
        if not hospital_exists:
            # the hospital does not exist because we have reset the database, we create a new one
            new_hospital = Hospital(name=self.execution.hospital_name, counter=counter)
            hospitals = [new_hospital]
            write_in_file(resource_list=hospitals, current_working_dir=self.execution.working_dir_current,
                          profile=TableNames.HOSPITAL, is_feature=False, dataset_number=dataset_number, file_counter=file_counter)
            file_counter += 1
            self.database.load_json_in_table(profile=TableNames.HOSPITAL, table_name=TableNames.HOSPITAL, unique_variables=["name"], dataset_number=dataset_number)
        return file_counter
