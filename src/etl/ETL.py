import locale
import os

import pandas as pd

from catalogue.FeatureProfileComputation import FeatureProfileComputation
from constants.defaults import NO_ID
from constants.structure import DOCKER_FOLDER_DATA
from database.Counter import Counter
from database.Database import Database
from entities.Dataset import Dataset
from database.Execution import Execution
from entities.Hospital import Hospital
from enums.MetadataColumns import MetadataColumns
from enums.Profile import Profile
from enums.TableNames import TableNames
from enums.TimerKeys import TimerKeys
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
        time_stats.start(dataset=None, key=TimerKeys.TOTAL_TIME)
        compute_indexes = False

        quality_stats = QualityStatistics(record_stats=True)
        all_filenames = os.getenv("DATA_FILES").split(",")
        log.info(all_filenames)

        log.info("********** create hospital")
        counter = Counter()
        counter.set_with_database(database=self.database)

        all_metadata = read_tabular_file_as_string(self.execution.metadata_filepath)  # keep all metadata as str
        first = True
        for one_filename in all_filenames:
            if ".vcf" in one_filename:
                # we probably have a regex for specifying the VCF location (*.vcf or my_folder/*.vcf)
                # we will process them during the pre-processing of the data
                pass
            elif one_filename != "":
                log.info(one_filename)

                # set the current filepath
                self.execution.current_filepath = os.path.join(DOCKER_FOLDER_DATA, one_filename)
                log.info(self.execution.current_filepath)

                # create a new Dataset instance
                counter.set_with_database(database=self.database)
                dataset = Dataset(identifier=NO_ID, database=self.database, docker_path=self.execution.current_filepath, version_notes=None, license=None, counter=counter)
                self.datasets.append(dataset)
                self.execution.current_dataset_gid = dataset.global_identifier

                # create the hospital only once
                if first:
                    self.create_hospital(counter=counter, dataset_id=dataset.identifier)
                    first = False

                # get metadata of file
                log.info(one_filename)
                if one_filename not in all_metadata[MetadataColumns.DATASET_NAME].unique():
                    raise ValueError(f"The current dataset ({one_filename}) is not described in the provided metadata file.")
                else:
                    log.info(f"--- Extract metadata for file '{self.execution.current_filepath}', with number {self.execution.current_file_number}")
                    metadata = pd.DataFrame(all_metadata[all_metadata[MetadataColumns.DATASET_NAME].values == one_filename])

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
                        if dataset.identifier == len(all_filenames) and count_profiles == len(unique_profiles_of_current_dataset):
                            compute_indexes = True

                        # EXTRACT
                        time_stats.start(dataset=dataset.global_identifier, key=TimerKeys.EXTRACT_TIME)
                        self.extract = Extract(metadata=metadata, profile=profile, database=self.database, execution=self.execution, quality_stats=quality_stats)
                        self.extract.run()
                        time_stats.increment(dataset=dataset.global_identifier, key=TimerKeys.EXTRACT_TIME)

                        if self.extract.metadata is not None:
                            log.info(f"running transform on dataset {self.execution.current_filepath} with profile {profile}")
                            # TRANSFORM
                            time_stats.start(dataset=dataset.global_identifier, key=TimerKeys.TRANSFORM_TIME)
                            self.transform = Transform(database=self.database, execution=self.execution, data=self.extract.data,
                                                       metadata=self.extract.metadata,
                                                       mapping_column_to_categorical_value=self.extract.mapping_column_to_categorical_value,
                                                       mapping_column_to_unit=self.extract.mapping_column_to_unit,
                                                       mapping_column_to_domain=self.extract.mapping_column_to_domain,
                                                       mapping_column_to_type=None,  # this will be computed during the Transform step
                                                       profile=profile, load_patients=count_profiles == 1,
                                                       dataset_id=dataset.identifier, dataset_key=dataset,
                                                       quality_stats=quality_stats)
                            self.transform.run()
                            time_stats.increment(dataset=dataset.global_identifier, key=TimerKeys.TRANSFORM_TIME)

                            # LOAD
                            time_stats.start(dataset=dataset.global_identifier, key=TimerKeys.LOAD_TIME)
                            # log.info(f"{one_filename} -> {compute_indexes}")
                            # create indexes only if this is the last file (otherwise, we would create useless intermediate indexes)
                            self.load = Load(database=self.database, execution=self.execution, create_indexes=compute_indexes,
                                             dataset_id=dataset.identifier, profile=profile,
                                             quality_stats=quality_stats)
                            self.load.run()
                            time_stats.increment(dataset=dataset.global_identifier, key=TimerKeys.LOAD_TIME)
                self.execution.current_file_number += 1

        # save the datasets in the DB
        log.info(len(self.datasets))
        log.info(self.datasets[0])
        log.info(self.datasets)
        if self.database is not None and len(self.datasets) > 0:
            log.info([dataset.to_json() for dataset in self.datasets])
            self.database.upsert_one_batch_of_tuples(table_name=TableNames.DATASET, unique_variables=["docker_path"], the_batch=[dataset.to_json() for dataset in self.datasets], ordered=False)
            # for Dataset entity only, we create an index on the global identifier
            self.database.create_unique_index(table_name=TableNames.DATASET, columns={"global_identifier": 1})
        # compute their profiles
        log.info("profile computation")
        self.profile_computation = FeatureProfileComputation(database=self.database)
        self.profile_computation.compute_features_profiles()
        # compute DB stats
        db_stats = DatabaseStatistics(record_stats=True)
        db_stats.compute_stats(database=self.database)
        # compute the final report with all the stats
        self.reporting = Reporting(database=self.database, execution=self.execution, quality_stats=quality_stats, time_stats=time_stats, db_stats=db_stats)
        self.reporting.run()

    def create_hospital(self, counter: Counter, dataset_id: int) -> None:
        log.info(f"create hospital instance in memory")
        cursor = self.database.find_operation(table_name=TableNames.HOSPITAL, filter_dict={Hospital.NAME_: self.execution.hospital_name}, projection={})
        hospital_exists = False
        for _ in cursor:
            # the hospital already exists within the database, we do nothing
            # the ETL will take care of retrieving the existing hospital ID while creating records
            hospital_exists = True
        if not hospital_exists:
            # the hospital does not exist because we have reset the database, we create a new one
            log.info(self.execution.hospital_name)
            new_hospital = Hospital(identifier=NO_ID, name=self.execution.hospital_name, counter=counter)
            log.info(new_hospital)
            hospitals = [new_hospital.to_json()]
            write_in_file(resource_list=hospitals, current_working_dir=self.execution.working_dir_current,
                          table_name=TableNames.HOSPITAL, is_feature=False, dataset_id=dataset_id, to_json=False)
            self.database.load_json_in_table(table_name=TableNames.HOSPITAL, unique_variables=[Hospital.NAME_], dataset_id=dataset_id)
