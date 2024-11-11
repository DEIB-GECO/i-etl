import locale
import os

from constants.locales import DATASET_LOCALES
from constants.structure import DOCKER_FOLDER_DATA
from database.Database import Database
from database.Execution import Execution
from enums.Profile import Profile
from etl.Extract import Extract
from etl.Load import Load
from etl.Reporting import Reporting
from etl.Transform import Transform
from statistics.DatabaseStatistics import DatabaseStatistics
from statistics.QualityStatistics import QualityStatistics
from statistics.TimeStatistics import TimeStatistics
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
        is_last_file = False
        file_counter = 0

        quality_stats = QualityStatistics(record_stats=True)
        all_filenames = os.getenv("DATA_FILES").split(",")
        log.info(all_filenames)
        for one_filename in all_filenames:
            log.info(one_filename)
            file_counter = file_counter + 1
            if file_counter == len(all_filenames):
                is_last_file = True
            # set the current filepath
            self.execution.current_filepath = os.path.join(DOCKER_FOLDER_DATA, one_filename)
            log.info(self.execution.current_filepath)

            log.info(f"--- Starting to ingest file '{self.execution.current_filepath}'")

            # we have to iterate over all profiles because we do not know yet
            # which dataset is associated to which subset of profiles
            # the Extract class will take care of setting the profile if it is associated to the current dataset
            for profile in Profile.values():
                log.info(f"using profile {profile}")
                #### EXTRACT
                time_stats.start_total_extract_timer()
                self.extract = Extract(profile=profile, database=self.database, execution=self.execution, quality_stats=quality_stats, time_stats=time_stats)
                self.extract.run()
                time_stats.stop_total_extract_timer()

                if self.extract.metadata is not None:
                    log.info(f"running transform on dataset {self.execution.current_filepath} with profile {self.execution.current_file_profile}")
                    #### TRANSFORM
                    time_stats.start_total_transform_timer()
                    self.transform = Transform(database=self.database, execution=self.execution, data=self.extract.data,
                                               metadata=self.extract.metadata,
                                               mapping_categorical_value_to_onto_resource=self.extract.mapping_categorical_value_to_onto_resource,
                                               mapping_column_to_categorical_value=self.extract.mapping_column_to_categorical_value,
                                               mapping_column_to_dimension=self.extract.mapping_column_to_dimension,
                                               patient_ids_mapping=self.extract.patient_ids_mapping,
                                               quality_stats=quality_stats, time_stats=time_stats)
                    self.transform.run()
                    time_stats.stop_total_transform_timer()

                    #### LOAD
                    time_stats.start_total_load_timer()
                    # create indexes only if this is the last file (otherwise, we would create useless intermediate indexes)
                    self.load = Load(database=self.database, execution=self.execution, create_indexes=is_last_file, quality_stats=quality_stats, time_stats=time_stats)
                    self.load.run()
                    time_stats.stop_total_load_timer()
        # finally, compute DB stats and give all (time, quality and db) stats to the report
        time_stats.stop_total_execution_timer()
        db_stats = DatabaseStatistics(record_stats=True)
        db_stats.compute_stats(database=self.database)
        self.reporting = Reporting(database=self.database, execution=self.execution, quality_stats=quality_stats, time_stats=time_stats, db_stats=db_stats)
        self.reporting.run()
