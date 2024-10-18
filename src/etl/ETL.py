import locale

from database.Database import Database
from database.Execution import Execution
from enums.FileTypes import FileTypes
from enums.HospitalNames import HospitalNames
from etl.Extract import Extract
from etl.Load import Load
from etl.Reporting import Reporting
from etl.Transform import Transform
from constants.locales import DATASET_LOCALES
from preprocessing.PreprocessBuzziUC1 import PreprocessBuzziUC1
from preprocessing.PreprocessCovid import PreprocessCovid
from preprocessing.PreprocessEda import PreprocessEda
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

        # 1. aggregate all filepaths given by the user
        # in order to be able to ingest them all with a single loop
        # we will also store the file type (phen., diagnosis, etc) so that Transform knows in which table store data
        all_filepaths = {}
        if self.execution.sample_filepaths is not None:
            for sample_filepath in self.execution.sample_filepaths:
                all_filepaths[sample_filepath] = FileTypes.SAMPLE
        if self.execution.phenotypic_filepaths is not None:
            for phenotypic_filepath in self.execution.phenotypic_filepaths:
                all_filepaths[phenotypic_filepath] = FileTypes.PHENOTYPIC
        if self.execution.diagnosis_filepaths is not None:
            for diagnosis_filepath in self.execution.diagnosis_filepaths:
                all_filepaths[diagnosis_filepath] = FileTypes.DIAGNOSIS
        if self.execution.medicine_filepaths is not None:
            for medicine_filepath in self.execution.medicine_filepaths:
                all_filepaths[medicine_filepath] = FileTypes.MEDICINE
        if self.execution.genomic_filepaths is not None:
            for genomic_filepath in self.execution.genomic_filepaths:
                all_filepaths[genomic_filepath] = FileTypes.GENOMIC
        if self.execution.imaging_filepaths is not None:
            for imaging_filepath in self.execution.imaging_filepaths:
                all_filepaths[imaging_filepath] = FileTypes.IMAGING

        # 2. iterate over all the files
        log.info(all_filepaths)
        quality_stats = QualityStatistics(record_stats=True)
        for one_file in all_filepaths.keys():
            file_counter = file_counter + 1
            if file_counter == len(all_filepaths):
                is_last_file = True
            # set the current filepath
            # this may be an absolute path (starting with /)
            # or a relative filepath, for which we consider it to be relative to the project root (BETTER-fairificator)
            self.execution.current_filepath = one_file
            self.execution.current_file_type = all_filepaths[one_file]

            if self.execution.is_extract:
                log.info(f"--- Starting to ingest file '{self.execution.current_filepath}' of type {self.execution.current_file_type}")
                time_stats.start_total_extract_timer()
                self.extract = Extract(database=self.database, execution=self.execution, quality_stats=quality_stats, time_stats=time_stats)
                self.extract.run()
                time_stats.stop_total_extract_timer()
                if self.execution.is_transform:
                    time_stats.start_total_transform_timer()
                    self.transform = Transform(database=self.database, execution=self.execution, data=self.extract.data,
                                               metadata=self.extract.metadata,
                                               mapping_categorical_value_to_onto_resource=self.extract.mapping_categorical_value_to_onto_resource,
                                               mapping_column_to_categorical_value=self.extract.mapping_column_to_categorical_value,
                                               mapping_column_to_dimension=self.extract.mapping_column_to_dimension,
                                               patient_ids_mapping=self.extract.patient_ids_mapping,
                                               mapping_diagnosis_to_onto_resource=self.extract.mapping_diagnosis_to_onto_resource,
                                               quality_stats=quality_stats, time_stats=time_stats)
                    self.transform.run()
                    time_stats.stop_total_transform_timer()
                    if self.execution.is_load:
                        # create indexes only if this is the last file (otherwise, we would create useless intermediate indexes)
                        time_stats.start_total_load_timer()
                        self.load = Load(database=self.database, execution=self.execution, create_indexes=is_last_file, quality_stats=quality_stats, time_stats=time_stats)
                        self.load.run()
                        time_stats.stop_total_load_timer()
        # finally, compute DB stats and give all (time, quality and db) stats to the report
        time_stats.stop_total_execution_timer()
        db_stats = DatabaseStatistics(record_stats=True)
        db_stats.compute_stats(database=self.database)
        self.reporting = Reporting(database=self.database, execution=self.execution, quality_stats=quality_stats, time_stats=time_stats, db_stats=db_stats)
        self.reporting.run()
