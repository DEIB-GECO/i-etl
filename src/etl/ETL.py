import locale

from database.Database import Database
from database.Execution import Execution
from enums.FileTypes import FileTypes
from etl.Extract import Extract
from etl.Load import Load
from etl.Transform import Transform
from utils.constants import DATASET_LOCALES
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

    def run(self) -> None:
        is_last_file = False
        file_counter = 0
        log.debug(f"{self.execution.laboratory_filepaths}")
        log.debug(f"{type(self.execution.laboratory_filepaths)}")

        # 1. aggregate all filepaths given by the user
        # in order to be able to ingest them all with a single loop
        # we will also store the file type (lab., diagnosis, etc) so that Transform knows in which table store data
        all_filepaths = {}
        if self.execution.laboratory_filepaths is not None:
            for laboratory_filepath in self.execution.laboratory_filepaths:
                all_filepaths[laboratory_filepath] = FileTypes.LABORATORY
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
        for one_file in all_filepaths.keys():
            log.debug(f"{one_file}")
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
                self.extract = Extract(database=self.database, execution=self.execution)
                self.extract.run()
                if self.execution.is_transform:
                    self.transform = Transform(database=self.database, execution=self.execution, data=self.extract.data,
                                               metadata=self.extract.metadata,
                                               mapping_categorical_value_to_cc=self.extract.mapping_categorical_value_to_cc,
                                               mapping_column_to_categorical_value=self.extract.mapping_column_to_categorical_value,
                                               mapping_column_to_dimension=self.extract.mapping_column_to_dimension,
                                               patient_ids_mapping=self.extract.patient_ids_mapping,
                                               diagnosis_classification=self.extract.mapping_disease_to_classification,
                                               mapping_diagnosis_to_cc=self.extract.mapping_disease_to_cc)
                    self.transform.run()
                    if self.execution.is_load:
                        # create indexes only if this is the last file (otherwise, we would create useless intermediate indexes)
                        self.load = Load(database=self.database, execution=self.execution, create_indexes=is_last_file)
                        self.load.run()
