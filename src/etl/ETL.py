import locale

from database.Database import Database
from database.Execution import Execution
from etl.Extract import Extract
from etl.Load import Load
from etl.Transform import Transform
from enums.DatasetsLocales import DatasetsLocales
from enums.HospitalNames import HospitalNames
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
            log.debug(f"custom locale: {DatasetsLocales[HospitalNames[self.execution.hospital_name]]}")
            locale.setlocale(category=locale.LC_NUMERIC, locale=DatasetsLocales[HospitalNames[self.execution.hospital_name]])

        log.info(f"Current locale is: {locale.getlocale(locale.LC_NUMERIC)}")

        # init ETL steps
        self.extract = None
        self.transform = None
        self.load = None

    def run(self) -> None:
        is_last_file = False
        file_counter = 0
        log.debug(f"{self.execution.clinical_filepaths}")
        log.debug(f"{type(self.execution.clinical_filepaths)}")
        for one_file in self.execution.clinical_filepaths:
            log.debug(f"{one_file}")
            file_counter = file_counter + 1
            if file_counter == len(self.execution.clinical_filepaths):
                is_last_file = True
            # set the current path in the config because the ETL only knows files declared in the config
            if one_file.startswith("/"):
                # this is an absolute filepath, so we keep it as is
                self.execution.current_filepath = one_file
            else:
                # this is a relative filepath, we consider it to be relative to the project root (BETTER-fairificator)
                # we need to add three times ".." because the data files are never copied to the working dir (but remain in their place)
                # NOPE: full_path = os.path.join(self.execution.working_dir_current, "..", "..", "..", str(one_file))
                self.execution.current_filepath = one_file

            log.info(f"--- Starting to ingest file '{self.execution.current_filepath}'")
            if self.execution.is_extract:
                self.extract = Extract(database=self.database, execution=self.execution)

                self.extract.run()
            if self.execution.is_transform:
                self.transform = Transform(database=self.database, execution=self.execution, data=self.extract.data, metadata=self.extract.metadata, mapped_values=self.extract.mapped_values)
                self.transform.run()
            if self.execution.is_load:
                # create indexes only if this is the last file (otherwise, we would create useless intermediate indexes)
                self.load = Load(database=self.database, execution=self.execution, create_indexes=is_last_file)
                self.load.run()
