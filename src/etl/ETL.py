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
        self._execution = execution
        self._database = database

        # set the locale
        if self._execution.use_en_locale:
            # this user explicitly asked for loading data with en_US locale
            log.debug(f"default locale: en_US")
            locale.setlocale(category=locale.LC_NUMERIC, locale="en_US")
        else:
            # we use the default locale assigned to each center based on their country
            log.debug(f"custom locale: {DatasetsLocales[HospitalNames[self._execution.hospital_name].value].value}")
            locale.setlocale(category=locale.LC_NUMERIC, locale=DatasetsLocales[HospitalNames[self._execution.hospital_name].value].value)

        log.info(f"Current locale is: {locale.getlocale(locale.LC_NUMERIC)}")

        # init ETL steps
        self._extract = None
        self._transform = None
        self._load = None

    def run(self) -> None:
        is_last_file = False
        file_counter = 0
        log.debug(f"{self._execution.clinical_filepaths}")
        log.debug(f"{type(self._execution.clinical_filepaths)}")
        for one_file in self._execution.clinical_filepaths:
            log.debug(f"{one_file}")
            file_counter = file_counter + 1
            if file_counter == len(self._execution.clinical_filepaths):
                is_last_file = True
            # set the current path in the config because the ETL only knows files declared in the config
            if one_file.startswith("/"):
                # this is an absolute filepath, so we keep it as is
                self._execution.current_filepath = one_file
            else:
                # this is a relative filepath, we consider it to be relative to the project root (BETTER-fairificator)
                # we need to add three times ".." because the data files are never copied to the working dir (but remain in their place)
                # NOPE: full_path = os.path.join(self._execution.working_dir_current, "..", "..", "..", str(one_file))
                self._execution.current_filepath = one_file

            log.info(f"--- Starting to ingest file '{self._execution.current_filepath}'")
            if self._execution.is_extract:
                self._extract = Extract(database=self._database, execution=self._execution)

                self._extract.run()
            if self._execution.is_transform:
                self._transform = Transform(database=self._database, execution=self._execution, data=self._extract.data, metadata=self._extract.metadata, mapped_values=self._extract.mapped_values)
                self._transform.run()
            if self._execution.is_load:
                # create indexes only if this is the last file (otherwise, we would create useless intermediate indexes)
                self._load = Load(database=self._database, execution=self._execution, create_indexes=is_last_file)
                self._load.run()
