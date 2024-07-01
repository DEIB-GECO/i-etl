import locale
import os
import traceback

from config.BetterConfig import BetterConfig
from database.Database import Database
from database.Execution import Execution
from etl.Extract import Extract
from etl.Load import Load
from etl.Transform import Transform
from utils.Counter import Counter
from utils.HospitalNames import HospitalNames
from utils.constants import LOCALES
from utils.setup_logger import log


class ETL:
    def __init__(self, config: BetterConfig, database: Database):
        self.config = config
        self.database = database

        if self.database.check_server_is_up():
            log.info("The MongoDB client could be set up properly.")
        else:
            log.error("The MongoDB client could not be set up properly. The given connection string was %s.", self.config.get_db_connection())
            exit()

        # set the locale
        if self.config.get_use_en_locale():
            # this user explicitly asked for loading data with en_US locale
            log.debug("default locale: en_US")
            locale.setlocale(category=locale.LC_NUMERIC, locale="en_US")
        else:
            # we use the default locale assigned to each center based on their country
            log.debug("custom locale: %s", LOCALES[HospitalNames[self.config.get_hospital_name()].value])
            locale.setlocale(category=locale.LC_NUMERIC, locale=LOCALES[HospitalNames[self.config.get_hospital_name()].value])

        log.info("Current locale is: %s", locale.getlocale(locale.LC_NUMERIC))

        # init ETL steps
        self.extract = None
        self.transform = None
        self.load = None

    def run(self) -> None:
        error_occurred = False
        is_last_file = False
        file_counter = 0
        for one_file in self.config.get_data_filepaths():
            log.debug(one_file)
            file_counter = file_counter + 1
            if file_counter == len(self.config.get_data_filepaths()):
                is_last_file = True
            # set the current path in the config because the ETL only knows files declared in the config
            if one_file.startswith("/"):
                # this is an absolute filepath, so we keep it as is
                self.config.set_current_filepath(current_filepath=one_file)
            else:
                # this is a relative filepath, we consider it to be relative to the project root (BETTER-fairificator)
                # we need to add twice .. because the data files are never copied to the working dir (but remain in their place)
                full_path = os.path.join(self.config.get_working_dir_current(), "..", "..", str(one_file))
                self.config.set_current_filepath(current_filepath=full_path)

            log.info("--- Starting to ingest file '%s'", self.config.get_current_filepath())
            try:
                if self.config.get_extract():
                    self.extract = Extract(database=self.database, config=self.config)

                    self.extract.run()
                if self.config.get_transform():
                    self.transform = Transform(database=self.database, config=self.config, data=self.extract.data,
                                               metadata=self.extract.metadata, mapped_values=self.extract.mapped_values)
                    self.transform.run()
                if self.config.get_load():
                    # create indexes only if this is the last file (otherwise, we would create useless intermediate indexes)
                    self.load = Load(database=self.database, config=self.config, create_indexes=is_last_file)
                    self.load.run()
            except Exception:
                traceback.print_exc()  # print the stack trace
                log.error("An error occurred during the ETL. Please check the complete log. ")
                error_occurred = True

        # saving the execution parameters in the database before closing the execution
        log.info("Saving execution parameters in the database.")
        # we ensure to have an existing counter, otherwise we create a new one and set it to the max current id
        if self.transform is not None:
            counter_transform = self.transform.counter
        else:
            counter_transform = Counter()
            counter_transform.set_with_database(database=self.database)
        execution = Execution(config=self.config, database=self.database, counter=counter_transform)
        execution.store_in_database()

        if not error_occurred:
            log.info("All given files have been processed without error. Goodbye!")
        else:
            log.error("The script stopped at some point due to errors. Please check the complete log.")
