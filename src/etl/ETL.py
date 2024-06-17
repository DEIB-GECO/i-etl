import os
import traceback

from src.config.BetterConfig import BetterConfig
from src.database.Database import Database
from src.etl.Extract import Extract
from src.etl.Load import Load
from src.etl.Transform import Transform
from src.utils.setup_logger import log


class ETL:
    def __init__(self, config: BetterConfig, database: Database):
        self.config = config
        self.database = database

        if self.database.check_server_is_up():
            log.info("The MongoDB client could be set up properly.")
        else:
            log.error("The MongoDB client could not be set up properly. The given connection string was %s.", self.config.get_db_connection())
            exit()

        # flags to know what to do during the ETL process
        self.extract_data = True
        self.run_analysis = False
        self.transform_data = True
        self.load_data = True

        self.extract = Extract(database=self.database, run_analysis=self.run_analysis, config=self.config)
        self.load = Load(database=self.database, config=self.config)
        self.transform = Transform(extract=self.extract, load=self.load, database=self.database, config=self.config)

    def run(self):
        error_occurred = False
        for one_file in self.config.get_data_filepaths():
            log.debug(one_file)
            # set the current path in the config because the ETL only knows files declared in the config
            if one_file.startswith("/"):
                # this is an absolute filepath, so we keep it as is
                self.config.set_current_filepath(one_file)
            else:
                # this is a relative filepath, we consider it to be relative to the project root (BETTER-fairificator)
                # we need to add twice .. because the data files are never copied to the working dir (but remain in their place)
                self.config.set_current_filepath(os.path.join(self.config.get_working_dir_current(), "..", "..", one_file))

            log.info("--- Starting to ingest file '%s'", self.config.get_current_filepath())
            try:
                if self.extract_data:
                    self.extract.run()
                if self.transform_data:
                    self.transform.run()
                if self.load_data:
                    self.load.run()
            except Exception:
                traceback.print_exc()  # print the stack trace
                log.error("An error occurred during the ETL. Please check the complete log. ")
                error_occurred = True

        if not error_occurred:
            log.info("All given files have been processed without error. Goodbye!")
        else:
            log.error("The script stopped at some point due to errors. Please check the complete log.")
