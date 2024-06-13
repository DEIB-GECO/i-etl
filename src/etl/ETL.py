from src.config.BetterConfig import BetterConfig
from src.database.Database import Database
from src.etl.Extract import Extract
from src.etl.Load import Load
from src.etl.Transform import Transform
from src.utils.setup_logger import log


class ETL:
    def __init__(self, config: BetterConfig):
        self.config = config
        self.database = Database(config=self.config)

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
        self.compute_plots = False

        self.extract = Extract(database=self.database, run_analysis=self.run_analysis, config=self.config)
        self.load = Load(database=self.database, config=self.config)
        self.transform = Transform(extract=self.extract, load=self.load, database=self.database, config=self.config)

    def run(self):
        if self.extract_data:
            self.extract.run()
        if self.transform_data:
            self.transform.run()
        if self.load_data:
            self.load.run()
