from src.database.Database import Database
from src.etl.Extract import Extract
from src.etl.Load import Load
from src.etl.Transform import Transform
from src.profiles.Hospital import Hospital
from src.utils.TableNames import TableNames
from src.utils.setup_logger import log
from random import randrange


class ETL:
    def __init__(self, config, metadata_filepath: str, data_filepath: str):
        self.config = config
        self.database = Database(self.config)
        if config.get("DATABASE", "reset"):
            self.database.reset()

        # flags to know what to do during the ETL process
        self.extract_data = True
        self.run_analysis = False
        self.transform_data = True
        self.load_data = True
        self.compute_plots = False

        self.extract = Extract(metadata_filepath=metadata_filepath, data_filepath=data_filepath, database=self.database, run_analysis=self.run_analysis, config=self.config)
        self.load = Load(database=self.database, config=self.config)
        self.transform = Transform(extract=self.extract, load=self.load, database=self.database, config=self.config)

    def run(self):
        if self.extract_data:
            self.extract.run()
        if self.transform_data:
            self.transform.run()
        if self.load_data:
            self.load.run()
