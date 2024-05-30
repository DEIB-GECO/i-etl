from src.database.Database import Database
from src.etl.Extract import Extract
from src.etl.Load import Load
from src.etl.Transform import Transform


class ETL:
    def __init__(self, hospital_name: str, metadata_filepath: str, samples_filepath: str, reset_db: bool):
        self.database = Database(connection_string="mongodb://localhost:27017/", database_name="better_database_subset")
        if reset_db:
            self.database.reset()

        # flags to know what to do during the ETL process
        self.create_structures = True
        self.extract_data = True
        self.run_analysis = True
        self.transform_data = True
        self.load_data = True
        self.compute_plots = False

        self.extract = Extract(metadata_filepath=metadata_filepath, samples_filepath=samples_filepath, database=self.database, run_analysis=self.run_analysis)
        self.transform = Transform(extract=self.extract, hospital_name=hospital_name, database=self.database)
        self.load = Load(extract=self.extract, transform=self.transform, database=self.database)

    def run(self):
        if self.extract_data:
            self.extract.run()
        if self.transform_data:
            self.transform.run()
        if self.load_data:
            self.load.run()
