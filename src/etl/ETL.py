from src.database.Database import Database
from src.etl.Extract import Extract
from src.etl.Load import Load
from src.etl.Transform import Transform
from src.profiles.Hospital import Hospital
from src.utils.TableNames import TableNames
from src.utils.setup_logger import log
from random import randrange


class ETL:
    def __init__(self, hospital_name: str, metadata_filepath: str, samples_filepath: str, reset_db: bool):
        self.database = Database(connection_string="mongodb://localhost:27017/", database_name="better_database_subset")
        if reset_db:
            self.database.reset()

        # four_thousands_hospitals = []
        # for i in range(0, 3):
        #     four_thousands_hospitals.append(Hospital(str(i), "Buzzi " + str(randrange(10))).to_json())

        # log.debug(len(four_thousands_hospitals))
        # an_hospital = Hospital("6", "Buzzi 5")
        # an_hospital2 = Hospital("2", "BUZZI 2")
        # hospitals = [an_hospital, an_hospital2]
        # json_hospitals = [hospital.to_json() for hospital in hospitals]
        # log.debug("json_hospitals: %s", json_hospitals)
        # upserted_id = self.database.upsert_one_tuple(table_name=TableNames.HOSPITAL.value, filter_dict={"name": an_hospital.name}, one_tuple=an_hospital.to_json())
        # (upserted_document, is_insert) = self.database.upsert_one_tuple(table_name=TableNames.HOSPITAL.value, unique_variables=["name"], one_tuple=an_hospital2.to_json())
        # log.debug(upserted_document)
        # log.debug(is_insert)
        # upserted_id = self.database.upsert_one_tuple(table_name=TableNames.HOSPITAL.value, filter_dict={"name": an_hospital3.name}, one_tuple=an_hospital3.to_json())
        # self.database.upsert_many_tuples(table_name=TableNames.HOSPITAL.value, unique_variables=["name"], tuples=four_thousands_hospitals)
        # log.debug(nb_upserts)

        # flags to know what to do during the ETL process
        self.extract_data = True
        self.run_analysis = False
        self.transform_data = True
        self.load_data = True
        self.compute_plots = False

        self.extract = Extract(metadata_filepath=metadata_filepath, samples_filepath=samples_filepath, database=self.database, run_analysis=self.run_analysis)
        self.load = Load(database=self.database)
        self.transform = Transform(extract=self.extract, load=self.load, hospital_name=hospital_name, database=self.database)

    def run(self):
        if self.extract_data:
            self.extract.run()
        if self.transform_data:
            self.transform.run()
        if self.load_data:
            self.load.run()
