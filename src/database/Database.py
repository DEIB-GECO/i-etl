from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.collection import Collection
import csv
import json

from src.utils.Utils import decorate_all_functions, check_types_before_func, check_types
from src.utils.setup_logger import log


#@decorate_all_functions(check_types_before_func)
class Database:
    def __init__(self, connection_string="mongodb://localhost:27017/", database_name="my_db"):
        assert type(connection_string) is str

        # self.connection_string = "mongodb+srv://"+username+":"+password+"@"+cluster+".qo5xs5j.mongodb.net/?retryWrites=true&w=majority&appName="+app_name
        self.connection_string = connection_string
        self.client = MongoClient(self.connection_string)  #, server_api=ServerApi(server_api))
        self.db = self.client[database_name]

        log.debug("the connection string is: %s", self.connection_string)
        log.debug("the new MongoClient is: %s", self.client)
        log.debug("the database is: %s", self.db)

        if self.__check_server_is_up():
            log.info("The connection is up.")
        else:
            log.error("There was a problem while connecting to the MongoDB instance at %s.", self.connection_string)
            exit()

    def __check_server_is_up(self) -> bool:
        # Send a ping to confirm a successful connection
        try:
            self.client.admin.command('ping')
            return True
        except Exception as e:
            log.error(e)
            return False

    def get_table(self, table_name: str) -> Collection:
        assert type(table_name) is str

        res = self.db[table_name]
        assert type(res) is Collection
        return res

    def create_table(self, table_name: str):
        assert type(table_name) is str
        self.db[table_name]

    def insert_one_tuple(self, table_name, one_tuple) -> int:
        # check_types((type(table_name), str), (type(one_tuple), dict))

        log.debug(type(one_tuple))

        return self.db[table_name].insert_one(one_tuple).inserted_id

    def insert_many_tuples(self, table_name: str, tuples: list[dict]) -> list[int]:
        return self.db[table_name].insert_many(tuples).inserted_ids

    def insert_tuples_from_csv(self, table_name: str, csv_path: str, delimiter: str, quotechar: str) -> None:
        log.debug("table_name is: %s", table_name)
        log.debug("csv_path is: %s", csv_path)
        with open(csv_path, newline='') as csvfile:
            # TODO Nelly: check if we can force DictReader to write double quoted json
            csv_reader = csv.DictReader(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            data = [row for row in csv_reader]
            log.debug(type(data))
            double_quoted_string_data = json.dumps(data)
            log.debug(type(double_quoted_string_data))
            double_quoted_data = json.loads(double_quoted_string_data)
            log.debug(type(double_quoted_data))

        # self.insert_many_tuples(table_name, double_quoted_data)

    def find_operation(self, table_name: str, filter_dict: dict):
        assert type(table_name) == str

        log.debug("table name is: %s", table_name)
        log.debug("filter_dict is: %s", filter_dict)
        return self.db[table_name].find(filter_dict)

    def count_documents(self, table_name: str, filter_dict: dict):
        assert type(table_name) == str
        assert type(filter_dict) == dict

        log.debug("table_name is: %s", table_name)
        log.debug("filter_dict is: %s", filter_dict)
        return self.db[table_name].count_documents(filter_dict)
    def __str__(self) -> str:
        return "Database " + self.connection_string

    def test_insert_patient(self):
        return True
    # mongodb+srv://nellybarret:aeyuZpyUOJGI0PpS@better-cluster.qo5xs5j.mongodb.net/?retryWrites=true&w=majority&appName=BETTER-application
    # mongodb+srv://nellybarret:<password>@better-cluster.qo5xs5j.mongodb.net/?retryWrites=true&w=majority&appName=BETTER-application

    # create a table, called a collection in the MongoDB language
    # patients_table = database.get_table("patients")

    # database.insert_tuples_from_csv("patients2", "../data/buzzi_subset_small_quoted.csv", ',', '')

    # create a first patient and fill its data
    # first_patient = { "first-name": "Bob", "last-name": "Doe", "condition": "breast cancer" }

    # log.debug("my first patient: ")
    # log.debug(first_patient)

    # actually insert the first patient in the database
    # this will also create the collection (as soon as the first tuple is inserted)
    # first_patient_id = patients_table.insert_one(first_patient).inserted_id

    # log.debug("First patient has been inserted. Its ID is: %d", first_patient_id)

    # # get one tuple of the patients table
    # log.debug(patients_table.find_one())

    # # print the first patient having for first-name "Alice"
    # log.debug(patients_table.find_one({"first-name": "Alice"}))

    def get_db(self):
        return self.db
