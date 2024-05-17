from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.collection import Collection
import csv
import json

from src.utils.setup_logger import log
from src.utils.utils import EXAMINATION_RECORD_TABLE_NAME


class Database:
    def __init__(self, connection_string="mongodb://localhost:27017/", database_name="my_db"):
        assert type(connection_string) is str

        # self.connection_string = "mongodb+srv://"+username+":"+password+"@"+cluster+".qo5xs5j.mongodb.net/?retryWrites=true&w=majority&appName="+app_name
        self.database_name = database_name
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

    def reset(self) -> None:
        self.client.drop_database(self.database_name)

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
        if len(tuples) == 0:
            log.error("An insert operation has been request but not data was provided.")
            pass
        else:
            return self.db[table_name].insert_many(tuples, ordered=False).inserted_ids

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

    def create_unique_index(self, table_name: str, columns: dict):
        log.debug(self.db[table_name])
        self.db[table_name].create_index(columns, unique=True)

    def get_min_value_of_examination_record(self, examination_url: str):
        return self.__get_min_max_value_of_examination_record(EXAMINATION_RECORD_TABLE_NAME, examination_url, "min")

    def get_max_value_of_examination_record(self, examination_url: str):
        return self.__get_min_max_value_of_examination_record(EXAMINATION_RECORD_TABLE_NAME, examination_url, "max")

    def __get_min_max_value_of_examination_record(self, table_name: str, examination_url: str, min_or_max: str):
        if min_or_max == "min":
            sort_order = 1
        elif min_or_max == "max":
            sort_order = -1
        else:
            log.warn("You asked for something else than min or max. This will be min by default.")

        return self.db[table_name].aggregate([
            {
                "$match": {
                    "instantiate.reference": examination_url
                }
            },
            {
                "$project": {
                    "value": 1
                }
            },
            {
                "$sort": {
                    "value": sort_order
                }
            }, {
                "$limit": 1  # sort in ascending order and return the first value: this is the min
            }
        ])

    def __get_avg_value_of_examination_record(self, table_name: str, examination_url: str):
        return self.db[table_name].aggregate([
            {
                "$match": {
                    "instantiate.reference": examination_url
                }
            },
            {
                "$project": {
                    "value": 1
                }
            },
            {
                "$group": {
                    "_id": None,
                    "avg_val": {
                        "$avg": "$value"  # $avg: $<the field on which the avg is computed>
                    }
                }
            }
        ])

    def get_value_distribution_of_examination(self, table_name: str, examination_url: str, limit: int, min_value: int):
        pipeline = [
            {
                "$match": {
                    "instantiate.reference": examination_url
                }
            },
            {
                "$project": {
                    "value": 1
                }
            }, {
                "$group": {
                    "_id": "$value",
                    "total": {
                        "$sum": 1
                    }
                }
            }, {
                "$match": {
                    "total": {
                        "$gt": min_value
                    }
                }
            }, {
                "$sort": {
                    "_id": 1,
                }
            }
        ]
        log.debug(pipeline)
        return self.db[table_name].aggregate(pipeline) #.collation({"locale": "en_US", "numericOrdering": "true"})

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
