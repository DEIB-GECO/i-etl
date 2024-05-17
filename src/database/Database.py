import csv
import json

from pymongo.collection import Collection
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor
from pymongo.mongo_client import MongoClient

from src.utils.setup_logger import log
from src.utils.utils import EXAMINATION_RECORD_TABLE_NAME, assert_type, assert_not_none, assert_variable, mongodb_match, \
    mongodb_project_one, mongodb_sort, mongodb_limit, mongodb_group_by


class Database:
    """
    The class Database represent the underlying MongoDB database, i.e., the connection, the database itself and
    auxiliary functions to make interactions with the database object easier from elsewhere in the code.
    """
    def __init__(self, connection_string, database_name):
        """
        Initiate a new connection to a MongoDB client, reachable based on the given connection string, and initialize
        class members.
        :param connection_string: A string being the complete URI to connect to the MongoDB client.
        :param database_name: A string being the MongoDB database name.
        """
        assert_variable(variable=connection_string, expected_type=str, variable_name="connection_string")
        assert_variable(variable=database_name, expected_type=str, variable_name="database_name")

        # mongodb://localhost:27017/
        # mongodb+srv://<username>:<password>@<cluster>.qo5xs5j.mongodb.net/?retryWrites=true&w=majority&appName=<app_name>
        self.database_name = database_name
        self.connection_string = connection_string
        self.client = MongoClient(host=self.connection_string)
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
        """
        Send a ping to confirm a successful connection.
        :return: A boolean being whether the MongoDB client is up.
        """
        #
        try:
            self.client.admin.command('ping')
            return True
        except Exception as e:
            log.error(e)
            return False

    def reset(self) -> None:
        """
        Truncate the current database.
        :return: Nothing.
        """
        self.client.drop_database(name_or_database=self.database_name)

    def insert_one_tuple(self, table_name: str, one_tuple: dict) -> int:
        """
        Insert the given tuple in the specified table.
        :param table_name: A string being the table name in which insert the tuple.
        :param one_tuple: A dict being the tuple to insert.
        :return: An integer being the MongoDB _id of the inserted tuple.
        """
        assert_variable(variable=table_name, expected_type=str, variable_name="table_name")
        assert_variable(variable=one_tuple, expected_type=dict, variable_name="one_tuple")

        return self.db[table_name].insert_one(one_tuple).inserted_id

    def insert_many_tuples(self, table_name: str, tuples: list[dict]) -> list[int]:
        """
        Insert the given tuples in the specified table.
        :param table_name: A string being the table name in which to insert the tuples.
        :param tuples: A list of dicts being the tuples to insert.
        :return: A list of integers being the MongoDB _id of the inserted tuples.
        """
        assert_variable(variable=table_name, expected_type=str, variable_name="table_name")
        assert_variable(variable=tuples, expected_type=list[dict], variable_name="tuples")

        return self.db[table_name].insert_many(tuples, ordered=False).inserted_ids

    def insert_tuples_from_csv(self, table_name: str, csv_path: str, delimiter: str, quote_char: str) -> list[int]:
        """
        Inert data from a CSV file in the given table, i.e., each line corresponds to a tuple, each pair of a
        column name and a value corresponds to a key-value pair in the inserted objects.
        :param table_name: A string being the table name in which data will be inserted.
        :param csv_path: A string being the file path to the CSV file containing the data.
        :param delimiter: A string being the CSV delimiter used in the CSV dataset, often ','.
        :param quote_char: A string being the CSV quote char used in the CSV dataset, often '"'.
        :return: A list of integers being the MongoDB _id of the inserted tuples.
        """
        assert_variable(variable=table_name, expected_type=str, variable_name="table_name")
        assert_variable(variable=csv_path, expected_type=str, variable_name="csv_path")
        assert_variable(variable=delimiter, expected_type=str, variable_name="delimiter")
        assert_variable(variable=quote_char, expected_type=str, variable_name="quote_char")

        log.debug("table_name is: %s", table_name)
        log.debug("csv_path is: %s", csv_path)
        with open(csv_path, newline='') as csvfile:
            # TODO Nelly: check if we can force DictReader to write double quoted json
            csv_reader = csv.DictReader(csvfile, delimiter=delimiter, quotechar=quote_char, quoting=csv.QUOTE_ALL)
            data = [row for row in csv_reader]
            log.debug(type(data))
            double_quoted_string_data = json.dumps(data)
            log.debug(type(double_quoted_string_data))
            double_quoted_data = json.loads(double_quoted_string_data)
            log.debug(type(double_quoted_data))

        return self.insert_many_tuples(table_name, double_quoted_data)

    def find_operation(self, table_name: str, filter_dict: dict) -> Cursor:
        """
        Perform a find operation (SELECT * FROM x WHERE filter_dict) in a given table.
        :param table_name: A string being the table name in which the find operation is performed.
        :param filter_dict: A dict being the set of filters (conditions) to apply on the data in the given table.
        :return: A Cursor on the results, i.e., filtered data.
        """
        assert_variable(variable=table_name, expected_type=str, variable_name="table_name")

        log.debug("table name is: %s", table_name)
        log.debug("filter_dict is: %s", filter_dict)
        return self.db[table_name].find(filter_dict)

    def count_documents(self, table_name: str, filter_dict: dict) -> int:
        """
        Count the number of documents in a table and matching a given filter.
        :param table_name: A string being the table name in which the count operation is performed.
        :param filter_dict: A dict being the set of filters to be applied on the documents.
        :return: An integer being the number of documents matched by the given filter.
        """
        assert_variable(variable=table_name, expected_type=str, variable_name="table_name")
        assert_variable(variable=filter_dict, expected_type=dict, variable_name="filter_dict")

        log.debug("table_name is: %s", table_name)
        log.debug("filter_dict is: %s", filter_dict)
        return self.db[table_name].count_documents(filter_dict)

    def create_unique_index(self, table_name: str, columns: dict) -> None:
        """
        Create a unique constraint/index on a (set of) column(s).
        :param table_name: A string being the table name on which the index will be created.
        :param columns: A dict being the set of columns to be included in the index. It may contain only one entry if
        only one column should be unique. The parameter should be of the form { "colA": 1, ... }.
        :return: Nothing.
        """
        assert_variable(variable=table_name, expected_type=str, variable_name="table_name")
        assert_variable(variable=columns, expected_type=dict, variable_name="columns")

        log.debug(self.db[table_name])
        self.db[table_name].create_index(columns, unique=True)

    def get_min_value_of_examination_record(self, examination_url: str) -> float:
        """
        Compute the minimum value among all the examination records for a certain examination.
        :param examination_url: A string being the examination url of the form Examination/X, where X is the
        Examination number, and for which the minimum value will be computed among the examination records referring
        to that examination url.
        :return: A float value being the minimum value for the given examination url.
        """
        assert_variable(variable=examination_url, expected_type=str, variable_name="examination_url")

        return self.__get_min_max_value_of_examination_record(examination_url=examination_url, min_or_max="min")

    def get_max_value_of_examination_record(self, examination_url: str) -> float:
        """
        Compute the maximum value among all the examination records for a certain examination.
        :param examination_url: A string being the examination url of the form Examination/X, where X is the
        Examination number, and for which the maximum value will be computed among the examination records referring
        to that examination url.
        :return: A float value being the maximum value for the given examination url.
        """
        assert_variable(variable=examination_url, expected_type=str, variable_name="examination_url")

        return self.__get_min_max_value_of_examination_record(examination_url=examination_url, min_or_max="max")

    def __get_min_max_value_of_examination_record(self, examination_url: str, min_or_max: str) -> float:
        """
        Compute the maximum or the maximum value among all the examination records for a certain examination.
        :param examination_url: A string being the examination url of the form Examination/X, where X is the
        Examination number, and for which the min or max value will be computed among the examination records referring
        to that examination url.
        :param min_or_max: A string being "min" or "max", depending on which value is needed.
        :return: A float value being the minimum or maximum value for the given examination url.
        """
        assert_variable(variable=examination_url, expected_type=str, variable_name="examination_url")
        assert_variable(variable=min_or_max, expected_type=str, variable_name="min_or_max")

        if min_or_max == "min":
            sort_order = 1
        elif min_or_max == "max":
            sort_order = -1
        else:
            sort_order = 1
            log.warn("You asked for something else than min or max. This will be min by default.")

        cursor = self.db[EXAMINATION_RECORD_TABLE_NAME].aggregate([
            mongodb_match(field="instantiate.reference", value=examination_url),
            mongodb_project_one(field="value"),
            mongodb_sort(field="value", sort_order=sort_order),
            mongodb_limit(nb=1)  # sort in ascending order and return the first value: this is the min
        ])

        for result in cursor:
            return float(result)  # There should be only one result, so we can return directly the min or max value

    def __get_avg_value_of_examination_record(self, examination_url: str):
        """
        Compute the average value among all the examination records for a certain examination.
        :param examination_url: A string being the examination url of the form Examination/X, where X is the
        Examination number, and for which the avg value will be computed among the examination records referring
        to that examination url.
        :return: A float value being the average value for the given examination url.
        """
        assert_variable(variable=examination_url, expected_type=str, variable_name="examination_url")

        return self.db[EXAMINATION_RECORD_TABLE_NAME].aggregate([
            mongodb_match(field="instantiate.reference", value=examination_url),
            mongodb_project_one(field="value"),
            mongodb_group_by(group_key=None, group_by_name="avg_val", operator="$avg", field="$value")
        ])

    def get_value_distribution_of_examination(self, examination_url: str, min_value: float) -> CommandCursor:
        """
        Compute the value distribution among all the examination records for a certain examination.
        :param examination_url: A string being the examination url of the form Examination/X, where X is the
        Examination number, and for which the value distribution will be computed among the examination records
        referring to that examination url.
        :param min_value: A float value being the minimum frequency that an element should have to be part of the plot.
        :return: A CommandCursor to iterate over the value distribution of the form { "value": frequency, ... }
        """
        assert_variable(variable=examination_url, expected_type=str, variable_name="examination_url")
        assert_variable(variable=min_value, expected_type=float, variable_name="min_value")

        pipeline = [
            mongodb_match(fild="instantiate.reference", value=examination_url),
            mongodb_project_one(field="value"),
            mongodb_group_by(group_key="$value", group_by_name="total", operator="$sum", field=1),
            mongodb_match(field="total", value={"$gt": min_value}),
            mongodb_sort(field="_id", sort_order=1)
        ]
        log.debug(pipeline)
        return self.db[EXAMINATION_RECORD_TABLE_NAME].aggregate(pipeline) #.collation({"locale": "en_US", "numericOrdering": "true"})

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
