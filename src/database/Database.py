import copy
import os
import re

import bson
import pymongo
from bson.json_util import loads
from pymongo import MongoClient
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor

from database.Execution import Execution
from enums.TableNames import TableNames
from enums.UpsertPolicy import UpsertPolicy
from utils.setup_logger import log
from utils.utils import mongodb_match, mongodb_project_one, mongodb_group_by, \
    mongodb_sort, mongodb_limit


class Database:
    """
    The class Database represents the underlying MongoDB database: the connection, the database itself and
    auxiliary functions to make interactions with the database object (insert, select, ...).
    """

    SERVER_TIMEOUT = 5000

    def __init__(self, execution: Execution):
        """
        Initiate a new connection to a MongoDB client, reachable based on the given connection string, and initialize
        class members.
        """
        self._execution = execution
        self._client = None
        self._db = None

        # 1. connect to the Mongo client
        try:
            # mongodb://localhost:27017/
            # mongodb://127.0.0.1:27017/
            # mongodb+srv://<username>:<password>@<cluster>.qo5xs5j.mongodb.net/?retryWrites=true&w=majority&appName=<app_name>
            # Starting with version 3.0 the MongoClient constructor no longer blocks while connecting to the server or servers,
            # and it no longer raises pymongo (ConnectionFailure, ConfigurationError) errors.
            # Instead, the constructor returns immediately and launches the connection process on background threads.
            # You can check if the server is available with a ping.
            self._client = MongoClient(host=self._execution.db_connection, serverSelectionTimeoutMS=Database.SERVER_TIMEOUT)  # timeout after 5 sec instead of 20 (the default)
            log.debug(f"{self._execution.db_connection}")
            log.debug(f"{self._client}")
        except Exception:
            raise ConnectionError(f"Could not connect to the MongoDB client located at {self._execution.db_connection} and with a timeout of {Database.SERVER_TIMEOUT} ms.")

        # 2 check if the client is running well
        self.check_server_is_up()
        # if we reach this point, the MongoDB client runs correctly.
        log.info(f"The MongoDB client, located at {self._execution.db_connection}, could be accessed properly.")

        # 3. access the database
        log.info(f"drop db is: {self._execution.db_drop}")
        if self._execution.db_drop:
            self.drop_db()
        self._db = self._client[self._execution.db_name]

        log.debug(f"the connection string is: {self._execution.db_connection}")
        log.debug(f"the new MongoClient is: {self._client}")
        log.debug(f"the database is: {self._db}")

    def check_server_is_up(self) -> None:
        """
        Send a ping to confirm a successful connection.
        :return: A boolean being whether the MongoDB client is up.
        """
        try:
            self._client.admin.command('ping')
        except Exception:
            raise ConnectionError(f"The MongoDB client located at {self._execution.db_connection} could not be accessed properly.")

    def drop_db(self) -> None:
        """
        Drop the current database.
        :return: Nothing.
        """
        if self._execution.db_drop:
            log.info(f"WARNING: The database {self._execution.db_name} will be dropped!", )
            self._client.drop_database(name_or_database=self._execution.db_name)

    def close(self) -> None:
        self._client.close()

    def insert_one_tuple(self, table_name: str, one_tuple: dict) -> None:
        self._db[table_name].insert_one(one_tuple)

    def insert_many_tuples(self, table_name: str, tuples: list[dict] | tuple) -> None:
        """
        Insert the given tuples in the specified table.
        :param table_name: A string being the table name in which to insert the tuples.
        :param tuples: A list of dicts being the tuples to insert.
        """
        _ = self._db[table_name].insert_many(tuples, ordered=False)

    def create_update_stmt(self, the_tuple: dict):
        if self._execution.db_upsert_policy == UpsertPolicy.DO_NOTHING.value:
            # insert the document if it does not exist
            # otherwise, do nothing
            return {"$setOnInsert": the_tuple}
        else:
            # insert the document if it does not exist
            # otherwise, replace it
            return {"$set": the_tuple}

    def upsert_one_tuple(self, table_name: str, unique_variables: list[str], one_tuple: dict) -> None:
        # filter_dict should only contain the fields on which we want a Resource to be unique,
        # e.g., name for Hospital instances, ID for Patient instances,
        #       the combination of Patient, Hospital, Sample and Examination instances for ExaminationRecord instances
        #       see https://github.com/Nelly-Barret/BETTER-fairificator/issues/3
        # one_tuple contains the Resource itself (with all its fields; as a JSON dict)
        # use $setOnInsert instead of $set to not modify the existing tuple if it already exists in the DB
        filter_dict = {}
        for unique_variable in unique_variables:
            try:
                filter_dict[unique_variable] = one_tuple[unique_variable]
            except Exception:
                raise KeyError(f"The tuple does not contains the attribute '{unique_variable}, thus the upsert cannot refer to it.")
        update_stmt = self.create_update_stmt(the_tuple=one_tuple)
        self._db[table_name].find_one_and_update(filter=filter_dict, update=update_stmt, upsert=True)

    def upsert_one_batch_of_tuples(self, table_name: str, unique_variables: list[str], the_batch: list[dict]) -> dict:
        """

        :param unique_variables:
        :param table_name:
        :param the_batch:
        :return: The `filter_dict` to know exactly which fields have been used for the upsert (some of the given fields in unique_variables may not exist in some instances)
        """
        # in case there are more than 1000 tuples to upserts, we split them in batch of 1000
        # and send one bulk operation per batch. This allows to save time by not doing a db call per upsert.
        # we use the bulk operation to send sets of BATCH_SIZE operations, each operation doing an upsert
        # this allows to have only one call to the database for each bulk operation (instead of one per upsert operation)
        operations = []
        filter_dict = {}
        for one_tuple in the_batch:
            filter_dict = {}
            for unique_variable in unique_variables:
                if unique_variable in one_tuple:
                    # only BUZZI ExaminationRecord instances have a "based_on" attribute for the Samples
                    # others do not have the "based_on", so we need to check whether that attribute is present or not
                    filter_dict[unique_variable] = one_tuple[unique_variable]
            update_stmt = self.create_update_stmt(the_tuple=one_tuple)
            operations.append(pymongo.UpdateOne(filter=filter_dict, update=update_stmt, upsert=True))
        log.debug(f"Table {table_name}: sending a bulk write of {len(operations)} operations")
        # July 18th, 2024: bulk_write modifies the hospital lists in Transform (avan if I use deep copies everywhere)
        # It changes (only?) the timestamp value with +1/100, e.g., 2024-07-18T14:34:32Z becomes 2024-07-18T14:34:33Z
        # in the tests I use a delta to compare datetime
        result_upsert = self._db[table_name].bulk_write(copy.deepcopy(operations))
        log.info(f"In {table_name}, {result_upsert.inserted_count} inserted, {result_upsert.upserted_count} upserted, {result_upsert.modified_count} modified tuples")
        return filter_dict

    def retrieve_identifiers(self, table_name: str, projection: str) -> dict:
        # projection contains the field name to which we want to associate identifiers,
        # e.g., if we have { "identifier": "1", "name": "Alice" } and {"identifier": "2", "name": "Bob" }
        # we would obtain the following mapping: { "Alice": "1", "Bob": "2" }
        # this is used for now to associate each column name to its Examination id, and each hospital to its Hospital id
        projection_as_dict = {projection: 1, "identifier": 1}
        cursor = self.find_operation(table_name=table_name, filter_dict={}, projection=projection_as_dict)
        mapping = {}
        for result in cursor:
            projected_value = result
            for key in projection.split("."):
                # this covers the case when the project is a nested key, e.g., code.text
                projected_value = projected_value[key]
            mapping[projected_value] = result["identifier"]["value"]
        log.debug(f"{mapping}")
        return mapping

    def load_json_in_table(self, table_name: str, unique_variables) -> None:
        log.info(f"insert data in {table_name}")
        for filename in os.listdir(self._execution.working_dir_current):
            if re.search(table_name+"[0-9]+", filename) is not None:
                # implementation note: we cannot simply use filename.startswith(table_name)
                # because both Examination and ExaminationRecord start with Examination
                # the solution is to use a regex
                with open(os.path.join(self._execution.working_dir_current, filename), "r") as json_datafile:
                    tuples = bson.json_util.loads(json_datafile.read())
                    log.debug(f"Table {table_name}, file {filename}, loading {len(tuples)} tuples")
                    _ = self.upsert_one_batch_of_tuples(table_name=table_name,
                                                        unique_variables=unique_variables,
                                                        the_batch=tuples)

    def find_operation(self, table_name: str, filter_dict: dict, projection: dict) -> Cursor:
        """
        Perform a find operation (SELECT * FROM x WHERE filter_dict) in a given table.
        :param table_name: A string being the table name in which the find operation is performed.
        :param filter_dict: A dict being the set of filters (conditions) to apply on the data in the given table.
        :param projection: A dict being the set of projections (selections) to apply on the data in the given table.
        :return: A Cursor on the results, i.e., filtered data.
        """
        return self._db[table_name].find(filter_dict, projection)

    def count_documents(self, table_name: str, filter_dict: dict) -> int:
        """
        Count the number of documents in a table and matching a given filter.
        :param table_name: A string being the table name in which the count operation is performed.
        :param filter_dict: A dict being the set of filters to be applied on the documents.
        :return: An integer being the number of documents matched by the given filter.
        """
        return self._db[table_name].count_documents(filter_dict)

    def create_unique_index(self, table_name: str, columns: dict) -> None:
        """
        Create a unique constraint/index on a (set of) column(s).
        :param table_name: A string being the table name on which the index will be created.
        :param columns: A dict being the set of columns to be included in the index. It may contain only one entry if
        only one column should be unique. The parameter should be of the form { "colA": 1, ... }.
        :return: Nothing.
        """
        self._db[table_name].create_index(columns, unique=True)

    def create_non_unique_index(self, table_name: str, columns: dict) -> None:
        """
        Create an index on a (set of) column(s) for which uniqueness is not guaranteed.
        :param table_name: A string being the table name on which the index will be created.
        :param columns: A dict being the set of columns to be included in the index. It may contain only one entry if
        only one column should be unique. The parameter should be of the form { "colA": 1, ... }.
        :return: Nothing.
        """
        self._db[table_name].create_index(columns, unique=False)

    def get_min_or_max_value(self, table_name: str, field: str, sort_order: int) -> int | float:
        operations = []
        field_split = field.split(".")
        last_field = field_split[-1]
        if "." in field:
            # this field is a nested one, we only keep the deepest one,
            # e.g. for { "identifier": {"value": 1}} we keep { "value": 1}
            operations.append(mongodb_project_one(field=field, projected_value=last_field))
            operations.append(mongodb_sort(field=last_field, sort_order=sort_order))
            operations.append(mongodb_limit(1))
        else:
            operations.append(mongodb_project_one(field=field, projected_value=None))
            operations.append(mongodb_sort(field=field, sort_order=sort_order))
            operations.append(mongodb_limit(1))

        cursor = self._db[table_name].aggregate(operations)

        for result in cursor:
            # There should be only one result, so we can return directly the min or max value
            if "." in field:
                return result[last_field]
            else:
                return result[field]

    def get_max_value(self, table_name: str, field: str) -> int | float:
        return self.get_min_or_max_value(table_name=table_name, field=field, sort_order=-1)

    def get_min_value(self, table_name: str, field: str) -> int | float:
        return self.get_min_or_max_value(table_name=table_name, field=field, sort_order=1)

    def get_avg_value_of_examination_record(self, examination_url: str) -> int | float:
        """
        Compute the average value among all the examination records for a certain examination.
        :param examination_url: A string being the examination url of the form Examination/X, where X is the
        Examination number, and for which the avg value will be computed among the examination records referring
        to that examination url.
        :return: A float value being the average value for the given examination url.
        """
        cursor = self._db[TableNames.LABORATORY_RECORD.value].aggregate([
            mongodb_match(field="instantiate.reference", value=examination_url, is_regex=False),
            mongodb_project_one(field="value", projected_value=None),
            mongodb_group_by(group_key=None, group_by_name="avg_val", operator="$avg", field="$value")
        ])

        for result in cursor:
            return float(result)  # There should be only one result, so we can return directly the min or max value

    def get_value_distribution_of_examination(self, examination_url: str, min_value: float) -> CommandCursor:
        """
        Compute the value distribution among all the examination records for a certain examination.
        :param examination_url: A string being the examination url of the form Examination/X, where X is the
        Examination number, and for which the value distribution will be computed among the examination records
        referring to that examination url.
        :param min_value: A float value being the minimum frequency that an element should have to be part of the plot.
        :return: A CommandCursor to iterate over the value distribution of the form { "value": frequency, ... }
        """
        pipeline = [
            mongodb_match(field="instantiate.reference", value=examination_url, is_regex=False),
            mongodb_project_one(field="value", projected_value=None),
            mongodb_group_by(group_key="$value", group_by_name="total", operator="$sum", field=1),
            mongodb_match(field="total", value={"$gt": min_value}, is_regex=False),
            mongodb_sort(field="_id", sort_order=1)
        ]
        # .collation({"locale": "en_US", "numericOrdering": "true"})
        return self._db[TableNames.LABORATORY_RECORD.value].aggregate(pipeline)

    def get_max_resource_counter_id(self) -> int:
        max_value = -1
        for table_name in TableNames:
            if table_name.value == TableNames.PATIENT.value or table_name.value == TableNames.SAMPLE.value:
                # pass because Patient and Sample resources have their ID assigned by hospitals, not the FAIRificator
                pass
            else:
                current_max_identifier = self.get_max_value(table_name=table_name.value, field="identifier.value")
                if current_max_identifier is not None:
                    try:
                        current_max_identifier = int(current_max_identifier)
                        if current_max_identifier > max_value:
                            max_value = current_max_identifier
                    except ValueError:
                        # this identifier is not an integer, e.g., a Sample ID like 24DL54
                        # we simply ignore it and try to find the next maximum integer ID
                        pass
                else:
                    # the table is not created yet (this happens when we start from a fresh new DB, thus we skip this)
                    pass
        return max_value

    def __str__(self) -> str:
        return "Database " + self._execution.db_name

    @property
    def db(self):
        # TODO Nelly: missing hint for return
        return self._db

    def db_exists(self, db_name: str) -> bool:
        list_dbs = self._client.list_databases()
        for db in list_dbs:
            if db['name'] == db_name:
                return True
        return False
