import json
import os
import re
import traceback

import bson
from bson.json_util import loads
import pymongo
from pymongo import MongoClient
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor

from src.config.BetterConfig import BetterConfig
from src.utils.TableNames import TableNames
from src.utils.utils import mongodb_project_one, mongodb_group_by, mongodb_match, mongodb_sort, \
    mongodb_max, mongodb_unwind, mongodb_min
from src.utils.constants import BATCH_SIZE
from src.utils.setup_logger import log
from src.utils.UpsertPolicy import UpsertPolicy


class Database:
    """
    The class Database represents the underlying MongoDB database: the connection, the database itself and
    auxiliary functions to make interactions with the database object (insert, select, ...).
    """

    def __init__(self, config: BetterConfig):
        """
        Initiate a new connection to a MongoDB client, reachable based on the given connection string, and initialize
        class members.
        """
        self.config = config

        # mongodb://localhost:27017/
        # mongodb://127.0.0.1:27017/
        # mongodb+srv://<username>:<password>@<cluster>.qo5xs5j.mongodb.net/?retryWrites=true&w=majority&appName=<app_name>
        self.config = config
        self.client = MongoClient(host=self.config.get_db_connection(),
                                  serverSelectionTimeoutMS=5000)  # timeout after 5 sec instead of 20 (the default)
        if config.get_db_drop():
            self.drop_db()
        self.db = self.client[self.config.get_db_name()]

        log.debug("the connection string is: %s", self.config.get_db_connection())
        log.debug("the new MongoClient is: %s", self.client)
        log.debug("the database is: %s", self.db)

    def check_server_is_up(self) -> bool:
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

    def drop_db(self) -> None:
        """
        Drop the current database.
        :return: Nothing.
        """
        log.info("WARNING: The database %s will be dropped!", self.config.get_db_name())
        self.client.drop_database(name_or_database=self.config.get_db_name())

    def close(self) -> None:
        self.client.close()

    def insert_one_tuple(self, table_name: str, one_tuple: dict) -> None:
        self.db[table_name].insert_one(one_tuple)

    def insert_many_tuples(self, table_name: str, tuples: list[dict]) -> None:
        """
        Insert the given tuples in the specified table.
        :param table_name: A string being the table name in which to insert the tuples.
        :param tuples: A list of dicts being the tuples to insert.
        """
        self.db[table_name].insert_many(tuples, ordered=False)

    def upsert_one_tuple(self, table_name: str, unique_variables: list[str], one_tuple: dict) -> None:
        # filter_dict should only contain the fields on which we want a Resource to be unique,
        # e.g., name for Hospital instances, ID for Patient instances,
        #       the combination of Patient, Hospital, Sample and Examination instances for ExaminationRecord instances
        #       see https://github.com/Nelly-Barret/BETTER-fairificator/issues/3
        # one_tuple contains the Resource itself (with all its fields; as a JSON dict)
        # use $setOnInsert instead of $set to not modify the existing tuple if it already exists in the DB
        filter_dict = {}
        for unique_variable in unique_variables:
            filter_dict[unique_variable] = one_tuple[unique_variable]
        # return_document:
        # If ReturnDocument.BEFORE (the default), returns the original document before it was replaced, or None if no document matches.
        # If ReturnDocument.AFTER, returns the replaced or inserted document.
        # We choose: AFTER so that
        # (i) when the instance already exists in the DB, we get the resource after its update (but the update does nothing with the help of $setOnInsert)
        # (ii) when the instance does not exist yet, we insert it and the returned value is the inserted document
        # as in both (i) and (ii) we get a Document, we need to use a timestamp in order to know whether this was an update or an insert
        if UpsertPolicy.DO_NOTHING:
            # insert the document if it does not exist
            # otherwise, do nothing
            update_stmt = {"$setOnInsert": one_tuple}
        else:
            # insert the document if it does not exist
            # otherwise, replace it
            update_stmt = {"$set": one_tuple}
        self.db[table_name].find_one_and_update(filter=filter_dict, update=update_stmt, upsert=True)

    def upsert_batch_of_tuples(self, table_name: str, unique_variables: list[str], tuples: list[dict]) -> None:
        """

        :param unique_variables:
        :param table_name:
        :param tuples:
        :return: An integer being the number of upserted tuples.
        """
        # in case there are more than 1000 tuples to upserts, we split them in batch of 1000
        # and send one bulk operation per batch. This allows to save time by not doing a db call per upsert
        # but do not overload the MongoDB with thousands of upserts.
        # we use the bulk operation to send sets of BATCH_SIZE operations, each doing an upsert
        # this allows to have only on call to the database for each bulk operation (instead of one per upsert operation)
        operations = []
        for one_tuple in tuples:
            filter_dict = {}
            for unique_variable in unique_variables:
                filter_dict[unique_variable] = one_tuple[unique_variable]
            update_stmt = {"$setOnInsert": one_tuple}
            operations.append(pymongo.UpdateOne(filter=filter_dict, update=update_stmt, upsert=True))
        log.debug("Table %s: sending a bulk write of %s operations", table_name, len(operations))
        result_upsert = self.db[table_name].bulk_write(operations)
        log.info("In %s, %s inserted, %s upserted, %s modified tuples", table_name, result_upsert.inserted_count, result_upsert.upserted_count, result_upsert.modified_count)

    def compute_batches(self, tuples: list[dict]) -> list[list[dict]]:
        batch_tuples = []
        if len(tuples) <= BATCH_SIZE:
            batch_tuples.append(tuples)
        else:
            # +1 to take the elements remaining after X thousands,
            # e.g., in 4321, we need one more iteration to gt the 321 remaining elements in a batch
            nb_batch = (len(tuples) // BATCH_SIZE) + 1
            for i in range(0, nb_batch):
                left_index = i * BATCH_SIZE
                right_index = (i + 1) * BATCH_SIZE
                batch = tuples[left_index: right_index]
                batch_tuples.append(batch)
        return batch_tuples

    def retrieve_identifiers(self, table_name: str, projection: str) -> dict:
        projection_as_dict = {projection: 1, "identifier": 1}
        cursor = self.find_operation(table_name=table_name, filter_dict={}, projection=projection_as_dict)
        mapping = {}
        for result in cursor:
            projected_value = result
            for key in projection.split("."):
                # this covers the case when the project is a nested key, e.g., code.text
                projected_value = projected_value[key]
            mapping[projected_value] = result["identifier"]
        log.debug(mapping)
        return mapping

    def write_in_file(self, data_array: list, table_name: str, count: int) -> None:
        if len(data_array) > 0:
            filename = os.path.join(self.config.get_working_dir_current(), table_name + str(count) + ".json")
            with open(filename, "w") as data_file:
                try:
                    json.dump([resource.to_json() for resource in data_array], data_file)
                except Exception:
                    traceback.print_exc()
                    log.error("The %s instances could not be converted to JSON. Stopping here.", table_name)
                    exit()
        else:
            log.info("No data when writing file %s/%s", table_name, count)

    def load_json_in_table(self, table_name: str, unique_variables) -> None:
        log.info("insert data in %s", table_name)
        for filename in os.listdir(self.config.get_working_dir_current()):
            if re.search(table_name+"[0-9]+", filename) is not None:
                # implementation note: we cannot simply use filename.startswith(table_name)
                # because both Examination and ExaminationRecord start with Examination
                # the solution is to use a regex
                with open(os.path.join(self.config.get_working_dir_current(), filename), "r") as json_datafile:
                    tuples = bson.json_util.loads(json_datafile.read())
                    log.info(tuples)
                    log.debug("Table %s, file %s, loading %s tuples", table_name, filename, len(tuples))
                    self.upsert_batch_of_tuples(table_name=table_name,
                                                         unique_variables=unique_variables,
                                                         tuples=tuples)

    def find_operation(self, table_name: str, filter_dict: dict, projection: dict) -> Cursor:
        """
        Perform a find operation (SELECT * FROM x WHERE filter_dict) in a given table.
        :param table_name: A string being the table name in which the find operation is performed.
        :param filter_dict: A dict being the set of filters (conditions) to apply on the data in the given table.
        :param projection: A dict being the set of projections (selections) to apply on the data in the given table.
        :return: A Cursor on the results, i.e., filtered data.
        """
        return self.db[table_name].find(filter_dict, projection)

    def count_documents(self, table_name: str, filter_dict: dict) -> int:
        """
        Count the number of documents in a table and matching a given filter.
        :param table_name: A string being the table name in which the count operation is performed.
        :param filter_dict: A dict being the set of filters to be applied on the documents.
        :return: An integer being the number of documents matched by the given filter.
        """
        return self.db[table_name].count_documents(filter_dict)

    def create_unique_index(self, table_name: str, columns: dict) -> None:
        """
        Create a unique constraint/index on a (set of) column(s).
        :param table_name: A string being the table name on which the index will be created.
        :param columns: A dict being the set of columns to be included in the index. It may contain only one entry if
        only one column should be unique. The parameter should be of the form { "colA": 1, ... }.
        :return: Nothing.
        """
        self.db[table_name].create_index(columns, unique=True)

    def get_min_or_max_value(self, table_name: str, field: str, sort_order: int) -> int | float:
        operations = [
            mongodb_project_one(field=field, split_delimiter="/"),
            mongodb_unwind(field=field),
            mongodb_match(field=field, value="[0-9]+", is_regex=True)
        ]

        if sort_order == 1:
            operations.append(mongodb_min(field=field))
        else:
            operations.append(mongodb_max(field=field))

        cursor = self.db[table_name].aggregate(operations)

        for result in cursor:
            log.debug(result)
            # There should be only one result, so we can return directly the min or max value
            if sort_order == 1:
                return result["min"]
            else:
                return result["max"]

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
        cursor = self.db[TableNames.EXAMINATION_RECORD.value].aggregate([
            mongodb_match(field="instantiate.reference", value=examination_url, is_regex=False),
            mongodb_project_one(field="value", split_delimiter=""),
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
            mongodb_project_one(field="value", split_delimiter=""),
            mongodb_group_by(group_key="$value", group_by_name="total", operator="$sum", field=1),
            mongodb_match(field="total", value={"$gt": min_value}, is_regex=False),
            mongodb_sort(field="_id", sort_order=1)
        ]
        # .collation({"locale": "en_US", "numericOrdering": "true"})
        return self.db[TableNames.EXAMINATION_RECORD.value].aggregate(pipeline)

    def __str__(self) -> str:
        return "Database " + self.config.get_db_name()

    def get_db(self):
        # TODO Nelly: missing hint for return
        return self.db
