from datetime import datetime, timedelta
from random import randrange

import pymongo
from bson import ObjectId
from pymongo import MongoClient, ReturnDocument
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor

from src.profiles import Disease, DiseaseRecord
from src.profiles.Examination import Examination
from src.profiles.ExaminationRecord import ExaminationRecord
from src.profiles.Hospital import Hospital
from src.profiles.Patient import Patient
from src.profiles.Sample import Sample
from src.utils.TableNames import TableNames
from src.utils.constants import BATCH_SIZE
from src.utils.setup_logger import log
from src.utils.utils import mongodb_match, mongodb_project_one, mongodb_sort, mongodb_limit, mongodb_group_by


class Database:
    """
    The class Database represents the underlying MongoDB database: the connection, the database itself and
    auxiliary functions to make interactions with the database object (insert, select, ...).
    """
    def __init__(self, connection_string: str, database_name: str):
        """
        Initiate a new connection to a MongoDB client, reachable based on the given connection string, and initialize
        class members.
        :param connection_string: A string being the complete URI to connect to the MongoDB client.
        :param database_name: A string being the MongoDB database name.
        """
        # mongodb://localhost:27017/
        # mongodb+srv://<username>:<password>@<cluster>.qo5xs5j.mongodb.net/?retryWrites=true&w=majority&appName=<app_name>
        self.database_name = database_name
        self.connection_string = connection_string
        self.client = MongoClient(host=self.connection_string)
        self.db = self.client[database_name]

        log.debug("the connection string is: %s", self.connection_string)
        log.debug("the new MongoClient is: %s", self.client)
        log.debug("the database is: %s", self.db)

        if self.check_server_is_up():
            log.info("The connection is up.")
        else:
            log.error("There was a problem while connecting to the MongoDB instance at %s.", self.connection_string)
            exit()

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

    def reset(self) -> None:
        """
        Truncate the current database.
        :return: Nothing.
        """
        log.error("Will drop the database %s", self.database_name)
        self.client.drop_database(name_or_database=self.database_name)

    def insert_many_tuples(self, table_name: str, tuples: list[dict]) -> list[int]:
        """
        Insert the given tuples in the specified table.
        :param table_name: A string being the table name in which to insert the tuples.
        :param tuples: A list of dicts being the tuples to insert.
        :return: A list of integers being the MongoDB _id of the inserted tuples.
        """
        return self.db[table_name].insert_many(tuples, ordered=False).inserted_ids

    def upsert_one_tuple(self, table_name: str, unique_variables: list[str], one_tuple: dict) -> tuple:
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
        timestamp = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        one_tuple["insertedAt"] = timestamp
        update_stmt = {"$setOnInsert": one_tuple}
        log.debug(one_tuple)

        result = self.db[table_name].find_one_and_update(filter=filter_dict, update=update_stmt, upsert=True, return_document=ReturnDocument.AFTER)
        is_insert = False
        log.debug(result)
        if result["insertedAt"] == timestamp:
            is_insert = True
        return (result, is_insert)   # this is the (MongoDB) _id of the object inserted or updated

    def upsert_many_tuples(self, table_name: str, unique_variables: list[str], tuples: list[dict]) -> None:
        """

        :param unique_variables:
        :param table_name:
        :param tuples:
        :return: An integer being the number of upserted tuples.
        """
        # in case there are more than 1000 tuples to upserts, we split them in batch of 1000
        # and send one bulk operation per batch. This allows to save time by not doing a db call per upsert
        # but do not overload the MongoDB with thousands of upserts.
        batch_tuples = self.compute_batches(tuples=tuples)
        log.debug(len(batch_tuples))
        # we use the bulk operation to send sets of 1K operations, each doing an upsert
        # this allows to have only on call to the database for each bulk operation (instead of one per upsert operation)
        for batch in batch_tuples:
            log.debug(len(batch))
            operations = []
            for one_tuple in batch:
                filter_dict = {}
                used_rands = []
                for unique_variable in unique_variables:
                    # if unique_variable="name", getattr() returns the value of the variable labelled "name" in the object
                    filter_dict[unique_variable] = one_tuple[unique_variable]
                rand_delta = randrange(0, 10)
                while rand_delta in used_rands:
                    rand_delta = randrange(0, 10)
                used_rands.append(rand_delta)
                timestamp = datetime.now() + timedelta(days=rand_delta)
                timestamp2 = timestamp.strftime("%m/%d/%Y, %H:%M:%S")
                one_tuple["insertedAt"] = timestamp2
                log.debug(one_tuple)
                update_stmt = {"$setOnInsert": one_tuple}
                operations.append(pymongo.UpdateOne(filter=filter_dict, update=update_stmt, upsert=True))

            results = self.db[table_name].bulk_write(operations)
            log.debug("sending a bulk write of %s operations", len(operations))
            log.debug(results)
            # return results.upserted_count  # this is the (MongoDB) _id of the object inserted or updated

    def compute_batches(self, tuples: list[dict]) -> list[list[dict]]:
        batch_tuples = []
        if len(tuples) <= BATCH_SIZE:
            batch_tuples.append(tuples)
        else:
            # +1 to take the elements remaining after X thousands,
            # e.g., in 4321, we need one more iteration to gt the 321 remaining elements in a batch
            nb_batch = (len(tuples) // BATCH_SIZE) + 1
            log.debug(nb_batch)
            for i in range(0, nb_batch):
                log.debug("iteration %s", i)
                left_index = i * BATCH_SIZE
                right_index = (i + 1) * BATCH_SIZE
                batch = tuples[left_index: right_index]
                log.debug("append an array with %s elements between indices %s and %s ", len(batch), left_index, right_index)
                batch_tuples.append(batch)
        return batch_tuples

    def retrieve_identifiers(self, table_name: str, projection: str) -> dict:
        projection_as_dict = {projection: 1, "identifier": 1}
        cursor = self.find_operation(table_name=table_name, filter_dict={}, projection=projection_as_dict)
        mapping = {}
        count = 0
        for result in cursor:
            log.debug(result)
            log.debug(result["identifier"])
            log.debug(result[projection])
            mapping[result[projection]] = result["identifier"]
            count = count + 1
        log.debug(count)
        log.debug(mapping)
        return mapping

    def find_operation(self, table_name: str, filter_dict: dict, projection: dict) -> Cursor:
        """
        Perform a find operation (SELECT * FROM x WHERE filter_dict) in a given table.
        :param table_name: A string being the table name in which the find operation is performed.
        :param filter_dict: A dict being the set of filters (conditions) to apply on the data in the given table.
        :param projection: A dict being the set of projections (selections) to apply on the data in the given table.
        :return: A Cursor on the results, i.e., filtered data.
        """
        log.debug("table name is: %s", table_name)
        log.debug("filter_dict is: %s", filter_dict)
        log.debug("projection is: %s", projection)
        return self.db[table_name].find(filter_dict, projection)

    def count_documents(self, table_name: str, filter_dict: dict) -> int:
        """
        Count the number of documents in a table and matching a given filter.
        :param table_name: A string being the table name in which the count operation is performed.
        :param filter_dict: A dict being the set of filters to be applied on the documents.
        :return: An integer being the number of documents matched by the given filter.
        """
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
        return self.get_min_max_value_of_examination_record(examination_url=examination_url, min_or_max="min")

    def get_max_value_of_examination_record(self, examination_url: str) -> float:
        """
        Compute the maximum value among all the examination records for a certain examination.
        :param examination_url: A string being the examination url of the form Examination/X, where X is the
        Examination number, and for which the maximum value will be computed among the examination records referring
        to that examination url.
        :return: A float value being the maximum value for the given examination url.
        """
        return self.get_min_max_value_of_examination_record(examination_url=examination_url, min_or_max="max")

    def get_min_max_value_of_examination_record(self, examination_url: str, min_or_max: str) -> float:
        """
        Compute the maximum or the maximum value among all the examination records for a certain examination.
        :param examination_url: A string being the examination url of the form Examination/X, where X is the
        Examination number, and for which the min or max value will be computed among the examination records referring
        to that examination url.
        :param min_or_max: A string being "min" or "max", depending on which value is needed.
        :return: A float value being the minimum or maximum value for the given examination url.
        """
        if min_or_max == "min":
            sort_order = 1
        elif min_or_max == "max":
            sort_order = -1
        else:
            sort_order = 1
            log.warn("You asked for something else than min or max. This will be min by default.")

        cursor = self.db[TableNames.EXAMINATION_RECORD.value].aggregate([
            mongodb_match(field="instantiate.reference", value=examination_url),
            mongodb_project_one(field="value"),
            mongodb_sort(field="value", sort_order=sort_order),
            mongodb_limit(nb=1)  # sort in ascending order and return the first value: this is the min
        ])

        for result in cursor:
            return float(result)  # There should be only one result, so we can return directly the min or max value

    def get_avg_value_of_examination_record(self, examination_url: str) -> float:
        """
        Compute the average value among all the examination records for a certain examination.
        :param examination_url: A string being the examination url of the form Examination/X, where X is the
        Examination number, and for which the avg value will be computed among the examination records referring
        to that examination url.
        :return: A float value being the average value for the given examination url.
        """
        cursor = self.db[TableNames.EXAMINATION_RECORD.value].aggregate([
            mongodb_match(field="instantiate.reference", value=examination_url),
            mongodb_project_one(field="value"),
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
            mongodb_match(field="instantiate.reference", value=examination_url),
            mongodb_project_one(field="value"),
            mongodb_group_by(group_key="$value", group_by_name="total", operator="$sum", field=1),
            mongodb_match(field="total", value={"$gt": min_value}),
            mongodb_sort(field="_id", sort_order=1)
        ]
        log.debug(pipeline)
        # .collation({"locale": "en_US", "numericOrdering": "true"})
        return self.db[TableNames.EXAMINATION_RECORD.value].aggregate(pipeline)

    def __str__(self) -> str:
        return "Database " + self.connection_string

    def get_db(self):
        return self.db
