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
from constants.idColumns import DELIMITER_RESOURCE_ID
from query.Operators import Operators
from utils.setup_logger import log


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
        self.execution = execution
        self.client = None
        self.db = None

        # 1. connect to the Mongo client
        try:
            # mongodb://localhost:27017/
            # mongodb://127.0.0.1:27017/
            # mongodb+srv://<username>:<password>@<cluster>.qo5xs5j.mongodb.net/?retryWrites=true&w=majority&appName=<app_name>
            # Starting with version 3.0 the MongoClient constructor no longer blocks while connecting to the server or servers,
            # and it no longer raises pymongo (ConnectionFailure, ConfigurationError) errors.
            # Instead, the constructor returns immediately and launches the connection process on background threads.
            # You can check if the server is available with a ping.
            self.client = MongoClient(host=self.execution.db_connection, serverSelectionTimeoutMS=Database.SERVER_TIMEOUT)  # timeout after 5 sec instead of 20 (the default)
        except Exception:
            raise ConnectionError(f"Could not connect to the MongoDB client located at {self.execution.db_connection} and with a timeout of {Database.SERVER_TIMEOUT} ms.")

        # 2 check if the client is running well
        self.check_server_is_up()
        # if we reach this point, the MongoDB client runs correctly.
        log.info(f"The MongoDB client, located at {self.execution.db_connection}, could be accessed properly.")

        # 3. access the database
        log.info(f"drop db is: {self.execution.db_drop}")
        if self.execution.db_drop:
            self.drop_db()
            self.db = self.client[self.execution.db_name]
            self.drop_table(table_name=TableNames.STATS_DB)
            self.drop_table(table_name=TableNames.STATS_TIME)
            self.drop_table(table_name=TableNames.STATS_QUALITY)
        else:
            self.db = self.client[self.execution.db_name]

        log.debug(f"the connection string is: {self.execution.db_connection}")
        log.debug(f"the new MongoClient is: {self.client}")
        log.debug(f"the database is: {self.db}")

    def check_server_is_up(self) -> None:
        """
        Send a ping to confirm a successful connection.
        :return: A boolean being whether the MongoDB client is up.
        """
        try:
            self.client.admin.command('ping')
        except Exception:
            raise ConnectionError(f"The MongoDB client located at {self.execution.db_connection} could not be accessed properly.")

    def check_table_exists(self, table_name: str) -> bool:
        return table_name in self.db.list_collection_names()

    def drop_table(self, table_name: str) -> None:
        self.db.drop_collection(table_name)

    def drop_db(self) -> None:
        """
        Drop the current database.
        :return: Nothing.
        """
        if self.execution.db_drop:
            log.info(f"WARNING: The database {self.execution.db_name} will be dropped!", )
            self.client.drop_database(name_or_database=self.execution.db_name)

    def close(self) -> None:
        self.client.close()

    def insert_one_tuple(self, table_name: str, one_tuple: dict) -> None:
        self.db[table_name].insert_one(one_tuple)

    def insert_many_tuples(self, table_name: str, tuples: list[dict] | tuple) -> None:
        """
        Insert the given tuples in the specified table.
        :param table_name: A string being the table name in which to insert the tuples.
        :param tuples: A list of dicts being the tuples to insert.
        """
        _ = self.db[table_name].insert_many(tuples, ordered=False)

    def create_update_stmt(self, the_tuple: dict):
        if self.execution.db_upsert_policy == UpsertPolicy.DO_NOTHING:
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
        #       the combination of Patient, Hospital, Sample and LabFeature instances for LabRecord instances
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
        self.db[table_name].find_one_and_update(filter=filter_dict, update=update_stmt, upsert=True)

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
                    # only BUZZI LabRecord instances have a "based_on" attribute for the Samples
                    # others do not have the "based_on", so we need to check whether that attribute is present or not
                    filter_dict[unique_variable] = one_tuple[unique_variable]
            update_stmt = self.create_update_stmt(the_tuple=one_tuple)
            operations.append(pymongo.UpdateOne(filter=filter_dict, update=update_stmt, upsert=True))
        # July 18th, 2024: bulk_write modifies the hospital lists in Transform (even if I use deep copies everywhere)
        # It changes (only?) the timestamp value with +1/100, e.g., 2024-07-18T14:34:32Z becomes 2024-07-18T14:34:33Z
        # in the tests I use a delta to compare datetime
        self.db[table_name].bulk_write(copy.deepcopy(operations))
        return filter_dict

    def retrieve_mapping(self, table_name: str, key_fields: str, value_fields: str):
        # TODO Nelly: add a distinct to the find
        cursor = self.find_operation(table_name=table_name, filter_dict={}, projection={key_fields: 1, value_fields: 1})
        mapping = {}
        for result in cursor:
            projected_key = result
            for one_key in key_fields.split("."):
                # this covers the case when the key of the mapping is a nested field, e.g., identifier.value
                projected_key = projected_key[one_key]
            projected_value = result
            for one_value in value_fields.split("."):
                # this covers the case when the value of the mapping is a nested field, e.g., ontology_resource.label
                projected_value = projected_value[one_value]
            mapping[projected_key] = projected_value
        return mapping

    def load_json_in_table(self, table_name: str, unique_variables) -> None:
        for filename in os.listdir(self.execution.working_dir_current):
            if re.search(table_name+"[0-9]+", filename) is not None:
                # implementation note: we cannot simply use filename.startswith(table_name)
                # because both LaboratoryFeature and LaboratoryRecord start with Laboratory
                # the solution is to use a regex
                with open(os.path.join(self.execution.working_dir_current, filename), "r") as json_datafile:
                    tuples = bson.json_util.loads(json_datafile.read())
                    log.debug(f"Table {table_name}, file {filename}, loading {len(tuples)} tuples")
                    _ = self.upsert_one_batch_of_tuples(table_name=table_name,
                                                        unique_variables=unique_variables,
                                                        the_batch=tuples)

    def find_operation(self, table_name: str, filter_dict: dict, projection: dict) -> Cursor:
        """
        Perform a find operation (SELECT * FROM x WHERE filter_dict) in a given table.
        :param table_name: A string being the table name in which the find operation is performed.
        :param filter_dict: A dict being the set of filters (conditions) to apply on the data in the given table. Give {} to not apply any filter.
        :param projection: A dict being the set of projections (selections) to apply on the data in the given table. Give {} to return all fields.
        :return: A Cursor on the results, i.e., filtered data.
        """
        return self.db[table_name].find(filter_dict, projection)

    def find_distinct_operation(self, table_name: str, key: str, filter_dict: dict):
        """
        Perform a distinct operation on a field "key", with some filters on instances ("filter")
        :param table_name: A string being the table name in which the find operation is performed.
        :param key: A string being the key on which to apply the distinct; this is also the returned field.
        :param filter_dict: A dict being the set of filters (conditions) to apply on the data in the given table. Give {} to apply no filter.
        :return: A Cursor on the results, i.e., the distinct results.
        """
        # db["LaboratoryRecord"].distinct("instantiate", {"value": {"$exists": 0}})
        return self.db[table_name].distinct(key, filter_dict)

    def count_documents(self, table_name: str, filter_dict: dict) -> int:
        """
        Count the number of documents in a table and matching a given filter.
        :param table_name: A string being the table name in which the count operation is performed.
        :param filter_dict: A dict being the set of filters to be applied on the documents.
        :return: An integer being the number of documents matched by the given filter.
        """
        return self.db[table_name].count_documents(filter_dict)

    def inverse_inner_join(self, name_table_1: str, name_table_2: str, field_table_1: str, field_table_2: str, lookup_name: str) -> CommandCursor:
        operations = []
        operations.append(Operators.lookup(join_table_name=name_table_2, field_table_1=field_table_1, field_table_2=field_table_2, lookup_field_name=lookup_name))
        operations.append(Operators.match(field=lookup_name, value={"$eq": []}, is_regex=False))  # we get only pairs that could not match
        return self.db[name_table_1].aggregate(operations)

    def create_unique_index(self, table_name: str, columns: dict) -> None:
        """
        Create a unique constraint/index on a (set of) column(s).
        :param table_name: A string being the table name on which the index will be created.
        :param columns: A dict being the set of columns to be included in the index. It may contain only one entry if
        only one column should be unique. The parameter should be of the form { "colA": 1, ... }.
        :return: Nothing.
        """
        self.db[table_name].create_index(columns, unique=True)

    def create_non_unique_index(self, table_name: str, columns: dict) -> None:
        """
        Create an index on a (set of) column(s) for which uniqueness is not guaranteed.
        :param table_name: A string being the table name on which the index will be created.
        :param columns: A dict being the set of columns to be included in the index. It may contain only one entry if
        only one column should be unique. The parameter should be of the form { "colA": 1, ... }.
        :return: Nothing.
        """
        self.db[table_name].create_index(columns, unique=False)

    def get_min_or_max_value(self, table_name: str, field: str, sort_order: int, from_string: bool) -> int | float:
        operations = []
        last_field = field.split(".")[-1]

        if from_string:
            # we need to parse the string to long
            operations.append(Operators.project(field=field, projected_value={"split_var": {"$split": [f"${field}", DELIMITER_RESOURCE_ID]}}))
            operations.append(Operators.unwind(field="split_var"))
            operations.append(Operators.match(field="split_var", value="^[0-9]+$", is_regex=True))  # only numbers
            operations.append(Operators.group_by(group_key={"var": "$split_var"}, groups=[{"name": "min_max", "operator": "$max", "field": {"$toLong": "$split_var"}}]))
            operations.append(Operators.sort(field="min_max", sort_order=sort_order))
            operations.append(Operators.limit(1))

            # better_default > db["ExaminationRecord"].aggregate([
            #     {"$project": {"identifier.value": {"$split": ["$identifier.value", "/"]}}},
            #     {"$unwind": "$identifier.value"},
            #     {"$match": {"identifier.value": / [0 - 9] + /}},
            # {"$group": {"_id": "identifier.value", "Max": {"$max": {"$toLong": "$identifier.value"}}}}
            # ])
        else:
            if "." in field:
                # this field is a nested one, we only keep the deepest one,
                # e.g. for { "identifier": {"value": 1}} we keep { "value": 1}
                operations.append(Operators.project(field=field, projected_value=last_field))
                operations.append(Operators.sort(field=last_field, sort_order=sort_order))
                operations.append(Operators.limit(1))
            else:
                operations.append(Operators.project(field=field, projected_value=None))
                operations.append(Operators.sort(field=field, sort_order=sort_order))
                operations.append(Operators.limit(1))

        cursor = self.db[table_name].aggregate(operations)
        for result in cursor:
            # There should be only one result, so we can return directly the min or max value
            if from_string:
                return result["min_max"]
            else:
                if "." in field:
                    return result[last_field]
                else:
                    return result[field]
        return -1

    def get_max_value(self, table_name: str, field: str, from_string: bool) -> int | float:
        return self.get_min_or_max_value(table_name=table_name, field=field, sort_order=-1, from_string=from_string)

    def get_min_value(self, table_name: str, field: str, from_string: bool) -> int | float:
        return self.get_min_or_max_value(table_name=table_name, field=field, sort_order=1, from_string=from_string)

    def get_avg_value_of_lab_feature(self, lab_feature_url: str) -> int | float:
        """
        Compute the average value among all the LabRecord instances for a certain LabFeature.
        :param lab_feature_url: A string being the LabFeature url of the form LabFeature/X, where X is the
        LabFeature number, and for which the avg value will be computed among the LabRecord instance referring
        to that LabFeature.
        :return: A float value being the average value for the given LabFeature instance (url).
        """
        cursor = self.db[TableNames.LABORATORY_RECORD].aggregate([
            Operators.match(field="instantiate.reference", value=lab_feature_url, is_regex=False),
            Operators.project(field="value", projected_value=None),
            Operators.group_by(group_key=None, groups=[{"name": "avg_val", "operator": "$avg", "field": "$value"}])
        ])

        for result in cursor:
            return float(result)  # There should be only one result, so we can return directly the min or max value

    def get_max_resource_counter_id(self) -> int:
        max_value = -1
        for table_name in TableNames.values(db=self):
            if table_name == TableNames.SAMPLE_RECORD:
                # pass because Sample resources have their ID assigned by hospitals, not the FAIRificator
                pass
            else:
                current_max_identifier = self.get_max_value(table_name=table_name, field="identifier", from_string=True)
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
        return f"Database {self.execution.db_name}"

    def db_exists(self, db_name: str) -> bool:
        list_dbs = self.client.list_databases()
        for db in list_dbs:
            if db['name'] == db_name:
                return True
        return False
