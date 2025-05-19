import dataclasses
import json
import os
import re

import bson
import pymongo
from bson.json_util import loads
from filesplit.split import Split
from jsonlines import jsonlines
from pymongo import MongoClient
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor

from constants.defaults import MAX_FILE_SIZE
from constants.methods import factory
from database.Execution import Execution
from database.Operators import Operators
from enums.TableNames import TableNames
from utils.file_utils import get_json_resource_file
from utils.setup_logger import log


@dataclasses.dataclass(kw_only=True)
class Database:
    """
    The class Database represents the underlying MongoDB database: the connection, the database itself and
    auxiliary functions to make interactions with the database object (insert, select, ...).
    """

    SERVER_TIMEOUT = 5000
    execution: Execution
    # DO NOT DECLARE THOSE FIELDS HERE TO NOT ADD THEM TO ASDICT(),
    # because they are not thread-safe, thus are not pickable, thus cannot be jsonified
    # client: MongoClient = dataclasses.field(init=False, repr=False)
    # db: Any = dataclasses.field(init=False, repr=False)

    def __post_init__(self):
        """
        Initiate a new connection to a MongoDB client, reachable based on the given connection string, and initialize
        class members.
        """

        # 1. connect to the Mongo client
        try:
            # mongodb://localhost:27018/ -> goes through the host machine (open port - no need to have Mongo on the machine)
            # mongodb://mongo:27017/ -> mongo=Docker service name. goes through Docker network (no need to have Mongo on the machine)
            # Starting with version 3.0 the MongoClient constructor no longer blocks while connecting to the server or servers,
            # and it no longer raises pymongo (ConnectionFailure, ConfigurationError) errors.
            # Instead, the constructor returns immediately and launches the connection process on background threads.
            # You can check if the server is available with a ping.
            # w=0 disable acknowledgments from MongoDB (they are not necessary because we don't check them) to speedup write operations
            self.client = MongoClient(host=self.execution.db_connection, serverSelectionTimeoutMS=Database.SERVER_TIMEOUT, w=0)  # timeout after 5 sec instead of 20 (the default)
            log.info(type(self.client))
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
            log.info(type(self.db))
            self.drop_table(table_name=TableNames.STATS_DB)
            self.drop_table(table_name=TableNames.STATS_TIME)
            self.drop_table(table_name=TableNames.STATS_QUALITY)
        else:
            self.db = self.client[self.execution.db_name]
            log.info(type(self.db))

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
        # log.info(f"In table {table_name}, insert {one_tuple}")
        self.db[table_name].insert_one(one_tuple)

    def insert_many_tuples(self, table_name: str, tuples: list[dict] | tuple) -> None:
        """
        Insert the given tuples in the specified table.
        :param table_name: A string being the table name in which to insert the tuples.
        :param tuples: A list of dicts being the tuples to insert.
        """
        _ = self.db[table_name].insert_many(tuples, ordered=False)

    def update_one_tuple(self, table_name: str, filter_dict: dict, update: dict) -> None:
        _ = self.db[table_name].update_one(filter=filter_dict, update=self.create_update_stmt(the_tuple=update))

    def create_update_stmt(self, the_tuple: dict):
        # if self.execution.db_upsert_policy == UpsertPolicy.DO_NOTHING:
        #     # insert the document if it does not exist
        #     # otherwise, do nothing
        #     return {"$setOnInsert": the_tuple}
        # else:
        # insert the document if it does not exist
        # otherwise, replace it
        #     return {"$set": the_tuple}
        return {"$set": the_tuple}

    def upsert_one_tuple(self, table_name: str, unique_variables: list[str], one_tuple: dict) -> None:
        # filter_dict should only contain the fields on which we want a Resource to be unique,
        # e.g., name for Hospital instances, ID for Patient instances,
        #       the combination of Patient, Hospital, Clinical and LabFeature instances for LabRecord instances
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

    def upsert_one_batch_of_tuples(self, table_name: str, unique_variables: list[str], the_batch: list[dict], ordered: bool) -> None:
        """

        :param unique_variables:
        :param table_name:
        :param the_batch:
        :return: The `filter_dict` to know exactly which fields have been used for the upsert (some of the given fields in unique_variables may not exist in some instances)
        """
        log.info(unique_variables)
        operations = [pymongo.UpdateOne(
            filter={unique_variable: one_tuple[unique_variable] for unique_variable in unique_variables if unique_variable in one_tuple},
            update=self.create_update_stmt(the_tuple=one_tuple), upsert=True)
            for one_tuple in the_batch]
        # July 18th, 2024: bulk_write modifies the hospital lists in Transform (even if I use deep copies everywhere)
        # It changes (only?) the timestamp value with +1/100, e.g., 2024-07-18T14:34:32Z becomes 2024-07-18T14:34:33Z
        # in the tests I use a delta to compare datetime
        self.db[table_name].bulk_write(operations, ordered=ordered)

    def retrieve_mapping(self, table_name: str, key_fields: str, value_fields: str, filter_dict: dict):
        # TODO Nelly: add a distinct to the find
        cursor = self.find_operation(table_name=table_name, filter_dict=filter_dict, projection={key_fields: 1, value_fields: 1})
        mapping = {}
        for result in cursor:
            projected_key = result
            for one_key in key_fields.split("."):
                # this covers the case when the key of the mapping is a nested field, e.g., identifier.value
                log.info(projected_key)
                log.info(one_key)
                projected_key = projected_key[one_key]
            projected_value = result
            for one_value in value_fields.split("."):
                # this covers the case when the value of the mapping is a nested field, e.g., ontology_resource.label
                projected_value = projected_value[one_value]
            mapping[projected_key] = projected_value
        return mapping

    def load_json_in_table(self, table_name: str, unique_variables: list[str], dataset_id: int) -> None:
        self.load_json_in_table_general(table_name=table_name, unique_variables=unique_variables, dataset_id=dataset_id, ordered=False)

    def load_json_in_table_for_tests(self, unique_variables: list[str], dataset_id: int) -> None:
        self.load_json_in_table_general(table_name=TableNames.TEST, unique_variables=unique_variables, dataset_id=dataset_id, ordered=True)

    def load_json_in_table_general(self, table_name: str, unique_variables: list[str], dataset_id: int, ordered: bool) -> None:
        log.info(f"Write {table_name} data in table {table_name} with unique variables {unique_variables}")
        first_file = True
        expected_filename = get_json_resource_file(self.execution.working_dir_current, dataset_id, table_name)
        if os.path.exists(expected_filename):
            # this is a big file with all records for patients, or records, or features, or hospitals
            # we split it in smaller files of 15Mo (the MogoDB limit is 16Mo)
            # and send each chunk to the db
            split = Split(expected_filename, self.execution.working_dir_current)
            split.bysize(MAX_FILE_SIZE, newline=True)
            counter_files = 0
            regex_chunk_filename = re.compile(f"{dataset_id}{table_name}_[0-9]+\\.jsonl")
            total_count_files = sum([1 if regex_chunk_filename.search(elem) else 0 for elem in os.listdir(self.execution.working_dir_current)])
            for chunk_filename in os.listdir(self.execution.working_dir_current):
                if regex_chunk_filename.search(chunk_filename):
                    with jsonlines.open(os.path.join(self.execution.working_dir_current, chunk_filename), "r") as json_datafile:
                        if first_file:
                            # first, create an index on the unique variables to speed up the upsert (which checks whether each document already exists)
                            # we do this only if we have data for that kind of data
                            log.info(f"For table {table_name}, creating unique index {unique_variables}")
                            self.create_unique_index(table_name=table_name, columns={elem: 1 for elem in unique_variables})
                            first_file = False
                        # the chunk file is a JSON-by-line file, meaning that each record is on a line, with no separating comma and no encompassing array
                        # this needs to be added back to the JSON read string before parsing it
                        tuples = [obj for obj in json_datafile]  # this transforms the JSONL file to a list of objects
                        tuples = bson.json_util.loads(json.dumps(tuples))  # we need to read the objects with bson to interpret dates
                        self.upsert_one_batch_of_tuples(table_name=table_name, unique_variables=unique_variables, the_batch=tuples, ordered=ordered)
                        counter_files += 1
                        if counter_files % 5 == 0:
                            log.debug(f"Table {table_name}, loaded {counter_files}/{total_count_files}")
            log.debug(f"Table {table_name}, loaded {counter_files}/{total_count_files}")
        else:
            log.debug(f"Table {table_name}, no file to load.")

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
        return self.db[table_name].distinct(key, filter_dict)

    def count_documents(self, table_name: str, filter_dict: dict) -> int:
        """
        Count the number of documents in a table and matching a given filter.
        :param table_name: A string being the table name in which the count operation is performed.
        :param filter_dict: A dict being the set of filters to be applied on the documents.
        :return: An integer being the number of documents matched by the given filter.
        """
        return self.db[table_name].count_documents(filter_dict)

    def inverse_inner_join(self, name_table_1: str, name_table_2: str, foreign_field: str, local_field: str, lookup_name: str) -> CommandCursor:
        operations = [
            Operators.lookup(join_table_name=name_table_2, foreign_field=foreign_field, local_field=local_field, lookup_field_name=lookup_name, let=None, pipeline=None),
            Operators.match(field=lookup_name, value={"$eq": []}, is_regex=False),
            Operators.set_variables([{"name": "_id", "operation": 0}])
        ]
        return self.db[name_table_1].aggregate(operations)

    def list_existing_indexes(self, table_name: str) -> list:
        index_list = [res.values() for res in self.db[table_name].list_indexes()]
        log.info(index_list)
        return index_list

    def create_unique_index(self, table_name: str, columns: dict) -> None:
        """
        Create a unique constraint/index on a (set of) column(s).
        :param table_name: A string being the table name on which the index will be created.
        :param columns: A dict being the set of columns to be included in the index. It may contain only one entry if
        only one column should be unique. The parameter should be of the form { "colA": 1, ... }.
        :return: Nothing.
        """
        log.info(f"create unique index in {table_name} on columns {columns}")
        self.db[table_name].create_index(columns, unique=True)

    def create_non_unique_index(self, table_name: str, columns: dict) -> None:
        """
        Create an index on a (set of) column(s) for which uniqueness is not guaranteed.
        :param table_name: A string being the table name on which the index will be created.
        :param columns: A dict being the set of columns to be included in the index. It may contain only one entry if
        only one column should be unique. The parameter should be of the form { "colA": 1, ... }.
        :return: Nothing.
        """
        log.info(f"create non-unique index in {table_name} on columns {columns}")
        self.db[table_name].create_index(columns, unique=False)

    def create_on_demand_view(self, table_name: str, view_name: str, pipeline: list) -> None:
        self.drop_table(table_name=view_name)
        self.db.create_collection(view_name, viewOn=table_name, pipeline=pipeline)

    def refresh_on_demand_view(self, table_name: str, view_name: str, pipeline: list) -> None:
        # there is no explicit mechanism to refresh a materialized (on-demand) view
        # one need to recompute it
        self.create_on_demand_view(table_name=table_name, view_name=view_name, pipeline=pipeline)

    def get_min_or_max_value(self, table_name: str, field: str, sort_order: int) -> int | float:
        operations = []
        last_field = field.split(".")[-1]

        # if from_string:
        #     # we need to parse the string to long
        #     operations.append(Operators.project(field=field, projected_value={"split_var": {"$split": [f"${field}", DELIMITER_RESOURCE_ID]}}))
        #     operations.append(Operators.unwind(field="split_var"))
        #     operations.append(Operators.match(field="split_var", value="^[0-9]+$", is_regex=True))  # only numbers
        #     operations.append(Operators.group_by(group_key={"var": "$identifier"}, groups=[{"name": "min_max", "operator": "$max", "field": {"$toLong": "$split_var"}}]))
        #     operations.append(Operators.sort(field="min_max", sort_order=sort_order))
        #     operations.append(Operators.limit(1))
        #
        #     # better_default > db["ExaminationRecord"].aggregate([
        #     #     {"$project": {"identifier.value": {"$split": ["$identifier.value", "/"]}}},
        #     #     {"$unwind": "$identifier.value"},
        #     #     {"$match": {"identifier.value": / [0 - 9] + /}},
        #     # {"$group": {"_id": "identifier.value", "Max": {"$max": {"$toLong": "$identifier.value"}}}}
        #     # ])
        # else:
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
            # if from_string:
            #     return result["min_max"]
            # else:
            if "." in field:
                return result[last_field]
            else:
                return result[field]
        return -1

    def get_max_value(self, table_name: str, field: str) -> int | float:
        return self.get_min_or_max_value(table_name=table_name, field=field, sort_order=-1)

    def get_min_value(self, table_name: str, field: str) -> int | float:
        return self.get_min_or_max_value(table_name=table_name, field=field, sort_order=1)

    def get_max_resource_counter_id(self) -> int:
        max_value = -1
        for table_name in TableNames.data_tables():
            current_max_identifier = self.get_max_value(table_name=table_name, field="identifier") # cannot use Resource.IDENTIFIER_ because ti leads to a circular dependency
            log.info(f"current max identifier for {table_name} is {current_max_identifier}")
            if current_max_identifier is not None:
                try:
                    current_max_identifier = int(current_max_identifier)
                    if current_max_identifier > max_value:
                        max_value = current_max_identifier
                except ValueError:
                    # this identifier is not an integer, e.g., a Clinical base ID like 24DL54
                    # we simply ignore it and try to find the next maximum integer ID
                    pass
            else:
                # the table is not created yet (this happens when we start from a fresh new DB, thus we skip this)
                pass
        return max_value

    def db_exists(self, db_name: str) -> bool:
        list_dbs = self.client.list_databases()
        for db in list_dbs:
            if db['name'] == db_name:
                return True
        return False

    def to_json(self):
        # for this class specifically, we cannot use the default factory
        # because PyMongo objects are not serializable
        return dataclasses.asdict(self, dict_factory=factory)
        # return {
        #     "execution": self.execution.to_json(),
        #     "mongo_client": str(self.client)
        # }

    def __str__(self):
        return json.dumps(self.to_json())
