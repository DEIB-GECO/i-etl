import copy
import json
import os.path
import random
import unittest

import pytest
from jsonlines import jsonlines

from constants.defaults import NO_ID
from constants.structure import TEST_DB_NAME
from database.Counter import Counter
from database.Database import Database
from database.Execution import Execution
from entities.Record import Record
from entities.Resource import Resource
from entities.ResourceTest import ResourceTest
from enums.HospitalNames import HospitalNames
from enums.ParameterKeys import ParameterKeys
from enums.TableNames import TableNames
from utils.file_utils import write_in_file, from_json_line_to_json_str, get_json_resource_file
from utils.test_utils import wrong_number_of_docs, compare_tuples, set_env_variables_from_dict


class TestDatabase(unittest.TestCase):
    execution = Execution()

    def setUp(self):
        # before each test, get back to the original test configuration
        args = {
            ParameterKeys.DB_NAME: TEST_DB_NAME,
            ParameterKeys.DB_DROP: "True",
            ParameterKeys.HOSPITAL_NAME: HospitalNames.TEST_H1
        }
        set_env_variables_from_dict(env_vars=args)
        TestDatabase.execution.internals_set_up()
        TestDatabase.execution.file_set_up(setup_files=False)

    def test_check_server_is_up(self):
        # test with the correct (default) string
        _ = Database(execution=TestDatabase.execution)  # this should return no exception (successful connection)
        # database.close()

        # test with a wrong connection string
        # NB Aug 20, 2024: this cannot be tested anymore because the MongoDB uri is now internal to Docker, thus cannot be changed.
        # set_env_variables_from_dict(env_vars={ ExecutionKeys.DB_CONNECTION_KEY: "a_random_string" })
        # log.debug("Set up in test")
        # TestDatabase.execution.set_up(setup_data_files=False)
        # with pytest.raises(ConnectionError):
        #     _ = Database(execution=TestDatabase.execution)  # this should return an exception (broken connection) because check_server_is_up() will return one

    def test_drop(self):
        # create a test database
        # and add only one triple to be sure that the db is created
        database = Database(execution=TestDatabase.execution)
        database.db[TableNames.TEST].insert_one(document={"id": 1, "name": "Alice Doe"})
        assert database.db_exists(TEST_DB_NAME) is True, "The database does not exist."
        database.drop_db()
        # check the DB does not exist anymore after drop
        assert database.db_exists(TEST_DB_NAME) is False, "The database has not been dropped."

    def test_no_drop(self):
        # test that the database is not dropped when the user asks for it
        # 1. create a db with a single table and only one tuple
        database = Database(execution=TestDatabase.execution)
        database.db[TableNames.TEST].insert_one(document={"id": 1, "name": "Alice Doe"})
        assert database.db_exists(TEST_DB_NAME) is True, "The database does not exist."  # we make sure that the db exists

        # 2. we create a new instance to the same database, with drop_db=False
        set_env_variables_from_dict(env_vars={ParameterKeys.DB_DROP: "False"})
        TestDatabase.execution.internals_set_up()
        TestDatabase.execution.file_set_up(setup_files=False)
        database = Database(execution=TestDatabase.execution)

        # 3. check that the database still exists (i.e., the constructor did not reset it)
        assert database.db_exists(TEST_DB_NAME) is True, "The database does not exist."

        # 4. check that the database still exists, even after a drop
        database.drop_db()  # this should do nothing as we explicitly specified to not drop the db
        assert database.db_exists(TEST_DB_NAME) is True, "The database has not been dropped."

    def test_insert_empty_tuple(self):
        database = Database(execution=TestDatabase.execution)
        my_tuple = {}
        my_original_tuple = copy.deepcopy(my_tuple)
        database.insert_one_tuple(table_name=TableNames.TEST, one_tuple=my_tuple)
        docs = [doc for doc in database.db[TableNames.TEST].find({})]
        assert len(docs) == 1, wrong_number_of_docs(1)
        compare_tuples(original_tuple=my_original_tuple, inserted_tuple=docs[0])

    def test_insert_one_tuple(self):
        # 1. create a db with a single table and only one tuple
        database = Database(execution=TestDatabase.execution)
        my_tuple = {"id": "1", "name": "Alice Doe"}
        my_original_tuple = copy.deepcopy(my_tuple)
        database.insert_one_tuple(table_name=TableNames.TEST, one_tuple=my_tuple)
        docs = [doc for doc in database.db[TableNames.TEST].find({})]

        assert len(docs) == 1, wrong_number_of_docs(1)
        # if assert does not fail, we indeed have one tuple, thus we can check its attributes directly
        compare_tuples(original_tuple=my_original_tuple, inserted_tuple=docs[0])

    def test_insert_no_tuples(self):
        database = Database(execution=TestDatabase.execution)
        my_tuples = []

        with pytest.raises(TypeError):
            # mongoDB does not allow to insert an empty array
            database.insert_many_tuples(table_name=TableNames.TEST, tuples=my_tuples)

    def test_insert_many_tuples(self):
        database = Database(execution=TestDatabase.execution)
        my_tuples = [{"id": 1, "name": "Louise", "country": "FR", "job": "PhD student"},
                      {"id": 2, "name": "Francesca", "country": "IT", "university": True},
                      {"id": 3, "name": "Martin", "country": "DE", "age": 26}]
        my_original_tuples = copy.deepcopy(my_tuples)  # we need to do a deep copy because MongoDB's insert does modify the given list of tuples
        database.insert_many_tuples(table_name=TableNames.TEST, tuples=my_tuples)
        docs = [doc for doc in database.db[TableNames.TEST].find({}).sort({"id": 1})]

        assert len(my_original_tuples) == len(docs), wrong_number_of_docs(len(my_original_tuples))
        # the three tuples are sorted, so we can iterate over them easily
        for i in range(len(my_original_tuples)):
            compare_tuples(original_tuple=my_original_tuples[i], inserted_tuple=docs[i])

    def test_upsert_one_tuple_single_key(self):
        # remark: even if that would be cleaner, I cannot separate each individual upsert test
        # because we don't know in advance the test execution order
        # and, they need to be executed in order to test properly whether upsert works
        database = Database(execution=TestDatabase.execution)
        my_tuple = {"name": "Nelly", "age": 26}
        my_original_tuple = copy.deepcopy(my_tuple)
        database.upsert_one_tuple(table_name=TableNames.TEST, unique_variables=["name"], one_tuple=my_tuple)

        # 1. first, we check that the initial upserted tuple has been correctly inserted
        docs = [doc for doc in database.db[TableNames.TEST].find({})]
        assert len(docs) == 1, wrong_number_of_docs(1)
        compare_tuples(original_tuple=my_original_tuple, inserted_tuple=docs[0])

        # 2. We upsert the exact same tuple:
        # there should be no duplicate
        database.upsert_one_tuple(table_name=TableNames.TEST, unique_variables=["name"], one_tuple=my_tuple)
        docs = [doc for doc in database.db[TableNames.TEST].find({})]
        assert len(docs) == 1, wrong_number_of_docs(1)
        compare_tuples(original_tuple=my_original_tuple, inserted_tuple=docs[0])  # we also check that the tuple did not change

        # 3. We upsert the same tuple with a different age with REPLACE:
        # no new tuple should be inserted (that document already exists)
        # the tuple should be updated
        args = {
            ParameterKeys.DB_DROP: "False"
        }
        set_env_variables_from_dict(env_vars=args)
        TestDatabase.execution.internals_set_up()
        TestDatabase.execution.file_set_up(setup_files=False)
        database = Database(execution=TestDatabase.execution)
        my_tuple_age = {"name": "Nelly", "age": 27, "city": "Lyon"}  # same as my_tuple but with a different age and a new field city
        my_original_tuple_age = copy.deepcopy(my_tuple_age)
        database.upsert_one_tuple(table_name=TableNames.TEST, unique_variables=["name"], one_tuple=my_tuple_age)
        docs = [doc for doc in database.db[TableNames.TEST].find({})]
        assert len(docs) == 1, wrong_number_of_docs(1)
        compare_tuples(original_tuple=my_original_tuple_age, inserted_tuple=docs[0])  # we check that the tuple did change (REPLACE)

    def test_upsert_one_tuple_unknown_key(self):
        database = Database(execution=TestDatabase.execution)
        my_tuple = {"name": "Nelly", "age": 26}

        with pytest.raises(KeyError):
            # this should return an exception (value error) because the upsert contains an unknown key (city does not exist in my_tuple)
            database.upsert_one_tuple(table_name=TableNames.TEST, unique_variables=["city"], one_tuple=my_tuple)

    def test_upsert_one_tuple_multi_key(self):
        database = Database(execution=TestDatabase.execution)
        my_tuple = {"name": "Nelly", "age": 26}
        my_original_tuple = copy.deepcopy(my_tuple)
        database.upsert_one_tuple(table_name=TableNames.TEST, unique_variables=["name", "age"], one_tuple=my_tuple)

        # 1. first, we check that the initial upserted tuple has been correctly inserted
        docs = [doc for doc in database.db[TableNames.TEST].find({})]
        assert len(docs) == 1, wrong_number_of_docs(1)
        compare_tuples(original_tuple=my_original_tuple, inserted_tuple=docs[0])

        # 2. We upsert the exact same tuple:
        # there should be no duplicate
        database.upsert_one_tuple(table_name=TableNames.TEST, unique_variables=["name", "age"], one_tuple=my_tuple)
        docs = [doc for doc in database.db[TableNames.TEST].find({})]
        assert len(docs) == 1, wrong_number_of_docs(1)
        compare_tuples(original_tuple=my_original_tuple, inserted_tuple=docs[0])  # we also check that the tuple did not change

        # 3. We upsert the same tuple with a different age with REPLACE:
        # a new tuple is added because no existing tuple has this combination (name, age)
        # and the current one should not be updated because no tuple should have matched
        args = {
            ParameterKeys.DB_DROP: "False"
        }
        set_env_variables_from_dict(env_vars=args)
        TestDatabase.execution.internals_set_up()
        TestDatabase.execution.file_set_up(setup_files=False)
        database = Database(execution=TestDatabase.execution)
        my_tuple_age = {"name": "Nelly", "age": 27, "city": "Lyon"}  # same as my_tuple but with a different age and a new field city
        my_original_tuple_age = copy.deepcopy(my_tuple_age)
        database.upsert_one_tuple(table_name=TableNames.TEST, unique_variables=["name", "age"], one_tuple=my_tuple_age)
        docs = [doc for doc in database.db[TableNames.TEST].find({}).sort({"name": 1, "age": 1})]
        expected_docs = [my_original_tuple, my_original_tuple_age]
        assert len(docs) == 2, wrong_number_of_docs(2)
        for i in range(len(expected_docs)):
            compare_tuples(original_tuple=expected_docs[i], inserted_tuple=docs[i])

        # 4. We upsert a similar tuple (same name and age but different city) with REPLACE:
        # no new tuple should be inserted (that document does already exist)
        # the former one should have changed (because REPLACE)
        args = {
            ParameterKeys.DB_DROP: "False"
        }
        set_env_variables_from_dict(env_vars=args)
        TestDatabase.execution.internals_set_up()
        TestDatabase.execution.file_set_up(setup_files=False)
        database = Database(execution=TestDatabase.execution)
        my_new_tuple = {"name": "Nelly", "age": 26, "city": "Lyon"}
        my_original_new_tuple = copy.deepcopy(my_new_tuple)
        database.upsert_one_tuple(table_name=TableNames.TEST, unique_variables=["name", "age"],
                                  one_tuple=my_new_tuple)
        docs = [doc for doc in database.db[TableNames.TEST].find({}).sort({"name": 1, "age": 1})]
        expected_docs = [my_original_new_tuple, my_original_tuple_age]  # alphabetical order
        assert len(docs) == len(expected_docs), wrong_number_of_docs(len(expected_docs))
        for i in range(len(expected_docs)):
            compare_tuples(original_tuple=expected_docs[i], inserted_tuple=docs[i])

    def test_upsert_one_batch_of_tuples_single_key(self):
        database = Database(execution=TestDatabase.execution)
        # 1. upsert an initial batch of 2 tuples
        my_batch = [{"name": "Nelly", "age": 26}, {"name": "Julien", "age": 30, "city": "Lyon"}]
        my_original_batch = copy.deepcopy(my_batch)
        database.upsert_one_batch_of_tuples(table_name=TableNames.TEST, unique_variables=["name"], the_batch=my_batch, ordered=True)
        docs = [doc for doc in database.db[TableNames.TEST].find({})]
        assert len(docs) == 2, wrong_number_of_docs(2)
        for i in range(len(my_original_batch)):
            compare_tuples(original_tuple=my_original_batch[i], inserted_tuple=docs[i])

        # 2. upsert a batch with one similar tuple, and one different
        my_batch_2 = [{"name": "Nelly", "age": 27}, {"name": "Pietro", "city": "Milano"}]
        database.upsert_one_batch_of_tuples(table_name=TableNames.TEST, unique_variables=["name"], the_batch=my_batch_2, ordered=True)
        expected_docs = [my_original_batch[1], my_batch_2[0], my_batch_2[1]]  # ordered by name
        docs = [doc for doc in database.db[TableNames.TEST].find({}).sort({"name": 1})]
        assert len(docs) == len(expected_docs), wrong_number_of_docs(len(expected_docs))
        for i in range(len(expected_docs)):
            compare_tuples(original_tuple=expected_docs[i], inserted_tuple=docs[i])

        # 3. upsert a batch with one similar tuple and one different
        # with upsert policy being REPLACE
        args = {
            ParameterKeys.DB_DROP: "False"
        }
        set_env_variables_from_dict(env_vars=args)
        TestDatabase.execution.internals_set_up()
        TestDatabase.execution.file_set_up(setup_files=False)
        database = Database(execution=TestDatabase.execution)
        my_batch_3 = [{"name": "Nelly", "age": 27}, {"name": "Anna", "citizenship": "Italian"}]
        database.upsert_one_batch_of_tuples(table_name=TableNames.TEST, unique_variables=["name"], the_batch=my_batch_3, ordered=True)
        expected_docs = [my_batch_3[1], my_original_batch[1], my_batch_3[0], my_batch_2[1]]  # ordered by name
        docs = [doc for doc in database.db[TableNames.TEST].find({}).sort({"name": 1})]
        assert len(docs) == len(expected_docs), wrong_number_of_docs(len(expected_docs))
        for i in range(len(expected_docs)):
            compare_tuples(original_tuple=expected_docs[i], inserted_tuple=docs[i])

    def test_upsert_one_batch_of_tuples_multi_key(self):
        database = Database(execution=TestDatabase.execution)
        my_tuples = [{"name": "Nelly", "age": 26}, {"name": "Julien", "age": 30}]
        my_original_tuples = copy.deepcopy(my_tuples)
        database.upsert_one_batch_of_tuples(table_name=TableNames.TEST, unique_variables=["name", "age"], the_batch=my_tuples, ordered=True)

        # 1. first, we check that the initial upserted tuple has been correctly inserted
        docs = [doc for doc in database.db[TableNames.TEST].find({})]
        assert len(docs) == len(my_original_tuples), wrong_number_of_docs(len(my_original_tuples))
        for i in range(len(my_original_tuples)):
            compare_tuples(original_tuple=my_original_tuples[i], inserted_tuple=docs[i])

        # 2. We upsert the exact same batch of tuples:
        # there should be no duplicate
        my_original_tuples_2 = copy.deepcopy(my_original_tuples)
        database.upsert_one_batch_of_tuples(table_name=TableNames.TEST, unique_variables=["name", "age"], the_batch=my_original_tuples, ordered=True)
        docs = [doc for doc in database.db[TableNames.TEST].find({})]
        assert len(docs) == len(my_original_tuples_2), wrong_number_of_docs(len(my_original_tuples_2))
        for i in range(len(my_original_tuples_2)):
            compare_tuples(original_tuple=my_original_tuples_2[i], inserted_tuple=docs[i])

        # 3. We upsert one (out of 2) same tuple with a different city with REPLACE:
        # a new tuple is added because no existing tuple has this combination (name, age)
        # and the current ones should not be updated because no tuple should have matched
        args = {
            ParameterKeys.DB_DROP: "False"
        }
        set_env_variables_from_dict(env_vars=args)
        TestDatabase.execution.internals_set_up()
        TestDatabase.execution.file_set_up(setup_files=False)
        database = Database(execution=TestDatabase.execution)
        my_tuples_age_2 = [{"name": "Nelly", "age": 26, "city": "Paris"}, {"name": "Pietro", "age": -1, "city": "Milano"}]
        my_original_tuple_age_2 = copy.deepcopy(my_tuples_age_2)
        database.upsert_one_batch_of_tuples(table_name=TableNames.TEST, unique_variables=["name", "age"], the_batch=my_tuples_age_2, ordered=True)
        docs = [doc for doc in database.db[TableNames.TEST].find({}).sort({"name": 1, "age": 1})]
        expected_docs = [my_original_tuples[1], my_original_tuple_age_2[0], my_original_tuple_age_2[1]]
        assert len(docs) == len(expected_docs), wrong_number_of_docs(len(expected_docs))
        for i in range(len(expected_docs)):
            compare_tuples(original_tuple=expected_docs[i], inserted_tuple=docs[i])

    def test_retrieve_resource_identifiers_1(self):
        database = Database(execution=TestDatabase.execution)
        my_tuple = {"identifier": 123, "name": "Nelly"}
        database.db[TableNames.TEST].insert_one(my_tuple)
        the_doc = database.retrieve_mapping(table_name=TableNames.TEST, key_fields="name", value_fields="identifier", filter_dict={})
        expected_doc = {"Nelly": 123}
        assert the_doc == expected_doc

    def test_retrieve_resource_identifiers_10(self):
        database = Database(execution=TestDatabase.execution)
        my_tuples = [{Resource.IDENTIFIER_: i, Record.VALUE_: i + random.randint(0, 100)} for i in range(0, 10)]
        my_original_tuples = copy.deepcopy(my_tuples)
        database.db[TableNames.TEST].insert_many(my_tuples)
        docs = database.retrieve_mapping(table_name=TableNames.TEST, key_fields=Record.VALUE_, value_fields=Resource.IDENTIFIER_, filter_dict={})
        expected_docs = {}
        for doc in my_original_tuples:
            expected_docs[doc[Record.VALUE_]] = doc[Resource.IDENTIFIER_]
        assert len(docs) == len(expected_docs), wrong_number_of_docs(len(expected_docs))
        for key, value in expected_docs.items():
            compare_tuples(original_tuple={key: value}, inserted_tuple={key: docs[key]})

    def test_retrieve_resource_identifiers_wrong_key(self):
        database = Database(execution=TestDatabase.execution)
        my_tuple = {"identifier": 123, "name": "Nelly"}
        database.db[TableNames.TEST].insert_one(my_tuple)
        with pytest.raises(KeyError):
            _ = database.retrieve_mapping(table_name=TableNames.TEST, key_fields="name2", value_fields="identifier", filter_dict={})

    def test_retrieve_patient_identifiers_1(self):
        database = Database(execution=TestDatabase.execution)
        my_tuple = {"identifier": 123, "name": "Nelly"}
        database.db[TableNames.TEST].insert_one(my_tuple)
        the_doc = database.retrieve_mapping(table_name=TableNames.TEST, key_fields="name", value_fields="identifier", filter_dict={})
        expected_doc = {"Nelly": 123}
        assert the_doc == expected_doc

    def test_retrieve_patient_identifiers_10(self):
        database = Database(execution=TestDatabase.execution)
        my_tuples = [{Resource.IDENTIFIER_: i, Record.VALUE_: i + random.randint(0, 100)} for i in range(0, 10)]
        my_original_tuples = copy.deepcopy(my_tuples)
        database.db[TableNames.TEST].insert_many(my_tuples)
        docs = database.retrieve_mapping(table_name=TableNames.TEST, key_fields=Record.VALUE_, value_fields=Resource.IDENTIFIER_, filter_dict={})
        expected_docs = {}
        for doc in my_original_tuples:
            expected_docs[doc[Record.VALUE_]] = doc[Resource.IDENTIFIER_]
        assert len(docs) == len(expected_docs), wrong_number_of_docs(len(expected_docs))
        for key, value in expected_docs.items():
            compare_tuples(original_tuple={key: value}, inserted_tuple={key: docs[key]})

    def test_retrieve_patient_identifiers_wrong_key(self):
        database = Database(execution=TestDatabase.execution)
        my_tuple = {"identifier": 123, "name": "Nelly"}
        database.db[TableNames.TEST].insert_one(my_tuple)
        with pytest.raises(KeyError):
            _ = database.retrieve_mapping(table_name=TableNames.TEST, key_fields="name2", value_fields="identifier", filter_dict={})

    def test_write_in_file(self):
        counter = Counter()
        my_tuples = [
            ResourceTest(identifier=NO_ID, counter=counter),
            ResourceTest(identifier=NO_ID, counter=counter),
            ResourceTest(identifier=NO_ID, counter=counter),
        ]
        my_tuples_as_json = [my_tuples[i].to_json() for i in range(len(my_tuples))]

        write_in_file(resource_list=my_tuples_as_json, current_working_dir=self.execution.working_dir_current, table_name=TableNames.TEST, is_feature=False, dataset_id=1, to_json=False)
        filepath = get_json_resource_file(current_working_dir=self.execution.working_dir_current, table_name=TableNames.TEST, dataset_id=1)
        assert os.path.exists(filepath) is True
        with jsonlines.open(filepath) as my_file:
            read_tuples = [obj for obj in my_file]
            assert len(my_tuples_as_json) == len(read_tuples), wrong_number_of_docs(len(my_tuples))
            assert my_tuples_as_json == read_tuples

    def test_write_in_file_no_resource(self):
        _ = Database(execution=TestDatabase.execution)
        my_tuples = []
        write_in_file(resource_list=my_tuples, current_working_dir=self.execution.working_dir_current, table_name=TableNames.TEST, is_feature=False, dataset_id=98, to_json=False)
        filepath = get_json_resource_file(current_working_dir=self.execution.working_dir_current, table_name=TableNames.TEST, dataset_id=98)
        assert os.path.exists(filepath) is False  # no file should have been created since there is no data to write

    def test_load_json_in_table(self):
        database = Database(execution=TestDatabase.execution)
        my_tuples = [
            {"name": "Nelly", "age": 26},
            {"name": "Julien", "age": 30, "city": "Lyon"},
            {"name": "Julien", "age": 30, "city": "Paris"},
            {"name": "Nelly", "age": 27, "job": "post-doc"},
            {"name": "Pietro", "age": -1, "country": "Italy"}
        ]
        my_original_tuples = copy.deepcopy(my_tuples)

        # I need to write the tuples in the working dir
        # because load_json_in_table() looks for files having the given table name in that directory
        write_in_file(resource_list=my_tuples, current_working_dir=self.execution.working_dir_current, table_name=TableNames.TEST, is_feature=False, dataset_id=97, to_json=False)
        database.load_json_in_table_for_tests(unique_variables=["name", "age"], dataset_id=97)

        docs = [doc for doc in database.db[TableNames.TEST].find({}).sort({"name": 1, "age": 1})]
        expected_docs = [my_original_tuples[2], my_original_tuples[0], my_original_tuples[3], my_original_tuples[4]]
        assert len(docs) == len(expected_docs), wrong_number_of_docs(len(expected_docs))
        for i in range(len(expected_docs)):
            compare_tuples(original_tuple=expected_docs[i], inserted_tuple=docs[i])

    def test_find_operation_1(self):
        database = Database(execution=TestDatabase.execution)
        my_tuples = [
            {"name": "Julien", "job": "engineer", "city": "Lyon"},
            {"name": "Nelly", "job": "bachelor student", "city": "Lyon"},
            {"name": "Nelly", "job": "master student", "city": "Lyon"},
            {"name": "Nelly", "job": "phd", "city": "Massy"},
            {"name": "Nelly", "job": "post-doc", "city": "Milano"},
            {"name": "Pietro", "job": "assistant prof.", "citizenship": "Italian"}
        ]
        my_original_tuples = copy.deepcopy(my_tuples)
        database.db[TableNames.TEST].insert_many(documents=my_tuples)

        # 1. no filter, no projection
        docs = [doc for doc in database.find_operation(table_name=TableNames.TEST, filter_dict={}, projection={}).sort({"name": 1})]
        assert len(docs) == len(my_original_tuples), wrong_number_of_docs(len(my_original_tuples))
        for i in range(len(my_original_tuples)):
            compare_tuples(original_tuple=my_original_tuples[i], inserted_tuple=docs[i])

        # 2. a single filter, no projection
        docs = [doc for doc in database.find_operation(table_name=TableNames.TEST, filter_dict={"city": "Lyon"}, projection={})]
        expected_docs = [my_original_tuples[0], my_original_tuples[1], my_original_tuples[2]]
        assert len(docs) == len(expected_docs), wrong_number_of_docs(len(expected_docs))
        for i in range(len(expected_docs)):
            compare_tuples(original_tuple=expected_docs[i], inserted_tuple=docs[i])

        # 3. a multi filter, no projection
        docs = [doc for doc in database.find_operation(table_name=TableNames.TEST, filter_dict={"name": "Nelly", "city": "Lyon"}, projection={})]
        expected_docs = [my_original_tuples[1], my_original_tuples[2]]
        assert len(docs) == len(expected_docs), wrong_number_of_docs(len(expected_docs))
        for i in range(len(expected_docs)):
            compare_tuples(original_tuple=expected_docs[i], inserted_tuple=docs[i])

        # 4. no filter, a single projection
        docs = [doc for doc in database.find_operation(table_name=TableNames.TEST, filter_dict={}, projection={"name": 1}).sort({"name": 1})]
        expected_docs = [{"name": doc["name"]} for doc in my_original_tuples]
        assert len(docs) == len(expected_docs), wrong_number_of_docs(len(expected_docs))
        for i in range(len(expected_docs)):
            compare_tuples(original_tuple=expected_docs[i], inserted_tuple=docs[i])

        # 5. a single filter, a single projection
        docs = [doc for doc in database.find_operation(table_name=TableNames.TEST, filter_dict={"city": "Lyon"}, projection={"name": 1})]
        expected_docs = [{"name": my_original_tuples[0]["name"]}, {"name": my_original_tuples[1]["name"]}, {"name": my_original_tuples[2]["name"]}]
        assert len(docs) == len(expected_docs), wrong_number_of_docs(len(expected_docs))
        for i in range(len(expected_docs)):
            compare_tuples(original_tuple=expected_docs[i], inserted_tuple=docs[i])

        # 6. a multi filter, a single projection
        docs = [doc for doc in database.find_operation(table_name=TableNames.TEST, filter_dict={"name": "Nelly", "city": "Lyon"}, projection={"name": 1})]
        expected_docs = [{"name": my_original_tuples[1]["name"]}, {"name": my_original_tuples[2]["name"]}]
        assert len(docs) == len(expected_docs), wrong_number_of_docs(len(expected_docs))
        for i in range(len(expected_docs)):
            compare_tuples(original_tuple=expected_docs[i], inserted_tuple=docs[i])

        # 7. no filter, a multi projection
        docs = [doc for doc in database.find_operation(table_name=TableNames.TEST, filter_dict={}, projection={"name": 1, "job": 1}).sort({"name": 1, "job": 1})]
        expected_docs = [{"name": doc["name"], "job": doc["job"]} for doc in my_original_tuples]
        assert len(docs) == len(expected_docs), wrong_number_of_docs(len(expected_docs))
        for i in range(len(expected_docs)):
            compare_tuples(original_tuple=expected_docs[i], inserted_tuple=docs[i])

        # 8. a single filter, a multi projection
        docs = [doc for doc in database.find_operation(table_name=TableNames.TEST, filter_dict={"city": "Lyon"}, projection={"name": 1, "job": 1})]
        expected_docs = [
            {"name": my_original_tuples[0]["name"], "job": my_original_tuples[0]["job"]},
            {"name": my_original_tuples[3]["name"], "job": my_original_tuples[1]["job"]},
            {"name": my_original_tuples[4]["name"], "job": my_original_tuples[2]["job"]}
        ]
        assert len(docs) == len(expected_docs), wrong_number_of_docs(len(expected_docs))
        for i in range(len(expected_docs)):
            compare_tuples(original_tuple=expected_docs[i], inserted_tuple=docs[i])

        # 9. a multi filter, a multi projection
        docs = [doc for doc in database.find_operation(table_name=TableNames.TEST, filter_dict={"name": "Nelly", "city": "Lyon"}, projection={"name": 1, "job": 1})]
        expected_docs = [
            {"name": my_original_tuples[3]["name"], "job": my_original_tuples[1]["job"]},
            {"name": my_original_tuples[4]["name"], "job": my_original_tuples[2]["job"]}
        ]
        assert len(docs) == len(expected_docs), wrong_number_of_docs(len(expected_docs))
        for i in range(len(expected_docs)):
            compare_tuples(original_tuple=expected_docs[i], inserted_tuple=docs[i])

    def test_count_documents(self):
        database = Database(execution=TestDatabase.execution)
        my_tuples = [
            {"name": "Julien", "from": "France"},
            {"name": "Nelly", "from": "France", "city": "Milano"},
            {"name": "Anna", "from": "Italy", "city": "Milano"}
        ]
        my_original_tuples = copy.deepcopy(my_tuples)
        database.db[TableNames.TEST].insert_many(documents=my_tuples)

        # 1. count with no filter
        current_count = database.count_documents(table_name=TableNames.TEST, filter_dict={})
        assert current_count == len(my_original_tuples), wrong_number_of_docs(len(my_original_tuples))

        # 2. count with single filter
        expected_docs = [my_original_tuples[0], my_original_tuples[1]]
        current_count = database.count_documents(table_name=TableNames.TEST, filter_dict={"from": "France"})
        assert current_count == len(expected_docs), wrong_number_of_docs(len(expected_docs))

        # 3. count with multi filter
        expected_docs = [my_original_tuples[1]]
        current_count = database.count_documents(table_name=TableNames.TEST, filter_dict={"from": "France", "name": "Nelly"})
        assert current_count == len(expected_docs), wrong_number_of_docs(len(expected_docs))

    def test_get_min_or_max_value(self):
        database = Database(execution=TestDatabase.execution)
        my_tuples = [
            {"value": 0.2},
            {"value": 1},
            {"value": 10},
            {"value": 2}
        ]
        database.db[TableNames.TEST].insert_many(documents=my_tuples)
        min_value = database.get_min_or_max_value(table_name=TableNames.TEST, field="value", sort_order=1)
        max_value = database.get_min_or_max_value(table_name=TableNames.TEST, field="value", sort_order=-1)
        assert min_value == 0.2, "The expected minimum value is 0.2."
        assert max_value == 10, "The expected maximum value is 10."

        database = Database(execution=TestDatabase.execution)
        my_tuples = [
            {"value": -0.2},
            {"value": -1},
            {"value": -10},
            {"value": -2}
        ]
        database.db[TableNames.TEST].insert_many(documents=my_tuples)
        min_value = database.get_min_or_max_value(table_name=TableNames.TEST, field="value", sort_order=1)
        max_value = database.get_min_or_max_value(table_name=TableNames.TEST, field="value", sort_order=-1)
        assert min_value == -10, "The expected minimum value is -10."
        assert max_value == -0.2, "The expected maximum value is -0.2."

    def test_get_min_or_max_resource_id(self):
        database = Database(execution=TestDatabase.execution)
        my_tuples = [
            {"value": 45},
            {"value": 54},
            {"value": 9},
            {"value": 154}
        ]
        database.db[TableNames.TEST].insert_many(documents=my_tuples)
        min_value = database.get_min_or_max_value(table_name=TableNames.TEST, field="value", sort_order=1)
        max_value = database.get_min_or_max_value(table_name=TableNames.TEST, field="value", sort_order=-1)
        assert min_value == 9, "The expected minimum value is 9."
        assert max_value == 154, "The expected maximum value is 154."

    def test_get_max_resource_counter_id(self):
        database = Database(execution=TestDatabase.execution)
        my_resources_1 = [
            {"identifier": 1, "name": "Anna"},
            {"identifier": 4, "name": "Julien"},
            {"identifier": 999, "name": "Nelly"},
        ]
        my_resources_2 = [
            {"identifier": 2, "name": "Anna"},
            {"identifier": 100, "name": "Nelly"},
            {"identifier": 998, "name": "Pietro"},
        ]
        # as an exception, we insert into LABORATORY_RECORD, not in TableNames.TEST,
        # because the method is made to set up resource counter and is expected to work on the
        # TableNames table names only
        database.db[TableNames.RECORD].insert_many(documents=my_resources_1)
        database.db[TableNames.FEATURE].insert_many(documents=my_resources_2)
        max_resource_id = database.get_max_resource_counter_id()
        assert max_resource_id == 999, "The expected max resource id is 999."
