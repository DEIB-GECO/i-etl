import copy
import json
import os.path
import random
import unittest

import pytest

from database.Database import Database
from database.Execution import Execution
from datatypes.Identifier import Identifier
from datatypes.PatientAnonymizedIdentifier import PatientAnonymizedIdentifier
from datatypes.ResourceIdentifier import ResourceIdentifier
from enums.HospitalNames import HospitalNames
from profiles.ResourceTest import ResourceTest
from utils.Counter import Counter
from enums.TableNames import TableNames
from enums.UpsertPolicy import UpsertPolicy
from utils.constants import TEST_DB_NAME, NO_ID
from utils.constants import DEFAULT_DB_CONNECTION
from utils.setup_logger import log
from utils.utils import compare_tuples, wrong_number_of_docs, write_in_file


class TestDatabase(unittest.TestCase):
    execution = Execution(TEST_DB_NAME)

    def setUp(self):
        # before each test, get back to the original test configuration
        args = {
            Execution.DB_CONNECTION_KEY: DEFAULT_DB_CONNECTION,
            Execution.DB_DROP_KEY: True,
            Execution.DB_UPSERT_POLICY_KEY: UpsertPolicy.DO_NOTHING
        }
        TestDatabase.execution.set_up(args, False)

    def test_check_server_is_up(self):
        # test with the correct (default) string
        _ = Database(execution=TestDatabase.execution)  # this should return no exception (successful connection)
        # database.close()

        # test with a wrong connection string
        args = {
            Execution.DB_CONNECTION_KEY: "a_random_string"
        }
        TestDatabase.execution.set_up(args, False)
        with pytest.raises(ConnectionError):
            _ = Database(execution=TestDatabase.execution)  # this should return an exception (broken connection) because check_server_is_up() will return one

    def test_drop(self):
        # create a test database
        # and add only one triple to be sure that the db is created
        database = Database(execution=TestDatabase.execution)
        database.db[TableNames.TEST].insert_one(document={"id": "1", "name": "Alice Doe"})
        assert database.db_exists(TEST_DB_NAME) is True, "The database does not exist."
        database.drop_db()
        # check the DB does not exist anymore after drop
        assert database.db_exists(TEST_DB_NAME) is False, "The database has not been dropped."

    def test_no_drop(self):
        # test that the database is not dropped when the user asks for it
        # 1. create a db with a single table and only one tuple
        database = Database(execution=TestDatabase.execution)
        database.db[TableNames.TEST].insert_one(document={"id": "1", "name": "Alice Doe"})
        assert database.db_exists(TEST_DB_NAME) is True, "The database does not exist."  # we make sure that the db exists

        # 2. we create a new instance to the same database, with drop_db=False
        args = {
            Execution.DB_DROP_KEY: False
        }
        TestDatabase.execution.set_up(args, False)
        database = Database(execution=TestDatabase.execution)

        # 3. check that the database still exists (i.e., the constructor did not reset it)
        assert database.db_exists(TEST_DB_NAME) is True, "The database does not exist."

        # 4. check that the database still exists, even after a drop
        database.drop_db()  # this should do nothing as we explicitly specified to not drop the db
        assert database.db_exists(TEST_DB_NAME) is True, "The database has not been dropped."

    def test_insert_empty_tuple(self):
        database = Database(TestDatabase.execution)
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
        database = Database(TestDatabase.execution)
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
        # and they need to be executed in order to test properly whether upsert works
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

        # 3. We upsert the same tuple with a different age with DO_NOTHING:
        # no new tuple should be inserted (that document already exists)
        # and the current one should not be updated
        args = {
            Execution.DB_UPSERT_POLICY_KEY: UpsertPolicy.DO_NOTHING,
            Execution.DB_DROP_KEY: False  # very important: we should not reset the database, otherwise we cannot test whether upsert works
        }
        TestDatabase.execution.set_up(args_as_dict=args, setup_data_files=False)
        database = Database(execution=TestDatabase.execution)
        my_tuple_age = {"name": "Nelly", "age": 27, "city": "Lyon"}  # same as my_tuple but with a different age and a new field
        database.upsert_one_tuple(table_name=TableNames.TEST, unique_variables=["name"], one_tuple=my_tuple_age)
        docs = [doc for doc in database.db[TableNames.TEST].find({})]
        assert len(docs) == 1, wrong_number_of_docs(1)
        compare_tuples(original_tuple=my_original_tuple, inserted_tuple=docs[0])  # we check that the tuple did not change (DO_NOTHING)

        # 4. We upsert the same tuple with a different age with REPLACE:
        # no new tuple should be inserted (that document already exists)
        # the tuple should be updated
        args = {
            Execution.DB_UPSERT_POLICY_KEY: UpsertPolicy.REPLACE,
            Execution.DB_DROP_KEY: False
        }
        TestDatabase.execution.set_up(args_as_dict=args, setup_data_files=False)
        database = Database(execution=TestDatabase.execution)
        my_tuple_age = {"name": "Nelly", "age": 27, "city": "Lyon"}  # same as my_tuple but with a different age and a new field city
        my_original_tuple_age = copy.deepcopy(my_tuple_age)
        database.upsert_one_tuple(table_name=TableNames.TEST, unique_variables=["name"], one_tuple=my_tuple_age)
        docs = [doc for doc in database.db[TableNames.TEST].find({})]
        assert len(docs) == 1, wrong_number_of_docs(1)
        compare_tuples(original_tuple=my_original_tuple_age, inserted_tuple=docs[0])  # we check that the tuple did change (REPLACE)

        # 5. We upsert a new tuple (different name) with DO NOTHING:
        # a new tuple should be inserted (that document does not already exist)
        # the former one should not have changed
        args = {
            Execution.DB_UPSERT_POLICY_KEY: UpsertPolicy.DO_NOTHING,
            Execution.DB_DROP_KEY: False
        }
        TestDatabase.execution.set_up(args_as_dict=args, setup_data_files=False)
        database = Database(execution=TestDatabase.execution)
        my_new_tuple = {"name": "Julien", "age": "30"}  # a new tuple (with a different key)
        my_original_new_tuple = copy.deepcopy(my_new_tuple)
        my_original_tuples = [my_original_new_tuple, my_original_tuple_age]  # Julien comes first in alphabetical order
        database.upsert_one_tuple(table_name=TableNames.TEST, unique_variables=["name"], one_tuple=my_new_tuple)
        docs = [doc for doc in database.db[TableNames.TEST].find({}).sort({"name": 1})]
        assert len(docs) == 2, wrong_number_of_docs(2)
        for i in range(len(my_original_tuples)):
            compare_tuples(original_tuple=my_original_tuples[i], inserted_tuple=docs[i])

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

        # 3. We upsert the same tuple with a different age with DO_NOTHING:
        # a new tuple is added because no existing tuple has this combination (name, age)
        # and the current one should not be updated because no tuple should have matched
        args = {
            Execution.DB_UPSERT_POLICY_KEY: UpsertPolicy.DO_NOTHING,
            Execution.DB_DROP_KEY: False
            # very important: we should not reset the database, otherwise we cannot test whether upsert works
        }
        TestDatabase.execution.set_up(args_as_dict=args, setup_data_files=False)
        database = Database(execution=TestDatabase.execution)
        my_tuple_age = {"name": "Nelly", "age": 27, "city": "Lyon"}  # same as my_tuple but with a different age and a new field
        my_original_tuple_age = copy.deepcopy(my_tuple_age)
        database.upsert_one_tuple(table_name=TableNames.TEST, unique_variables=["name", "age"], one_tuple=my_tuple_age)
        docs = [doc for doc in database.db[TableNames.TEST].find({}).sort({"name": 1, "age": 1})]
        expected_docs = [my_original_tuple, my_original_tuple_age]
        assert len(docs) == len(expected_docs), wrong_number_of_docs(len(expected_docs))
        for i in range(len(expected_docs)):
            compare_tuples(original_tuple=expected_docs[i], inserted_tuple=docs[i])

        # 4. We upsert the same tuple with a different age with REPLACE:
        # a new tuple is added because no existing tuple has this combination (name, age)
        # and the current one should not be updated because no tuple should have matched
        args = {
            Execution.DB_UPSERT_POLICY_KEY: UpsertPolicy.REPLACE,
            Execution.DB_DROP_KEY: False
        }
        TestDatabase.execution.set_up(args_as_dict=args, setup_data_files=False)
        database = Database(execution=TestDatabase.execution)
        my_tuple_age = {"name": "Nelly", "age": 27, "city": "Lyon"}  # same as my_tuple but with a different age and a new field city
        my_original_tuple_age = copy.deepcopy(my_tuple_age)
        database.upsert_one_tuple(table_name=TableNames.TEST, unique_variables=["name", "age"], one_tuple=my_tuple_age)
        docs = [doc for doc in database.db[TableNames.TEST].find({}).sort({"name": 1, "age": 1})]
        expected_docs = [my_original_tuple, my_original_tuple_age]
        assert len(docs) == 2, wrong_number_of_docs(2)
        for i in range(len(expected_docs)):
            compare_tuples(original_tuple=expected_docs[i], inserted_tuple=docs[i])

        # 5. We upsert a similar tuple (same name and age but different city) with DO NOTHING:
        # no new tuple should be inserted (that document does already exist)
        # the former one should not have changed (because DO_NOTHING)
        args = {
            Execution.DB_UPSERT_POLICY_KEY: UpsertPolicy.DO_NOTHING,
            Execution.DB_DROP_KEY: False
        }
        TestDatabase.execution.set_up(args_as_dict=args, setup_data_files=False)
        database = Database(execution=TestDatabase.execution)
        my_new_tuple = {"name": "Nelly", "age": 26, "city": "Lyon"}
        database.upsert_one_tuple(table_name=TableNames.TEST, unique_variables=["name", "age"], one_tuple=my_new_tuple)
        docs = [doc for doc in database.db[TableNames.TEST].find({}).sort({"name": 1, "age": 1})]
        expected_docs = [my_original_tuple, my_original_tuple_age]  # Julien comes first in alphabetical order
        assert len(docs) == len(expected_docs), wrong_number_of_docs(len(expected_docs))
        for i in range(len(expected_docs)):
            compare_tuples(original_tuple=expected_docs[i], inserted_tuple=docs[i])

        # 6. We upsert a similar tuple (same name and age but different city) with REPLACE:
        # no new tuple should be inserted (that document does already exist)
        # the former one should have changed (because REPLACE)
        args = {
            Execution.DB_UPSERT_POLICY_KEY: UpsertPolicy.REPLACE,
            Execution.DB_DROP_KEY: False
        }
        TestDatabase.execution.set_up(args_as_dict=args, setup_data_files=False)
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
        database.upsert_one_batch_of_tuples(table_name=TableNames.TEST, unique_variables=["name"], the_batch=my_batch)
        docs = [doc for doc in database.db[TableNames.TEST].find({})]
        assert len(docs) == 2, wrong_number_of_docs(2)
        for i in range(len(my_original_batch)):
            compare_tuples(original_tuple=my_original_batch[i], inserted_tuple=docs[i])

        # 2. upsert a batch with one similar tuple, and one different
        # with upsert policy being DO_NOTHING
        my_batch_2 = [{"name": "Nelly", "age": 27}, {"name": "Pietro", "city": "Milano"}]
        database.upsert_one_batch_of_tuples(table_name=TableNames.TEST, unique_variables=["name"], the_batch=my_batch_2)
        expected_docs = [my_original_batch[1], my_original_batch[0], my_batch_2[1]]  # ordered by name
        docs = [doc for doc in database.db[TableNames.TEST].find({}).sort({"name": 1})]
        assert len(docs) == len(expected_docs), wrong_number_of_docs(len(expected_docs))
        for i in range(len(expected_docs)):
            compare_tuples(original_tuple=expected_docs[i], inserted_tuple=docs[i])

        # 3. upsert a batch with one similar tuple and one different
        # with upsert policy being REPLACE
        args = {
            Execution.DB_UPSERT_POLICY_KEY: UpsertPolicy.REPLACE,
            Execution.DB_DROP_KEY: False  # do not reset the database to keep the already existing upserted tuples
        }
        TestDatabase.execution.set_up(args_as_dict=args, setup_data_files=False)
        database = Database(TestDatabase.execution)
        my_batch_3 = [{"name": "Nelly", "age": 27}, {"name": "Anna", "citizenship": "Italian"}]
        database.upsert_one_batch_of_tuples(table_name=TableNames.TEST, unique_variables=["name"], the_batch=my_batch_3)
        expected_docs = [my_batch_3[1], my_original_batch[1], my_batch_3[0], my_batch_2[1]]  # ordered by name
        docs = [doc for doc in database.db[TableNames.TEST].find({}).sort({"name": 1})]
        assert len(docs) == len(expected_docs), wrong_number_of_docs(len(expected_docs))
        for i in range(len(expected_docs)):
            compare_tuples(original_tuple=expected_docs[i], inserted_tuple=docs[i])

    def test_upsert_one_batch_of_tuples_multi_key(self):
        database = Database(execution=TestDatabase.execution)
        my_tuples = [{"name": "Nelly", "age": 26}, {"name": "Julien", "age": 30}]
        my_original_tuples = copy.deepcopy(my_tuples)
        database.upsert_one_batch_of_tuples(table_name=TableNames.TEST, unique_variables=["name", "age"], the_batch=my_tuples)

        # 1. first, we check that the initial upserted tuple has been correctly inserted
        docs = [doc for doc in database.db[TableNames.TEST].find({})]
        assert len(docs) == len(my_original_tuples), wrong_number_of_docs(len(my_original_tuples))
        for i in range(len(my_original_tuples)):
            compare_tuples(original_tuple=my_original_tuples[i], inserted_tuple=docs[i])

        # 2. We upsert the exact same batch of tuples:
        # there should be no duplicate
        my_original_tuples_2 = copy.deepcopy(my_original_tuples)
        database.upsert_one_batch_of_tuples(table_name=TableNames.TEST, unique_variables=["name", "age"], the_batch=my_original_tuples)
        docs = [doc for doc in database.db[TableNames.TEST].find({})]
        assert len(docs) == len(my_original_tuples_2), wrong_number_of_docs(len(my_original_tuples_2))
        for i in range(len(my_original_tuples_2)):
            compare_tuples(original_tuple=my_original_tuples_2[i], inserted_tuple=docs[i])

        # 3. We upsert one identical and one different tuple (on the age) with DO_NOTHING:
        # only one new tuple is added because no existing tuple has its combination (name, age)
        # and the current tuples should not be updated because DO_NOTHING
        args = {
            Execution.DB_UPSERT_POLICY_KEY: UpsertPolicy.DO_NOTHING,
            Execution.DB_DROP_KEY: False
            # very important: we should not reset the database, otherwise we cannot test whether upsert works
        }
        TestDatabase.execution.set_up(args_as_dict=args, setup_data_files=False)
        database = Database(execution=TestDatabase.execution)
        my_tuples_age = [{"name": "Nelly", "age": 26, "city": "Lyon"}, {"name": "Anna", "age": -1, "city": "Milano"}]  # same as my_tuple but with a different age and a new field
        my_original_tuples_age = copy.deepcopy(my_tuples_age)
        database.upsert_one_batch_of_tuples(table_name=TableNames.TEST, unique_variables=["name", "age"], the_batch=my_tuples_age)
        docs = [doc for doc in database.db[TableNames.TEST].find({}).sort({"name": 1, "age": 1})]
        expected_docs = [my_original_tuples_age[1], my_original_tuples[1], my_original_tuples[0]]
        assert len(docs) == len(expected_docs), wrong_number_of_docs(len(expected_docs))
        for i in range(len(expected_docs)):
            compare_tuples(original_tuple=expected_docs[i], inserted_tuple=docs[i])

        # 4. We upsert one (out of 2) same tuple with a different city with REPLACE:
        # a new tuple is added because no existing tuple has this combination (name, age)
        # and the current ones should not be updated because no tuple should have matched
        args = {
            Execution.DB_UPSERT_POLICY_KEY: UpsertPolicy.REPLACE,
            Execution.DB_DROP_KEY: False
        }
        TestDatabase.execution.set_up(args_as_dict=args, setup_data_files=False)
        database = Database(execution=TestDatabase.execution)
        my_tuples_age_2 = [{"name": "Nelly", "age": 26, "city": "Paris"}, {"name": "Pietro", "age": -1, "city": "Milano"}]
        my_original_tuple_age_2 = copy.deepcopy(my_tuples_age_2)
        database.upsert_one_batch_of_tuples(table_name=TableNames.TEST, unique_variables=["name", "age"], the_batch=my_tuples_age_2)
        docs = [doc for doc in database.db[TableNames.TEST].find({}).sort({"name": 1, "age": 1})]
        expected_docs = [my_original_tuples_age[1], my_original_tuples[1], my_original_tuple_age_2[0], my_original_tuple_age_2[1]]
        assert len(docs) == len(expected_docs), wrong_number_of_docs(len(expected_docs))
        for i in range(len(expected_docs)):
            compare_tuples(original_tuple=expected_docs[i], inserted_tuple=docs[i])

    def test_upsert_one_batch_of_tuples_unknown_key(self):
        # 1 key is unknown
        database = Database(execution=TestDatabase.execution)
        my_tuples = [{"name": "Nelly", "age": 26}, {"name": "Julien", "age": 30}]

        # this will not return an exception (value error) because the upsert contains an unknown key (city does not exist in my_tuple)
        # instead, it will return an empty dictionary, showing that the key "city" could not be used
        used_unique_variables = database.upsert_one_batch_of_tuples(table_name=TableNames.TEST, unique_variables=["city"], the_batch=my_tuples)
        assert len(used_unique_variables.keys()) == 0

        # 1 key is known, 1 key is unknown
        my_tuples = [{"name": "Nelly", "age": 26}, {"name": "Julien", "age": 30}]

        # similarly, this will return a dict with only "name" has a key, "city" will not appear
        used_unique_variables = database.upsert_one_batch_of_tuples(table_name=TableNames.TEST, unique_variables=["name", "city"], the_batch=my_tuples)
        assert len(used_unique_variables.keys()) == 1
        assert "name" in used_unique_variables

    def test_retrieve_resource_identifiers_1(self):
        database = Database(TestDatabase.execution)
        my_id = ResourceIdentifier(id_value="123")
        my_tuple = {"identifier": my_id.to_json(), "name": "Nelly"}
        database.db[TableNames.TEST].insert_one(my_tuple)
        the_doc = database.retrieve_identifiers(table_name=TableNames.TEST, projection="name")
        expected_doc = {"Nelly": "123"}
        assert the_doc == expected_doc

    def test_retrieve_resource_identifiers_10(self):
        database = Database(TestDatabase.execution)
        my_tuples = [{"identifier": ResourceIdentifier(id_value=str(i)).to_json(), "value": i+random.randint(0, 100)} for i in range(0, 10)]
        my_original_tuples = copy.deepcopy(my_tuples)
        database.db[TableNames.TEST].insert_many(my_tuples)
        docs = database.retrieve_identifiers(table_name=TableNames.TEST, projection="value")
        expected_docs = {}
        for doc in my_original_tuples:
            expected_docs[doc["value"]] = doc["identifier"]["value"]
        assert len(docs) == len(expected_docs), wrong_number_of_docs(len(expected_docs))
        for key, value in expected_docs.items():
            compare_tuples(original_tuple={key: value}, inserted_tuple={key: docs[key]})

    def test_retrieve_resource_identifiers_wrong_key(self):
        database = Database(TestDatabase.execution)
        my_id = ResourceIdentifier(id_value="123")
        my_tuple = {"identifier": my_id.to_json(), "name": "Nelly"}
        database.db[TableNames.TEST].insert_one(my_tuple)
        with pytest.raises(KeyError):
            _ = database.retrieve_identifiers(table_name=TableNames.TEST, projection="name2")

    def test_retrieve_patient_identifiers_1(self):
        database = Database(TestDatabase.execution)
        my_id = PatientAnonymizedIdentifier(id_value="123", hospital_name=HospitalNames.TEST_H1)
        my_tuple = {"identifier": my_id.to_json(), "name": "Nelly"}
        database.db[TableNames.TEST].insert_one(my_tuple)
        the_doc = database.retrieve_identifiers(table_name=TableNames.TEST, projection="name")
        expected_doc = {"Nelly": my_id.value}
        assert the_doc == expected_doc

    def test_retrieve_patient_identifiers_10(self):
        database = Database(TestDatabase.execution)
        my_tuples = [{"identifier": PatientAnonymizedIdentifier(id_value=str(i), hospital_name=HospitalNames.TEST_H1).to_json(), "value": i+random.randint(0, 100)} for i in range(0, 10)]
        my_original_tuples = copy.deepcopy(my_tuples)
        database.db[TableNames.TEST].insert_many(my_tuples)
        docs = database.retrieve_identifiers(table_name=TableNames.TEST, projection="value")
        expected_docs = {}
        for doc in my_original_tuples:
            expected_docs[doc["value"]] = doc["identifier"]["value"]
        assert len(docs) == len(expected_docs), wrong_number_of_docs(len(expected_docs))
        for key, value in expected_docs.items():
            compare_tuples(original_tuple={key: value}, inserted_tuple={key: docs[key]})

    def test_retrieve_patient_identifiers_wrong_key(self):
        database = Database(TestDatabase.execution)
        my_id = PatientAnonymizedIdentifier(id_value="123", hospital_name=HospitalNames.TEST_H1)
        my_tuple = {"identifier": my_id.to_json(), "name": "Nelly"}
        database.db[TableNames.TEST].insert_one(my_tuple)
        with pytest.raises(KeyError):
            _ = database.retrieve_identifiers(table_name=TableNames.TEST, projection="name2")

    def test_write_in_file(self):
        counter = Counter()
        my_tuples = [
            ResourceTest(id_value=NO_ID, resource_type=TableNames.TEST, counter=counter, hospital_name=HospitalNames.TEST_H1),
            ResourceTest(id_value=NO_ID, resource_type=TableNames.TEST, counter=counter, hospital_name=HospitalNames.TEST_H1),
            ResourceTest(id_value=NO_ID, resource_type=TableNames.TEST, counter=counter, hospital_name=HospitalNames.TEST_H1),
        ]
        my_tuples_as_json = [my_tuples[i].to_json() for i in range(len(my_tuples))]

        write_in_file(resource_list=my_tuples, current_working_dir=self.execution.working_dir_current, table_name=TableNames.TEST, count=1)
        filepath = os.path.join(TestDatabase.execution.working_dir_current, TableNames.TEST+"1.json")
        assert os.path.exists(filepath) is True
        with open(filepath) as my_file:
            read_tuples = json.load(my_file)
            assert len(my_tuples_as_json) == len(read_tuples), wrong_number_of_docs(len(my_tuples))
            assert my_tuples_as_json == read_tuples

    def test_write_in_file_no_resource(self):
        database = Database(TestDatabase.execution)
        my_tuples = []
        write_in_file(resource_list=my_tuples, current_working_dir=self.execution.working_dir_current, table_name=TableNames.TEST, count=2)
        filepath = os.path.join(TestDatabase.execution.working_dir_current, f"{TableNames.TEST}2.json")
        assert os.path.exists(filepath) is False  # no file should have been created since there is no data to write

    def test_load_json_in_table(self):
        database = Database(TestDatabase.execution)
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
        filepath = os.path.join(TestDatabase.execution.working_dir_current, f"{TableNames.TEST}1.json")
        with open(filepath, 'w') as f:
            json.dump(my_tuples, f)

        database.load_json_in_table(table_name=TableNames.TEST, unique_variables=["name", "age"])

        docs = [doc for doc in database.db[TableNames.TEST].find({}).sort({"name": 1, "age": 1})]
        expected_docs = [my_original_tuples[1], my_original_tuples[0], my_original_tuples[3], my_original_tuples[4]]
        assert len(docs) == len(expected_docs), wrong_number_of_docs(len(expected_docs))
        for i in range(len(expected_docs)):
            compare_tuples(original_tuple=expected_docs[i], inserted_tuple=docs[i])

    def test_find_operation_1(self):
        database = Database(TestDatabase.execution)
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
        database = Database(TestDatabase.execution)
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
        database = Database(TestDatabase.execution)
        my_tuples = [
            {"value": 0.2},
            {"value": 1},
            {"value": 10},
            {"value": 2}
        ]
        database.db[TableNames.TEST].insert_many(documents=my_tuples)
        min_value = database.get_min_or_max_value(table_name=TableNames.TEST, field="value", sort_order=1, from_string=False)
        max_value = database.get_min_or_max_value(table_name=TableNames.TEST, field="value", sort_order=-1, from_string=False)
        assert min_value == 0.2, "The expected minimum value is 0.2."
        assert max_value == 10, "The expected maximum value is 10."

        database = Database(TestDatabase.execution)
        my_tuples = [
            {"value": -0.2},
            {"value": -1},
            {"value": -10},
            {"value": -2}
        ]
        database.db[TableNames.TEST].insert_many(documents=my_tuples)
        min_value = database.get_min_or_max_value(table_name=TableNames.TEST, field="value", sort_order=1, from_string=False)
        max_value = database.get_min_or_max_value(table_name=TableNames.TEST, field="value", sort_order=-1, from_string=False)
        assert min_value == -10, "The expected minimum value is -10."
        assert max_value == -0.2, "The expected maximum value is -0.2."

    def test_get_min_or_max_resource_id(self):
        database = Database(TestDatabase.execution)
        my_tuples = [
            {"value": ResourceIdentifier(id_value="45").value},
            {"value": ResourceIdentifier(id_value="54").value},
            {"value": ResourceIdentifier(id_value="9").value},
            {"value": ResourceIdentifier(id_value="154").value}
        ]
        database.db[TableNames.TEST].insert_many(documents=my_tuples)
        min_value = database.get_min_or_max_value(table_name=TableNames.TEST, field="value", sort_order=1, from_string=True)
        max_value = database.get_min_or_max_value(table_name=TableNames.TEST, field="value", sort_order=-1, from_string=True)
        assert min_value == 9, "The expected minimum value is 9."
        assert max_value == 154, "The expected maximum value is 154."

    def test_get_min_or_max_patient_id(self):
        database = Database(TestDatabase.execution)
        my_tuples = [
            {"value": PatientAnonymizedIdentifier(id_value="45", hospital_name=HospitalNames.TEST_H1).value},
            {"value": PatientAnonymizedIdentifier(id_value="54", hospital_name=HospitalNames.TEST_H1).value},
            {"value": PatientAnonymizedIdentifier(id_value="9", hospital_name=HospitalNames.TEST_H1).value},
            {"value": PatientAnonymizedIdentifier(id_value="154", hospital_name=HospitalNames.TEST_H1).value}
        ]
        database.db[TableNames.TEST].insert_many(documents=my_tuples)
        min_value = database.get_min_or_max_value(table_name=TableNames.TEST, field="value", sort_order=1, from_string=True)
        max_value = database.get_min_or_max_value(table_name=TableNames.TEST, field="value", sort_order=-1, from_string=True)
        assert min_value == 9, "The expected minimum value is 9."
        assert max_value == 154, "The expected maximum value is 154."

    def test_get_max_resource_counter_id(self):
        # we need to check both with Resource and AnonymizedPatient identifiers
        # to be sure that both identifiers can be parsed accordingly and take the max value among them
        database = Database(TestDatabase.execution)
        my_resources_1 = [
            {"identifier": {"value": ResourceIdentifier(id_value="1").value}, "name": "Anna"},
            {"identifier": {"value": ResourceIdentifier(id_value="4").value}, "name": "Julien"},
            {"identifier": {"value": PatientAnonymizedIdentifier(id_value="999", hospital_name=HospitalNames.TEST_H1).value}, "name": "Nelly"},
        ]
        my_resources_2 = [
            {"identifier": {"value": ResourceIdentifier(id_value="2").value}, "name": "Anna"},
            {"identifier": {"value": PatientAnonymizedIdentifier(id_value="8", hospital_name=HospitalNames.TEST_H1).value}, "name": "Julien"},
            {"identifier": {"value": ResourceIdentifier(id_value="100").value}, "name": "Nelly"},
            {"identifier": {"value": ResourceIdentifier(id_value="1000b").value}, "name": "Pietro"},
        ]
        # as an exception, we insert into LABORATORY_RECORD, not in TableNames.TEST,
        # because the method is made to set up resource counter and is expected to work on the
        # TableNames table names only
        database.db[TableNames.LABORATORY_RECORD].insert_many(documents=my_resources_1)
        database.db[TableNames.LABORATORY_FEATURE].insert_many(documents=my_resources_2)
        max_resource_id = database.get_max_resource_counter_id()
        assert max_resource_id == 999, "The expected max resource id is 999."
