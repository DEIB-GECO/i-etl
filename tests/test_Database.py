import unittest

import pytest

from database.Database import Database
from database.Execution import Execution
from utils.constants import TEST_DB_NAME, TEST_TABLE_NAME
from utils.constants import DEFAULT_DB_CONNECTION


class TestDatabase(unittest.TestCase):
    execution = Execution(TEST_DB_NAME)

    def tearDown(self):
        # after each test, get back to the original test configuration
        args = {
            "db_connection": DEFAULT_DB_CONNECTION,
            "db_drop": True
        }
        TestDatabase.execution.set_up(args, False)

    def test_check_server_is_up(self):
        # test with the correct (default) string
        _ = Database(execution=TestDatabase.execution)  # this should return no exception (successful connection)
        # database.close()

        # test with a wrong connection string
        args = {
            "db_connection": "a_random_string"
        }
        TestDatabase.execution.set_up(args, False)
        with pytest.raises(ConnectionError):
            _ = Database(execution=TestDatabase.execution)  # this should return an exception (broken connection) because check_server_is_up() will return one
        # database.close()

    def test_drop(self):
        # check that, after drop, no db with the provided name exists
        # TODO Nelly assert
        pass
        # test also that no drop does not erase the db

    def test_reset(self):
        # create a test database
        # and add only one triple to be sure that the db is created
        database = Database(execution=TestDatabase.execution)
        database.insert_one_tuple(table_name=TEST_TABLE_NAME, one_tuple={"id": "1", "name": "Alice Doe"})
        assert database.db_exists(TEST_DB_NAME) is True
        database.drop_db()
        # check the DB does not exist anymore after drop
        assert database.db_exists(TEST_DB_NAME) is False
        # database.close()

    def test_insert_many_tuples(self):
        args = {
            "db_drop": True
        }
        TestDatabase.execution.set_up(args, False)

        database = Database(execution=TestDatabase.execution)
        tuples = [{"id": 1, "name": "Louise", "country": "FR", "job": "PhD student"},
                  {"id": 2, "name": "Francesca", "country": "IT", "university": True},
                  {"id": 3, "name": "Martin", "country": "DE", "age": 26}]
        database.insert_many_tuples(table_name=TEST_TABLE_NAME, tuples=tuples)
        docs = []
        for doc in database.db[TEST_TABLE_NAME].find({}):
            docs.append(doc)

        assert len(tuples) == len(docs)
        # TODO Nelly: test more
