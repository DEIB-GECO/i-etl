import unittest

from src.config.BetterConfig import BetterConfig
from src.database.Database import Database
from src.utils.constants import TEST_DB_NAME, TEST_TABLE_NAME


class TestDatabase(unittest.TestCase):
    def test_check_server_is_up(self):
        config = BetterConfig()

        # test with the correct (default) string
        config.set_db_name(TEST_DB_NAME)
        database = Database(config)
        self.assertTrue(database.check_server_is_up())
        # database.close()

        # test with a wrong connection string
        config.set_db_connection("a_random_connection_string")
        config.set_db_name(TEST_DB_NAME)
        database = Database(config)
        self.assertFalse(database.check_server_is_up())
        # database.close()

        TestDatabase.NB_TESTS_RUN = TestDatabase.NB_TESTS_RUN + 1

    def test_drop(self):
        # check that, after drop, no db with the provided name exists
        config = BetterConfig()
        config.set_db_name(TEST_DB_NAME)
        # TODO Nelly assert

    def test_reset(self):
        config = BetterConfig()
        config.set_db_name(TEST_DB_NAME)
        # create a test database
        # and add only one triple to be sure that the db is created
        database = Database(config)
        database.insert_one_tuple(TEST_TABLE_NAME, { "id": "1", "name": "Alice Doe"})
        list_dbs = database.client.list_databases()
        found = False
        for db in list_dbs:
            if db['name'] == TEST_DB_NAME:
                found = True
        self.assertTrue(found)
        database.drop_db()
        # check the DB does not exist anymore after drop
        list_dbs = database.client.list_databases()
        found = False
        for db in list_dbs:
            if db['name'] == TEST_DB_NAME:
                found = True
        self.assertFalse(found)
        # database.close()

        TestDatabase.NB_TESTS_RUN = TestDatabase.NB_TESTS_RUN + 1

    def my_test(self):

        # four_thousands_hospitals = []
        # for i in range(0, 3):
        #     four_thousands_hospitals.append(Hospital(str(i), "Buzzi " + str(randrange(10))).to_json())

        # an_hospital = Hospital("6", "Buzzi 5")
        # an_hospital2 = Hospital("2", "BUZZI 2")
        # hospitals = [an_hospital, an_hospital2]
        # json_hospitals = [hospital.to_json() for hospital in hospitals]
        # upserted_id = self.database.upsert_one_tuple(table_name=TableNames.HOSPITAL.value, filter_dict={"name": an_hospital.name}, one_tuple=an_hospital.to_json())
        # (upserted_document, is_insert) = self.database.upsert_one_tuple(table_name=TableNames.HOSPITAL.value, unique_variables=["name"], one_tuple=an_hospital2.to_json())
        # upserted_id = self.database.upsert_one_tuple(table_name=TableNames.HOSPITAL.value, filter_dict={"name": an_hospital3.name}, one_tuple=an_hospital3.to_json())
        # self.database.upsert_many_tuples(table_name=TableNames.HOSPITAL.value, unique_variables=["name"], tuples=four_thousands_hospitals)
        pass

    def test_insert_many_tuples(self):
        config = BetterConfig()
        config.set_db_name(TEST_DB_NAME)
        config.set_db_drop("True")

        database = Database(config)
        tuples = [{"id": 1, "name": "Louise", "country": "FR", "job": "PhD student"},
                  {"id": 2, "name": "Francesca", "country": "IT", "university": True},
                  {"id": 3, "name": "Martin", "country": "DE", "age": 26}]
        database.insert_many_tuples(TEST_TABLE_NAME, tuples)
        docs = []
        for doc in database.db[TEST_TABLE_NAME].find({}):
            docs.append(doc)

        self.assertEqual(len(tuples), len(docs))
        # TODO Nelly: test more
