from unittest import TestCase

from config.BetterConfig import BetterConfig
from database.Database import Database
from utils.constants import TEST_DB_NAME, TEST_TABLE_NAME
from utils.setup_logger import log


class TestDatabase(TestCase):
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

    def test_drop(self):
        # check that, after drop, no db with the provided name exists
        config = BetterConfig()
        config.set_db_name(TEST_DB_NAME)

    def test_reset(self):
        config = BetterConfig()
        log.debug(config.to_json())
        # create a test database
        # and add only one triple to be sure that the db is created
        database = Database(config)
        log.debug(database.client)
        log.debug(database.db)
        database.insert_one_tuple(TEST_TABLE_NAME, { "id": "1", "name": "Alice Doe"})
        list_dbs = database.client.list_databases()
        found = False
        for db in list_dbs:
            log.debug(db)
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

    def my_test(self):

        # four_thousands_hospitals = []
        # for i in range(0, 3):
        #     four_thousands_hospitals.append(Hospital(str(i), "Buzzi " + str(randrange(10))).to_json())

        # log.debug(len(four_thousands_hospitals))
        # an_hospital = Hospital("6", "Buzzi 5")
        # an_hospital2 = Hospital("2", "BUZZI 2")
        # hospitals = [an_hospital, an_hospital2]
        # json_hospitals = [hospital.to_json() for hospital in hospitals]
        # log.debug("json_hospitals: %s", json_hospitals)
        # upserted_id = self.database.upsert_one_tuple(table_name=TableNames.HOSPITAL.value, filter_dict={"name": an_hospital.name}, one_tuple=an_hospital.to_json())
        # (upserted_document, is_insert) = self.database.upsert_one_tuple(table_name=TableNames.HOSPITAL.value, unique_variables=["name"], one_tuple=an_hospital2.to_json())
        # log.debug(upserted_document)
        # log.debug(is_insert)
        # upserted_id = self.database.upsert_one_tuple(table_name=TableNames.HOSPITAL.value, filter_dict={"name": an_hospital3.name}, one_tuple=an_hospital3.to_json())
        # self.database.upsert_many_tuples(table_name=TableNames.HOSPITAL.value, unique_variables=["name"], tuples=four_thousands_hospitals)
        # log.debug(nb_upserts)
        self.fail()

    def test_insert_many_tuples(self):
        config = BetterConfig()
        config.set_db_name(TEST_DB_NAME)
        config.set_db_drop("True")
        log.debug(config.to_json())

        database = Database(config)
        tuples = [{"id": 1, "name": "Louise", "country": "FR", "job": "PhD student"},
                  {"id": 2, "name": "Francesca", "country": "IT", "university": True},
                  {"id": 3, "name": "Martin", "country": "DE", "age": 26}]
        log.debug(TEST_TABLE_NAME)
        log.debug(tuples)
        list_dbs = database.client.list_databases()
        for db in list_dbs:
            log.debug(db)
        database.insert_many_tuples(TEST_TABLE_NAME, tuples)
        log.debug(tuples)
        docs = database.db[TEST_TABLE_NAME].find({})  # return JSON data
        print("JSON data:", docs)
        database.write_in_file(docs, TEST_TABLE_NAME, 0)

    def test_upsert_one_tuple(self):
        pass

    def test_upsert_batch_of_tuples(self):
        pass

    def test_compute_batches(self):
        pass

    def test_retrieve_identifiers(self):
        pass

    def test_find_operation(self):
        pass

    def test_count_documents(self):
        pass

    def test_create_unique_index(self):
        pass

    def test_get_min_value_of_examination_record(self):
        pass

    def test_get_max_value_of_examination_record(self):
        pass

    def test_get_min_max_value_of_examination_record(self):
        pass

    def test_get_avg_value_of_examination_record(self):
        pass

    def test_get_value_distribution_of_examination(self):
        pass

    def test_get_db(self):
        pass
