from database.Database import Database
from database.Execution import Execution
from utils.constants import TEST_DB_NAME, TEST_TABLE_NAME
from utils.setup_logger import log


class TestDatabase:
    def test_check_server_is_up(self):
        execution = Execution()

        # # test with the correct (default) string
        # execution.set_db_name(db_name=TEST_DB_NAME)
        # database = Database(execution=execution)
        # assert database.check_server_is_up()
        # # database.close()

        # test with a wrong connection string
        execution.set_db_connection(db_connection="a_random_connection_string")
        execution.set_db_name(db_name=TEST_DB_NAME)
        database = Database(execution=execution)
        assert not database.check_server_is_up()
        # database.close()

    def test_drop(self):
        # check that, after drop, no db with the provided name exists
        execution = Execution()
        execution.set_db_name(db_name=TEST_DB_NAME)
        # TODO Nelly assert

    def test_reset(self):
        execution = Execution()
        execution.set_db_name(db_name=TEST_DB_NAME)
        # create a test database
        # and add only one triple to be sure that the db is created
        database = Database(execution=execution)
        database.insert_one_tuple(table_name=TEST_TABLE_NAME, one_tuple={ "id": "1", "name": "Alice Doe"})
        list_dbs = database.client.list_databases()
        found = False
        for db in list_dbs:
            if db['name'] == TEST_DB_NAME:
                found = True
        assert found
        database.drop_db()
        # check the DB does not exist anymore after drop
        list_dbs = database.client.list_databases()
        found = False
        for db in list_dbs:
            if db['name'] == TEST_DB_NAME:
                found = True
        assert not found
        # database.close()

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
        execution = Execution()
        execution.set_db_name(db_name=TEST_DB_NAME)
        execution.set_db_drop(drop=True)

        database = Database(execution=execution)
        tuples = [{"id": 1, "name": "Louise", "country": "FR", "job": "PhD student"},
                  {"id": 2, "name": "Francesca", "country": "IT", "university": True},
                  {"id": 3, "name": "Martin", "country": "DE", "age": 26}]
        database.insert_many_tuples(table_name=TEST_TABLE_NAME, tuples=tuples)
        docs = []
        for doc in database.db[TEST_TABLE_NAME].find({}):
            docs.append(doc)

        assert len(tuples) == len(docs)
        # TODO Nelly: test more
