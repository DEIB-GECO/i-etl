from unittest import TestCase


class TestDatabase(TestCase):
    def test_check_server_is_up(self):
        self.fail()

    def test_reset(self):
        self.fail()

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
        self.fail()

    def test_upsert_one_tuple(self):
        self.fail()

    def test_upsert_batch_of_tuples(self):
        self.fail()

    def test_compute_batches(self):
        self.fail()

    def test_retrieve_identifiers(self):
        self.fail()

    def test_find_operation(self):
        self.fail()

    def test_count_documents(self):
        self.fail()

    def test_create_unique_index(self):
        self.fail()

    def test_get_min_value_of_examination_record(self):
        self.fail()

    def test_get_max_value_of_examination_record(self):
        self.fail()

    def test_get_min_max_value_of_examination_record(self):
        self.fail()

    def test_get_avg_value_of_examination_record(self):
        self.fail()

    def test_get_value_distribution_of_examination(self):
        self.fail()

    def test_get_db(self):
        self.fail()
