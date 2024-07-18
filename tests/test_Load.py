import json
import os.path
import unittest
from datetime import datetime

from database.Database import Database
from database.Execution import Execution
from etl.Load import Load
from enums.HospitalNames import HospitalNames
from enums.Ontologies import Ontologies
from enums.TableNames import TableNames
from utils.constants import DEFAULT_DB_CONNECTION, TEST_DB_NAME
from utils.utils import get_mongodb_date_from_datetime


# personalized setup called at the beginning of each test
def my_setup(create_indexes: bool) -> Load:
    # 1. as usual, create a Load object (to setup the current working directory)
    args = {
        Execution.DB_CONNECTION_KEY: DEFAULT_DB_CONNECTION,
        Execution.DB_DROP_KEY: True,
        # no need to set the metadata and data filepaths as we get only insert data that is written in temporary JSON files
    }
    TestLoad.execution.set_up(args_as_dict=args, setup_data_files=False)  # no need to setup the files, we get data and metadata as input
    database = Database(TestLoad.execution)
    load = Load(database=database, execution=TestLoad.execution, create_indexes=create_indexes)

    # 2. create few "fake" files in the current working directory in order to tests insertion and index creation
    examinations = [
        {
            "identifier": {"value": 1},
            "code": {
                "text": "age (Age in weeks)",
                "coding": [{"system": Ontologies.LOINC.value["url"], "code": "123-456", "display": "age (Age in weeks)"}]},
            "createdAt": get_mongodb_date_from_datetime(current_datetime=datetime.now())
        }, {
            "identifier": {"value": 2},
            "code": {
                "text": "twin (Whether the baby has a twin)",
                "coding": [{"system": Ontologies.LOINC.value["url"], "code": "123-457", "display": "twin (Whether the baby has a twin)"}]},
            "createdAt": get_mongodb_date_from_datetime(current_datetime=datetime.now())
        }
    ]

    examination_records = [
        {
            "identifier": {"value": 1},
            "value": 12,
            "subject": {"reference": "1", "type": "Patient"},
            "recordedBy": {"reference": "1", "type": "Hospital"},
            "instantiate": {"reference": "1", "type": "Examination"},
            "createdAt": get_mongodb_date_from_datetime(current_datetime=datetime.now())
        }
    ]

    patients = [
        {"identifier": {"value": 1}, "createdAt": get_mongodb_date_from_datetime(current_datetime=datetime.now())},
        {"identifier": {"value": 2}, "createdAt": get_mongodb_date_from_datetime(current_datetime=datetime.now())},
        {"identifier": {"value": 3}, "createdAt": get_mongodb_date_from_datetime(current_datetime=datetime.now())}
    ]

    hospital = {"identifier": 1, "name": HospitalNames.TEST_H1.value }

    # 3. write them in temporary JSON files
    path_examinations = os.path.join(TestLoad.execution.working_dir_current, "Examination1.json")
    path_examination_records = os.path.join(TestLoad.execution.working_dir_current, "ExaminationRecord1.json")
    path_patients = os.path.join(TestLoad.execution.working_dir_current, "Patient1.json")
    path_hospital = os.path.join(TestLoad.execution.working_dir_current, "Hospital1.json")
    # insert the data that is inserted during the Transform step
    with open(path_examinations, 'w') as f:
        json.dump(examinations, f)
    load._database.db["Examination"].insert_many(examinations)
    with open(path_hospital, 'w') as f:
        json.dump(hospital, f)
    load._database.db["Hospital"].insert_one(hospital)
    # for other files, it will be inserted with the function load_remaining_data()
    with open(path_examination_records, 'w') as f:
        json.dump(examination_records, f)
    with open(path_patients, 'w') as f:
        json.dump(patients, f)

    return load


class TestLoad(unittest.TestCase):
    execution = Execution(TEST_DB_NAME)

    def test_run(self):
        pass

    def test_load_remaining_data(self):
        load = my_setup(create_indexes=True)
        load.load_remaining_data()

        assert load._database.db["Patient"].count_documents(filter={}) == 3
        assert load._database.db["Examination"].count_documents(filter={}) == 2
        assert load._database.db["ExaminationRecord"].count_documents(filter={}) == 1
        assert load._database.db["Hospital"].count_documents(filter={}) == 1
        assert load._database.db["Sample"].count_documents(filter={}) == 0

    def test_create_db_indexes(self):
        load = my_setup(create_indexes=True)
        load.create_db_indexes()

        # 1. for each table , we check that there are three indexes:
        #    - one on _id (mandatory, made by MongoDB)
        #    - one on identifier.value
        #    - one on createdAt
        for table_name in TableNames:
            index_cursor = load._database.db[table_name.value].list_indexes()
            print("\nindex_cursor TYPE:", type(index_cursor))
            count_indexes = 0
            # indexes are of the form
            # SON([('v', 2), ('key', SON([('_id', 1)])), ('name', '_id_')])
            # SON([('v', 2), ('key', SON([('identifier.value', 1)])), ('name', 'identifier.value_1'), ('unique', True)])
            # SON([('v', 2), ('key', SON([('createdAt', 1)])), ('name', 'createdAt_1')])
            for index in index_cursor:
                index_key = index["key"]
                if "_id" in index_key or "identifier.value" in index_key or "createdAt" in index_key:
                    # to check whether we have exactly the three indexes we expect
                    count_indexes = count_indexes + 1
                    # assert that only identifier.value is unique, createdAt is not (there may be several instances created at the same time)
                    if "identifier.value" in index_key:
                        assert index["unique"] is True
                    else:
                        assert "unique" not in index
                else:
                    if table_name.value == TableNames.EXAMINATION.value:
                        # there is also a double indexes (code.coding.system and code.coding.code)
                        if "code.coding.system" in index_key and "code.coding.code" in index_key:
                            count_indexes = count_indexes + 1
                            assert "unique" not in index
                        else:
                            assert False, f"{table_name.value} expects a compound index on two fields."
                    elif table_name.value == TableNames.EXAMINATION_RECORD.value:
                        # there are also three more indexes (instantiate.reference, subject.reference, basedOn.reference)
                        if "instantiate.reference" in index_key:
                            count_indexes = count_indexes + 1
                            assert "unique" not in index
                        elif "subject.reference" in index_key:
                            count_indexes = count_indexes + 1
                            assert "unique" not in index
                        elif "basedOn.reference" in index_key:
                            count_indexes = count_indexes + 1
                            assert "unique" not in index
                        else:
                            assert False, f"{table_name} has an unknown index named {index_key}."
                    else:
                        assert False, f"{table_name} should have no index."
            if table_name.value == TableNames.EXAMINATION.value:
                assert count_indexes == 4
            elif table_name.value == TableNames.EXAMINATION_RECORD.value:
                assert count_indexes == 6
            else:
                assert count_indexes == 3
