import json
import os.path
import unittest
from datetime import datetime

from database.Database import Database
from database.Execution import Execution
from enums.ParameterKeys import ParameterKeys
from etl.Load import Load
from enums.HospitalNames import HospitalNames
from enums.Ontologies import Ontologies
from enums.TableNames import TableNames
from constants.structure import TEST_DB_NAME
from query.Operators import Operators
from statistics.QualityStatistics import QualityStatistics
from statistics.TimeStatistics import TimeStatistics
from utils.setup_logger import log
from utils.test_utils import set_env_variables_from_dict


# personalized setup called at the beginning of each test
def my_setup(create_indexes: bool) -> Load:
    # 1. as usual, create a Load object (to setup the current working directory)
    args = {
        ParameterKeys.DB_NAME: TEST_DB_NAME,
        ParameterKeys.DB_DROP: "True"
        # no need to set the metadata and data filepaths as we get only insert data that is written in temporary JSON files
    }
    set_env_variables_from_dict(env_vars=args)

    TestLoad.execution.set_up(setup_data_files=False)  # no need to setup the files, we get data and metadata as input
    database = Database(TestLoad.execution)
    load = Load(database=database, execution=TestLoad.execution, create_indexes=create_indexes,
                quality_stats=QualityStatistics(record_stats=False), time_stats=TimeStatistics(record_stats=False))

    # 2. create few "fake" files in the current working directory in order to test insertion and index creation
    lab_features = [
        {
            "identifier": {"value": "1"},
            "code": {
                "text": "age (Age in weeks)",
                "coding": [{"system": Ontologies.LOINC["url"], "code": "123-456", "display": "age (Age in weeks)"}]},
            "timestamp": Operators.from_datetime_to_isodate(current_datetime=datetime.now())
        }, {
            "identifier": {"value": "2"},
            "code": {
                "text": "twin (Whether the baby has a twin)",
                "coding": [{"system": Ontologies.LOINC["url"], "code": "123-457", "display": "twin (Whether the baby has a twin)"}]},
            "timestamp": Operators.from_datetime_to_isodate(current_datetime=datetime.now())
        }
    ]

    lab_records = [
        {
            "identifier": {"value": "1"},
            "value": 12,
            "subject": {"reference": "1", "type": TableNames.PATIENT},
            "recorded_by": {"reference": "1", "type": TableNames.HOSPITAL},
            "instantiate": {"reference": "1", "type": TableNames.LABORATORY_FEATURE},
            "timestamp": Operators.from_datetime_to_isodate(current_datetime=datetime.now())
        }
    ]

    patients = [
        {"identifier": {"value": "1"}, "timestamp": Operators.from_datetime_to_isodate(current_datetime=datetime.now())},
        {"identifier": {"value": "2"}, "timestamp": Operators.from_datetime_to_isodate(current_datetime=datetime.now())},
        {"identifier": {"value": "3"}, "timestamp": Operators.from_datetime_to_isodate(current_datetime=datetime.now())}
    ]

    hospital = {"identifier": {"value": "1"}, "name": HospitalNames.TEST_H1}

    # 3. write them in temporary JSON files
    path_lab_features = os.path.join(TestLoad.execution.working_dir_current, TableNames.LABORATORY_FEATURE+"1.json")
    path_lab_records = os.path.join(TestLoad.execution.working_dir_current, TableNames.LABORATORY_RECORD+"1.json")
    path_patients = os.path.join(TestLoad.execution.working_dir_current, TableNames.PATIENT+"1.json")
    path_hospital = os.path.join(TestLoad.execution.working_dir_current, TableNames.HOSPITAL+"1.json")
    # insert the data that is inserted during the Transform step
    with open(path_lab_features, 'w') as f:
        json.dump(lab_features, f)
    load.database.db[TableNames.LABORATORY_FEATURE].insert_many(lab_features)
    with open(path_hospital, 'w') as f:
        json.dump(hospital, f)
    load.database.db[TableNames.HOSPITAL].insert_one(hospital)
    # for other files, it will be inserted with the function load_remaining_data()
    with open(path_lab_records, 'w') as f:
        json.dump(lab_records, f)
    with open(path_patients, 'w') as f:
        json.dump(patients, f)

    return load


class TestLoad(unittest.TestCase):
    execution = Execution()

    def test_run(self):
        pass

    def test_load_remaining_data(self):
        load = my_setup(create_indexes=True)
        load.load_remaining_data()

        assert load.database.db[TableNames.PATIENT].count_documents(filter={}) == 3
        assert load.database.db[TableNames.LABORATORY_FEATURE].count_documents(filter={}) == 2
        assert load.database.db[TableNames.LABORATORY_RECORD].count_documents(filter={}) == 1
        assert load.database.db[TableNames.HOSPITAL].count_documents(filter={}) == 1
        assert load.database.db[TableNames.SAMPLE].count_documents(filter={}) == 0

    def test_create_db_indexes(self):
        load = my_setup(create_indexes=True)
        load.create_db_indexes()

        # 1. for each table , we check that there are three indexes:
        #    - one on _id (mandatory, made by MongoDB)
        #    - one on identifier.value
        #    - one on timestamp
        for table_name in TableNames.values():
            index_cursor = load.database.db[table_name].list_indexes()
            log.debug(f"table {table_name}, index_cursor: {index_cursor}")
            count_indexes = 0
            # indexes are of the form
            # SON([('v', 2), ('key', SON([('_id', 1)])), ('name', '_id_')])
            # SON([('v', 2), ('key', SON([('identifier.value', 1)])), ('name', 'identifier.value_1'), ('unique', True)])
            # SON([('v', 2), ('key', SON([('timestamp', 1)])), ('name', 'timestamp_1')])
            for index in index_cursor:
                index_key = index["key"]
                log.debug(index_key)
                if "_id" in index_key or "identifier.value" in index_key or "timestamp" in index_key:
                    # to check whether we have exactly the three indexes we expect
                    count_indexes = count_indexes + 1
                    # assert that only identifier.value is unique, timestamp is not (there may be several instances created at the same time)
                    if "identifier.value" in index_key:
                        assert index["unique"] is True
                    else:
                        assert "unique" not in index
                else:
                    if table_name in TableNames.features():
                        # there is also a double index (code.coding.system and code.coding.code)
                        if "code.coding.system" in index_key and "code.coding.code" in index_key:
                            count_indexes = count_indexes + 1
                            assert "unique" not in index
                        else:
                            assert False, f"{table_name} expects a compound index on two fields."
                    elif table_name in TableNames.records():
                        # there are also two more indexes (instantiate.reference, subject.reference)
                        if "instantiate.reference" in index_key:
                            count_indexes = count_indexes + 1
                            assert "unique" not in index
                        elif "subject.reference" in index_key:
                            count_indexes = count_indexes + 1
                            assert "unique" not in index
                        elif "based_on.reference" in index_key:
                            # did not test Sample ref index because it is proper to BUZZI data
                            pass
                            #     count_indexes = count_indexes + 1
                            #     assert "unique" not in index
                        else:
                            assert False, f"{table_name} has an unknown index named {index_key}."
                    else:
                        assert False, f"{table_name} should have no index."
            if table_name in TableNames.features():
                assert count_indexes == 4
            elif table_name in TableNames.records():
                assert count_indexes == 5
            else:
                assert count_indexes == 3
