import unittest
from datetime import datetime

from constants.structure import TEST_DB_NAME
from database.Database import Database
from entities.Dataset import Dataset
from database.Execution import Execution
from database.Operators import Operators
from entities.Feature import Feature
from entities.Hospital import Hospital
from entities.OntologyResource import OntologyResource
from entities.Record import Record
from entities.Resource import Resource
from enums.HospitalNames import HospitalNames
from enums.Ontologies import Ontologies
from enums.ParameterKeys import ParameterKeys
from enums.Profile import Profile
from enums.TableNames import TableNames
from etl.Load import Load
from statistics.QualityStatistics import QualityStatistics
from utils.file_utils import write_in_file
from utils.setup_logger import log
from utils.test_utils import set_env_variables_from_dict


# personalized setup called at the beginning of each test
def my_setup(profile: str, create_indexes: bool) -> Load:
    # 1. as usual, create a Load object (to set up the current working directory)
    args = {
        ParameterKeys.DB_NAME: TEST_DB_NAME,
        ParameterKeys.DB_DROP: "True"
        # no need to set the metadata and data filepaths as we get only insert data that is written in temporary JSON files
    }
    set_env_variables_from_dict(env_vars=args)
    TestLoad.execution.internals_set_up()
    TestLoad.execution.file_set_up(setup_files=False)  # no need to set up the files, we get data and metadata as input
    database = Database(execution=TestLoad.execution)
    load = Load(database=database, execution=TestLoad.execution, create_indexes=create_indexes, dataset_id=99,
                profile=profile, quality_stats=QualityStatistics(record_stats=False))

    # 2. create few "fake" files in the current working directory in order to test insertion and index creation
    TestLoad.execution.current_dataset_gid = Dataset.compute_global_identifier()
    phen_features = [
        {
            Resource.IDENTIFIER_: 1,
            "ontology_resource": {
                OntologyResource.SYSTEM_: Ontologies.LOINC["url"],
                OntologyResource.CODE_: "123-456",
                OntologyResource.LABEL_: "age (Age in weeks)"
            },
            Resource.TIMESTAMP_: Operators.from_datetime_to_isodate(current_datetime=datetime.now()),
            Resource.ENTITY_TYPE_: f"{profile}{TableNames.FEATURE}"
        }, {
            Resource.IDENTIFIER_: 2,
            "ontology_resource": {
                OntologyResource.SYSTEM_: Ontologies.LOINC["url"],
                OntologyResource.CODE_: "123-457",
                OntologyResource.LABEL_: "twin (Whether the baby has a twin)"
            },
            Resource.TIMESTAMP_: Operators.from_datetime_to_isodate(current_datetime=datetime.now()),
            Resource.ENTITY_TYPE_: f"{profile}{TableNames.FEATURE}"
        }
    ]

    phen_records = [
        {
            Resource.IDENTIFIER_: 3,
            Record.VALUE_: 12,
            Record.SUBJECT_: 4,
            Record.REG_BY_: 1,
            Record.INSTANTIATES_: 2,
            Resource.DATASET_: f"{TestLoad.execution.current_dataset_gid}",
            Resource.TIMESTAMP_: Operators.from_datetime_to_isodate(current_datetime=datetime.now()),
            Resource.ENTITY_TYPE_: f"{profile}{TableNames.RECORD}"
        }
    ]

    patients = [
        {Resource.IDENTIFIER_: "test:1", Resource.TIMESTAMP_: Operators.from_datetime_to_isodate(current_datetime=datetime.now())},
        {Resource.IDENTIFIER_: "test:2", Resource.TIMESTAMP_: Operators.from_datetime_to_isodate(current_datetime=datetime.now())},
        {Resource.IDENTIFIER_: "test:3", Resource.TIMESTAMP_: Operators.from_datetime_to_isodate(current_datetime=datetime.now())}
    ]

    hospital = {Resource.IDENTIFIER_: f"{TableNames.HOSPITAL}:1", Hospital.NAME_: HospitalNames.TEST_H1}

    # 3. write them in temporary JSON files
    # we use 99 because we already have 1PhenotypicFeature1.json, and it would overwrite the json file
    # leading to inconsistencies and wrong inserts
    # insert the data that is inserted during the Transform step
    write_in_file(resource_list=phen_features, current_working_dir=TestLoad.execution.working_dir_current, table_name=TableNames.FEATURE, is_feature=True, dataset_id=99, to_json=False)
    load.database.insert_many_tuples(table_name=TableNames.FEATURE, tuples=phen_features)
    write_in_file(resource_list=[hospital], current_working_dir=TestLoad.execution.working_dir_current, table_name=TableNames.HOSPITAL, is_feature=False, dataset_id=99, to_json=False)
    load.database.insert_one_tuple(table_name=TableNames.HOSPITAL, one_tuple=hospital)
    # for other files, it will be inserted with the function load_remaining_data()
    write_in_file(resource_list=phen_records, current_working_dir=TestLoad.execution.working_dir_current, table_name=TableNames.RECORD, is_feature=False, dataset_id=99, to_json=False)
    write_in_file(resource_list=patients, current_working_dir=TestLoad.execution.working_dir_current, table_name=TableNames.PATIENT, is_feature=False, dataset_id=99, to_json=False)

    return load


class TestLoad(unittest.TestCase):
    execution = Execution()

    def test_run(self):
        pass

    def test_load_remaining_data(self):
        load = my_setup(profile=Profile.PHENOTYPIC, create_indexes=False)
        load.load_records()

        assert load.database.db[TableNames.FEATURE].count_documents(filter={}) == 2
        assert load.database.db[TableNames.RECORD].count_documents(filter={}) == 1
        assert load.database.db[TableNames.HOSPITAL].count_documents(filter={}) == 1

    def test_create_db_indexes(self):
        load = my_setup(profile=Profile.PHENOTYPIC, create_indexes=True)
        load.load_records()  # load also records (features, patients and hospital have been loaded in my_setup)
        load.create_db_indexes()

        # 1. for each table , we check that there are three indexes:
        #    - one on _id (mandatory, made by MongoDB)
        #    - one on identifier.value
        #    - one on timestamp
        log.info(TableNames.values(db=load.database))
        for table_name in TableNames.values(db=load.database):
            index_cursor = load.database.db[table_name].list_indexes()
            log.debug(f"table {table_name}, index_cursor: {index_cursor}")
            count_indexes = 0
            # indexes are of the form
            # SON([('v', 2), ('key', SON([('_id', 1)])), ('name', '_id_')])
            # SON([('v', 2), ('key', SON([('identifier.value', 1)])), ('name', 'identifier.value_1'), ('unique', True)])
            # SON([('v', 2), ('key', SON([('timestamp', 1)])), ('name', 'timestamp_1')])
            for index in index_cursor:
                index_keys = index["key"]
                log.info(index)
                log.info(index_keys)
                if len(index_keys) == 1 and ("_id" in index_keys or Resource.IDENTIFIER_ in index_keys or Resource.TIMESTAMP_ in index_keys or Resource.ENTITY_TYPE_ in index_keys):
                    # to check whether we have exactly the four indexes we expect
                    count_indexes += 1
                    # assert that only identifier is unique,
                    # timestamp is not (there may be several instances created at the same time)
                    # resource type is not either (we have several instances of the same type)
                    if Resource.IDENTIFIER_ in index_keys:
                        assert index["unique"] is True
                    else:
                        assert "unique" not in index
                else:
                    if table_name == TableNames.FEATURE:
                        # there is also a double index (ontology_resource.system and ontology_resource.code)
                        if len(index_keys) == 2 and f"{Feature.ONTO_}.{OntologyResource.SYSTEM_}" in index_keys and f"{Feature.ONTO_}.{OntologyResource.CODE_}" in index_keys:
                            count_indexes += 1
                            assert "unique" not in index
                        else:
                            assert False, f"{table_name} expects a compound index on two fields."
                    elif table_name == TableNames.RECORD:
                        if len(index_keys) == 5 and (Record.INSTANTIATES_ in index_keys and Record.REG_BY_ in index_keys and Record.SUBJECT_ in index_keys
                                and Resource.ENTITY_TYPE_ in index_keys and Record.BASE_ID_ in index_keys):
                            # this is the index created for upserts
                            count_indexes += 1
                            assert index["unique"] is True
                        elif len(index_keys) == 1:
                            if Record.INSTANTIATES_ in index_keys:
                                count_indexes += 1
                                assert "unique" not in index
                            elif Record.SUBJECT_ in index_keys:
                                count_indexes += 1
                                assert "unique" not in index
                            elif Resource.DATASET_ in index_keys:
                                count_indexes += 1
                                assert "unique" not in index
                            elif Record.REG_BY_ in index_keys:
                                count_indexes += 1
                                assert "unique" not in index
                            else:
                                assert False, f"{table_name} has an unknown index named {index_keys}."
                        else:
                            assert False, f"{table_name} has an unknown index named {index_keys}."
                    elif table_name == TableNames.DATASET:
                        if len(index_keys) == 1 and ("_id" in index_keys or Dataset.GID_ in index_keys):
                            count_indexes += 1
                            if Dataset.GID_ in index_keys:
                                assert index["unique"] is True
                            else:
                                assert "unique" not in index
                    else:
                        assert False, f"{table_name} should have no index."
            if table_name == TableNames.FEATURE:
                assert count_indexes == 5  # (_id, identifier, timestamp, entity_type, <onto.name, onto.code>)
            elif table_name == TableNames.RECORD:
                assert count_indexes == 8  # (_id, identifier, timestamp, entity_type, instantiates, has_subject, dataset, registered_by, <instantiates, has_subject, dataset, registered_by, entity type, base_id>)
            elif table_name == TableNames.DATASET:
                assert count_indexes == 4  # (_id, identifier, timestamp, global_identifier)
            elif table_name == TableNames.HOSPITAL:
                assert count_indexes == 4  # (_id, identifier, timestamp, entity_type)
            else:
                assert count_indexes == 4  # (_id, identifier, timestamp, entity_type)
