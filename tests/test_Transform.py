import json
import os
import re
import time
import unittest

import pandas as pd
import pytest

from constants.defaults import DEFAULT_ONTOLOGY_RESOURCE_LABEL, NO_ID
from constants.structure import TEST_DB_NAME, DOCKER_FOLDER_TEST
from database.Counter import Counter
from database.Database import Database
from entities.Dataset import Dataset
from database.Execution import Execution
from entities.Feature import Feature
from entities.Hospital import Hospital
from entities.OntologyResource import OntologyResource
from entities.Record import Record
from entities.Resource import Resource
from enums.DataTypes import DataTypes
from enums.HospitalNames import HospitalNames
from enums.MetadataColumns import MetadataColumns
from enums.Ontologies import Ontologies
from enums.ParameterKeys import ParameterKeys
from enums.Profile import Profile
from enums.TableNames import TableNames
from enums.TheTestFiles import TheTestFiles
from enums.Visibility import Visibility
from etl.Transform import Transform
from statistics.QualityStatistics import QualityStatistics
from utils.cast_utils import cast_str_to_datetime
from utils.file_utils import get_json_resource_file, from_json_line_to_json_str, clear_file
from utils.file_utils import read_tabular_file_as_string
from utils.setup_logger import log
from utils.test_utils import set_env_variables_from_dict, get_feature_by_text, \
    get_field_value_for_patient, get_records_for_patient


# personalized setup called at the beginning of each test
def my_setup(hospital_name: str, profile: str, extracted_metadata_path: str, extracted_data_paths: str,
             extracted_column_to_categorical_path: str,
             extracted_column_type_path: str,
             extracted_column_unit_path: str,
             extracted_domain_path: str,
             db_drop: str = "True") -> Transform:
    args = {
        ParameterKeys.DB_NAME: TEST_DB_NAME,
        ParameterKeys.DB_DROP: db_drop,
        ParameterKeys.HOSPITAL_NAME: hospital_name
        # no need to set the metadata and data filepaths as we get already the loaded data and metadata as arguments
    }
    set_env_variables_from_dict(env_vars=args)
    TestTransform.execution.internals_set_up()
    TestTransform.execution.file_set_up(setup_files=False)  # no need to set up the files, we get data and metadata as input
    TestTransform.execution.db_drop = True
    database = Database(execution=TestTransform.execution)
    # I load:
    # - the data and metadata from two CSV files that I obtained by running the Extract step
    # - and mapped_values as a JSON file that I obtained from the same Extract object
    metadata = read_tabular_file_as_string(os.path.join(DOCKER_FOLDER_TEST, extracted_metadata_path))
    data_path = os.path.join(DOCKER_FOLDER_TEST, extracted_data_paths)
    data = read_tabular_file_as_string(data_path)
    # for data only, we need to do a second pass on the data to replace explicit NaN values by np.nan
    # something Extract is doing, but here we need to do it because we simply read the expected CSV file,
    # thus NaN values are read as strings and need to be transformed
    for column in data:
        data.loc[:, column] = data[column].apply(lambda x: MetadataColumns.normalize_value(column_value=x))

    with open(os.path.join(DOCKER_FOLDER_TEST, extracted_column_to_categorical_path), "r") as f:
        mapping_column_to_categorical_value = json.load(f)
    with open(os.path.join(DOCKER_FOLDER_TEST, extracted_column_unit_path), "r") as f:
        column_to_unit = json.load(f)
    with open(os.path.join(DOCKER_FOLDER_TEST, extracted_domain_path), "r") as f:
        mapping_column_to_domain = json.load(f)
    with open(os.path.join(DOCKER_FOLDER_TEST, extracted_column_type_path), "r") as f:
        mapping_column_to_type = json.load(f)

    dataset_instance = Dataset(identifier=NO_ID, database=database, docker_path=data_path, version_notes=None, license=None, counter=Counter())
    transform = Transform(database=database, execution=TestTransform.execution, data=data, metadata=metadata,
                          profile=profile, dataset_id=get_dataset_id_from_profile(profile),
                          mapping_column_to_categorical_value=mapping_column_to_categorical_value,
                          mapping_column_to_unit=column_to_unit,
                          mapping_column_to_domain=mapping_column_to_domain,
                          mapping_column_to_type=mapping_column_to_type,
                          load_patients=True,
                          quality_stats=QualityStatistics(record_stats=False),
                          dataset_key=dataset_instance)

    # create a hospital instance, to be able to create records with references to the hospital
    transform.database.insert_one_tuple(table_name=TableNames.HOSPITAL, one_tuple=Hospital(identifier=NO_ID, name=HospitalNames.TEST_H1, counter=Counter()).to_json())
    counter = Counter()
    counter.set_with_database(database=transform.database)
    return transform


def get_dataset_id_from_profile(profile: str):
    if profile == Profile.PHENOTYPIC:
        return 1
    elif profile == Profile.CLINICAL:
        return 2
    elif profile == Profile.DIAGNOSIS:
        return 3
    else:
        return 0


@pytest.fixture(autouse=True)
def run_before_and_after_tests(tmpdir):
    """Fixture to execute asserts before and after a test is run"""
    # Setup: fill with any logic you want

    yield  # this is where the testing happens

    # Teardown : fill with any logic you want
    write_empty_pid_files()


def delete_pid_file():
    pid_path = os.path.join(DOCKER_FOLDER_TEST, TheTestFiles.PIDS_PATH)
    if os.path.exists(pid_path):
        os.remove(pid_path)


def create_pid_file():
    with open(os.path.join(DOCKER_FOLDER_TEST, TheTestFiles.PIDS_PATH), "w") as f:
        the_pids = json.dumps({
            "999999999": 999,
            "999999998": 998,
            "999999997": 997,
            "999999996": 996,
            "999999995": 995,
            "999999994": 994,
            "999999993": 993,
            "999999992": 992,
            "999999991": 991,
            "999999990": 990
        })
        f.write(the_pids)


def write_empty_pid_files():
    with open(os.path.join(DOCKER_FOLDER_TEST, TheTestFiles.PIDS_PATH), "w") as f:
        f.write(json.dumps({}))


def get_transform_features(profile):
    with open(get_json_resource_file(current_working_dir=TestTransform.execution.working_dir_current, dataset_id=get_dataset_id_from_profile(profile), table_name=TableNames.FEATURE)) as f:
        # return json.load(f)  # this is a JSON-line file, we need to append brackets and commas back before parsing it
        return json.loads(from_json_line_to_json_str(f))


def get_transform_records(profile):
    with open(get_json_resource_file(current_working_dir=TestTransform.execution.working_dir_current, dataset_id=get_dataset_id_from_profile(profile), table_name=TableNames.RECORD)) as f:
        # return json.load(f)  # this is a JSON-line file, we need to append brackets and commas back before parsing it
        return json.loads(from_json_line_to_json_str(f))


def get_transform_patients(dataset_id):
    with open(get_json_resource_file(current_working_dir=TestTransform.execution.working_dir_current, dataset_id=dataset_id, table_name=TableNames.PATIENT)) as f:
        # return json.load(f)  # this is a JSON-line file, we need to append brackets and commas back before parsing it
        return json.loads(from_json_line_to_json_str(f))


class TestTransform(unittest.TestCase):
    execution = Execution()

    def test_set_resource_counter_id(self):
        # when there is nothing in the database, the counter should be 0
        transform = my_setup(hospital_name=HospitalNames.TEST_H1, profile=Profile.PHENOTYPIC,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_PHENOTYPIC_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_PHENOTYPIC_DATA_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_PHENOTYPIC_COL_CAT_PATH,
                             extracted_column_unit_path=TheTestFiles.EXTR_PHENOTYPIC_UNITS_PATH,
                             extracted_domain_path=TheTestFiles.EXTR_PHENOTYPIC_DOMAIN_PATH,
                             extracted_column_type_path=TheTestFiles.EXTR_PHENOTYPIC_TYPE_PATH)
        counter = Counter()
        counter.set_with_database(database=transform.database)

        assert transform.counter.resource_id == 0

        # when some tables already contain resources, it should not be 0
        # I manually insert some resources in the database
        database = Database(execution=TestTransform.execution)
        my_tuples = [
            {"identifier": 1},
            {"identifier": 2},
            {"identifier": 7},
            {"identifier": 3},
            {"identifier": 124},
            {"identifier": 9},
            {"identifier": 123},
            {"identifier": 6}
        ]
        database.insert_many_tuples(table_name=TableNames.FEATURE, tuples=my_tuples)
        time.sleep(2)
        counter.set_with_database(database=transform.database)
        assert counter.resource_id == 125

    def test_phenotypic_data(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1, profile=Profile.PHENOTYPIC,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_PHENOTYPIC_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_PHENOTYPIC_DATA_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_PHENOTYPIC_COL_CAT_PATH,
                             extracted_column_unit_path=TheTestFiles.EXTR_PHENOTYPIC_UNITS_PATH,
                             extracted_domain_path=TheTestFiles.EXTR_PHENOTYPIC_DOMAIN_PATH,
                             extracted_column_type_path=TheTestFiles.EXTR_PHENOTYPIC_TYPE_PATH)
        transform.create_patients()
        transform.counter.set_with_database(database=transform.database)
        transform.create_features()
        transform.counter.set_with_database(database=transform.database)
        transform.create_records()

        # CHECK FEATURES
        features = get_transform_features(profile=Profile.PHENOTYPIC)
        assert len(features) == 4 - 1  # id does not count as a PhenFeature
        # assert the fourth and first PhenFeature instances:
        # lab_feature_a has one associated code
        # lab_feature_b has no associated code
        # because they may not be in the same order as in the metadata file, we get them based on their text
        # (which contains at least the column name, and maybe a description)
        # PhenFeature about sex
        lab_feature_a = get_feature_by_text(features, "sex")
        assert len(lab_feature_a) == 11  # inherited fields (identifier, entity_type, timestamp), proper fields (name, ontology_resource, data_type, unit, visibility, datasets, description, domain)
        assert Resource.IDENTIFIER_ in lab_feature_a
        assert lab_feature_a[Feature.NAME_] == "sex"
        assert lab_feature_a[Feature.ONTO_] == {
            OntologyResource.SYSTEM_: Ontologies.SNOMEDCT["url"],
            OntologyResource.CODE_: "123:789",
            OntologyResource.LABEL_: f"{DEFAULT_ONTOLOGY_RESOURCE_LABEL}:{DEFAULT_ONTOLOGY_RESOURCE_LABEL}"
            # the two codes do not exist for eal in SNOMED, thus using the empty label
        }
        assert lab_feature_a[Feature.DT_] == DataTypes.CATEGORY
        assert Feature.UNIT_ not in lab_feature_a
        assert lab_feature_a[Feature.ENTITY_TYPE_] == f"{Profile.PHENOTYPIC}{TableNames.FEATURE}"
        assert lab_feature_a[Feature.DOMAIN_] is not None
        assert "accepted_values" in lab_feature_a[Feature.DOMAIN_]
        assert len(lab_feature_a[Feature.DOMAIN_]["accepted_values"]) == 2
        assert "m" in lab_feature_a[Feature.DOMAIN_]["accepted_values"]
        assert "f" in lab_feature_a[Feature.DOMAIN_]["accepted_values"]

        # PhenFeature about date_of_birth
        lab_feature_b = get_feature_by_text(features, "date_of_birth")
        assert len(lab_feature_b) == 8  # no ontology_resource, no unit, no domain
        assert Resource.IDENTIFIER_ in lab_feature_b
        assert Resource.TIMESTAMP_ in lab_feature_b
        assert lab_feature_b[Feature.NAME_] == "date_of_birth"
        assert Feature.ONTO_ not in lab_feature_b
        assert lab_feature_b[Feature.DT_] == DataTypes.DATETIME
        assert Feature.UNIT_ not in lab_feature_b
        assert lab_feature_b[Feature.ENTITY_TYPE_] == f"{Profile.PHENOTYPIC}{TableNames.FEATURE}"
        assert lab_feature_b[Feature.DESCR_] == "The date of birth"
        assert lab_feature_b[Feature.VISIBILITY_] == Visibility.ANONYMIZED

        # check that there are no duplicates in PhenFeature instances
        # for this, we get the set of their names (in the field "text")
        lab_features_names_list = []
        for lab_feature in features:
            if lab_feature is not None:
                lab_features_names_list.append(lab_feature[Feature.NAME_])
        lab_features_names_set = set(lab_features_names_list)
        assert len(lab_features_names_list) == len(lab_features_names_set)

        # CHECK RECORDS
        records = get_transform_records(profile=Profile.PHENOTYPIC)
        log.info(records)
        assert len(records) == 18  # in total, 18 PhenRecord instances are created, between 2 and 5 per Patient, the only explicit NaN value is indeed created
        # assert that PhenRecord instances have been correctly created for a given data row
        # we take the seventh row
        patient_id = transform.patient_ids_mapping["999999994"]
        assert patient_id == 6  # patient anonymized IDs start at h1:1, because no hospital has been created beforehand.
        phen_records_patient = get_records_for_patient(records=records, patient_id=patient_id)
        log.info(phen_records_patient)
        female = OntologyResource(system=Ontologies.SNOMEDCT, code="248152002", label=None, quality_stats=None)
        assert phen_records_patient[0][Record.VALUE_] == female.to_json()  # the value as been replaced by its ontology code (sex is a categorical value)
        assert phen_records_patient[1][Record.VALUE_] == "black"
        assert phen_records_patient[2][Record.VALUE_] == {"$date": "2021-12-01T00:00:00Z"}  # the value as been converted to a MongoDB-style datetime and anonymized (remove day and time)
        pattern_date = re.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2,3}Z")
        assert pattern_date.match(phen_records_patient[0][Resource.TIMESTAMP_]["$date"])  # check the date is in datetime (CEST) form

        # we also check that string are normalized (no caps, etc.)
        assert transform.patient_ids_mapping["999999996"] == 4  # 0 Hospital before patients
        assert get_field_value_for_patient(records=records, features=features, patient_id=transform.patient_ids_mapping["999999999"], column_name="ethnicity") == "white"
        assert get_field_value_for_patient(records=records, features=features, patient_id=transform.patient_ids_mapping["999999998"], column_name="ethnicity") == "white"
        assert get_field_value_for_patient(records=records, features=features, patient_id=transform.patient_ids_mapping["999999997"], column_name="ethnicity") == "white"
        assert get_field_value_for_patient(records=records, features=features, patient_id=transform.patient_ids_mapping["999999996"], column_name="ethnicity") == "caucasian"
        assert get_field_value_for_patient(records=records, features=features, patient_id=transform.patient_ids_mapping["999999995"], column_name="ethnicity") is None  # no value so None
        assert get_field_value_for_patient(records=records, features=features, patient_id=transform.patient_ids_mapping["999999994"], column_name="ethnicity") == "black"
        assert get_field_value_for_patient(records=records, features=features, patient_id=transform.patient_ids_mapping["999999993"], column_name="ethnicity") == "caucasian"
        assert get_field_value_for_patient(records=records, features=features, patient_id=transform.patient_ids_mapping["999999992"], column_name="ethnicity") == "italian"
        assert pd.isnull(get_field_value_for_patient(records=records, features=features, patient_id=transform.patient_ids_mapping["999999991"], column_name="ethnicity"))  # explicit NaN value
        assert get_field_value_for_patient(records=records, features=features, patient_id=transform.patient_ids_mapping["999999990"], column_name="ethnicity") is None  # no value so None

    def test_clinical_data(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1, profile=Profile.CLINICAL,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_CLINICAL_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_CLINICAL_DATA_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_CLINICAL_COL_CAT_PATH,
                             extracted_column_unit_path=TheTestFiles.EXTR_CLINICAL_UNITS_PATH,
                             extracted_domain_path=TheTestFiles.EXTR_CLINICAL_DOMAIN_PATH,
                             extracted_column_type_path=TheTestFiles.EXTR_CLINICAL_TYPE_PATH)
        transform.load_patient_id_mapping()
        transform.create_patients()
        transform.counter.set_with_database(database=transform.database)
        transform.create_features()
        transform.counter.set_with_database(database=transform.database)
        transform.create_records()

        # CHECK FEATURES
        # we cannot use transform.features, because the array is cleared after all features are saved
        # we need to read from the JSON file written during the Transform step
        features = get_transform_features(profile=Profile.CLINICAL)
        log.info(features)
        assert len(features) == 6 - 2  # sid and id do not count as SamFeatures
        # assert the third and fourth SamFeature instances:
        # lab_feature_a has one associated code
        # lab_feature_b has no associated code
        # because they may not be in the same order as in the metadata file, we get them based on their text
        # (which contains at least the column name, and maybe a description)
        # SamFeature about molecule_a
        lab_feature_a = get_feature_by_text(features, "molecule_a")
        assert len(lab_feature_a) == 11  # inherited fields (identifier, entity_type, timestamp), proper fields (name, ontology_resource, data_type, unit, visibility, datasets, description, domain)
        assert Resource.IDENTIFIER_ in lab_feature_a
        assert lab_feature_a[Feature.NAME_] == "molecule_a"
        assert lab_feature_a[Feature.ONTO_] == {
            OntologyResource.SYSTEM_: Ontologies.LOINC["url"],
            OntologyResource.CODE_: "1234",
            OntologyResource.LABEL_: DEFAULT_ONTOLOGY_RESOURCE_LABEL
            # this resource does not exist for real in LOINC, thus display is empty
        }
        assert lab_feature_a[Feature.DT_] == DataTypes.FLOAT
        assert lab_feature_a[Feature.UNIT_] == "mg/L"
        assert lab_feature_a[Feature.VISIBILITY_] == Visibility.PUBLIC
        assert lab_feature_a[Feature.ENTITY_TYPE_] == f"{Profile.CLINICAL}{TableNames.FEATURE}"
        assert lab_feature_a[Feature.DESCR_] == "The molecule Alpha"
        assert lab_feature_a[Feature.DOMAIN_] == {"min": 0}
        # timestamp is not tested

        # LabFeature about molecule_b
        lab_feature_b = get_feature_by_text(features, "molecule_b")
        assert len(lab_feature_b) == 9
        assert Resource.IDENTIFIER_ in lab_feature_b
        assert lab_feature_b[Feature.NAME_] == "molecule_b"
        assert Feature.ONTO_ not in lab_feature_b
        assert lab_feature_b[Feature.DT_] == DataTypes.INTEGER
        assert lab_feature_b[Feature.UNIT_] == "g"  # unit is gram
        assert lab_feature_b[Feature.ENTITY_TYPE_] == f"{Profile.CLINICAL}{TableNames.FEATURE}"
        assert lab_feature_b[Feature.DESCR_] == "The molecule Beta"
        assert Feature.DOMAIN_ not in lab_feature_b

        # check that there are no duplicates in SamFeature instances
        # for this, we get the set of their names (in the field "text")
        lab_features_names_list = []
        for lab_feature in features:
            if lab_feature is not None:
                lab_features_names_list.append(lab_feature[Feature.NAME_])
        lab_features_names_set = set(lab_features_names_list)
        assert len(lab_features_names_list) == len(lab_features_names_set)

        # CHECK RECORDS
        records = get_transform_records(profile=Profile.CLINICAL)
        log.info(records)
        assert len(records) == 23  # in total, 16 ClinicalRecord instances are created, between 2 and 5 per Patient

        # assert that ClinicalRecord instances have been correctly created for a given data row
        # we take the seventh row
        patient_id = transform.patient_ids_mapping["999999994"]
        log.info(transform.patient_ids_mapping)
        assert patient_id == 6
        records_patient = get_records_for_patient(records=records, patient_id=patient_id)
        assert len(records_patient) == 2
        assert records_patient[0][Record.VALUE_] == -0.003  # the value as been converted to a float
        assert records_patient[0][Record.SUBJECT_] == patient_id
        assert records_patient[0][Record.REG_BY_] == 1  # Hospital:1
        assert records_patient[0][Record.INSTANTIATES_] == 12  # LabRecord 12 is about molecule_a (there are Hospital 1 and Dataset 2, and patients with normal anonymized pids)
        assert records_patient[0][Feature.ENTITY_TYPE_] == f"{Profile.CLINICAL}{TableNames.RECORD}"  #
        pattern_date = re.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2,3}Z")
        assert pattern_date.match(records_patient[0][Resource.TIMESTAMP_]["$date"])  # check the date is in datetime (CEST) form

        # check that all values are cast to the expected type
        assert records_patient[0][Record.VALUE_] == -0.003  # the value as been converted to an integer
        assert records_patient[1][Record.VALUE_] is False  # the value as been converted to a boolean

        # we also check that conversions str->int/float and category->bool worked
        assert get_field_value_for_patient(records=records, features=features, patient_id=transform.patient_ids_mapping["999999999"], column_name="molecule_b") == 100  # this has been cast as int because it matches the expected unit
        assert get_field_value_for_patient(records=records, features=features, patient_id=transform.patient_ids_mapping["999999998"], column_name="molecule_b") == 111  # this has been cast as int because it matches the expected unit
        assert get_field_value_for_patient(records=records, features=features, patient_id=transform.patient_ids_mapping["999999997"], column_name="molecule_b") == "231 grams"  # this has not been converted as this does not match the expected unit
        assert get_field_value_for_patient(records=records, features=features, patient_id=transform.patient_ids_mapping["999999996"], column_name="molecule_b") == 21  # this has been cast as int because it matches the expected unit
        assert get_field_value_for_patient(records=records, features=features, patient_id=transform.patient_ids_mapping["999999995"], column_name="molecule_b") == 100  # this has been cast as int even though there was no unit
        assert get_field_value_for_patient(records=records, features=features, patient_id=transform.patient_ids_mapping["999999994"], column_name="molecule_b") is None  # no value, thus no Record (thus None)
        assert pd.isnull(get_field_value_for_patient(records=records, features=features, patient_id=transform.patient_ids_mapping["999999993"], column_name="molecule_b"))  # null value converted to NaN
        assert get_field_value_for_patient(records=records, features=features, patient_id=transform.patient_ids_mapping["999999992"], column_name="molecule_b") == "116 kg"  # this has not been converted as this does not match the expected unit
        assert get_field_value_for_patient(records=records, features=features, patient_id=transform.patient_ids_mapping["999999991"], column_name="molecule_b") is None  # no value, thus no Record
        assert get_field_value_for_patient(records=records, features=features, patient_id=transform.patient_ids_mapping["999999990"], column_name="molecule_b") is None  # no value, thus no Record
    # TODO Nelly: check there are no duplicates for SamFeature instances

    def test_create_patients_without_pid(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1, profile=Profile.PHENOTYPIC,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_PHENOTYPIC_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_PHENOTYPIC_DATA_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_PHENOTYPIC_COL_CAT_PATH,
                             extracted_column_unit_path=TheTestFiles.EXTR_PHENOTYPIC_UNITS_PATH,
                             extracted_domain_path=TheTestFiles.EXTR_PHENOTYPIC_DOMAIN_PATH,
                             extracted_column_type_path=TheTestFiles.EXTR_PHENOTYPIC_TYPE_PATH)
        # before creating the patients, we need to erase the content of the patient file
        # that has been written by test_create_patients_without_pid
        # otherwise, it would count 20 patients instead of 10 (2 times)
        # we do not need to do this trick in the normal execution because each time a new working directory is created
        # thus, there are no overlapping files
        clear_file(current_working_dir=transform.execution.working_dir_current,
                   dataset_id=get_dataset_id_from_profile(profile=Profile.PHENOTYPIC),
                   table_name=TableNames.PATIENT)

        # this creates Patient resources (based on the data file) and insert them in a (JSON) temporary file
        transform.create_patients()

        patients = get_transform_patients(dataset_id=get_dataset_id_from_profile(profile=Profile.PHENOTYPIC))
        assert len(patients) == 10
        # we cannot simply order by identifier value because they are strings, not int
        # thus will need a bit more of processing to sort by the integer represented within the string
        sorted_patients = sorted(patients, key=lambda d: d[Resource.IDENTIFIER_])
        for i in range(0, len(sorted_patients)):
            # patients have their own anonymized ids
            assert sorted_patients[i][Resource.IDENTIFIER_] == i + 1

        # get back to the original file
        with open(self.execution.anonymized_patient_ids_filepath, "w") as f:
            f.write("{}")

    def test_create_patients_with_pid(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1, profile=Profile.PHENOTYPIC,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_PHENOTYPIC_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_PHENOTYPIC_DATA_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_PHENOTYPIC_COL_CAT_PATH,
                             extracted_column_unit_path=TheTestFiles.EXTR_PHENOTYPIC_UNITS_PATH,
                             extracted_domain_path=TheTestFiles.EXTR_PHENOTYPIC_DOMAIN_PATH,
                             extracted_column_type_path=TheTestFiles.EXTR_PHENOTYPIC_TYPE_PATH)
        # before creating the patients, we need to erase the content of the patient file
        # that has been written by test_create_patients_without_pid
        # otherwise, it would count 20 patients instead of 10 (2 times)
        # we do not need to do this trick in the normal execution because each time a new working directory is created
        # thus, there are no overlapping files
        clear_file(current_working_dir=transform.execution.working_dir_current,
                   dataset_id=get_dataset_id_from_profile(profile=Profile.PHENOTYPIC),
                   table_name=TableNames.PATIENT)

        # this creates Patient resources (based on the data file) and insert them in a (JSON) temporary file
        transform.load_patient_id_mapping()
        transform.create_patients()

        patients = get_transform_patients(dataset_id=get_dataset_id_from_profile(profile=Profile.PHENOTYPIC))
        assert len(patients) == 10
        # we cannot simply order by identifier value because they are strings, not int
        # thus will need a bit more of processing to sort by the integer represented within the string
        sorted_patients = sorted(patients, key=lambda d: d[Resource.IDENTIFIER_])
        # sorted_patients = sorted(transform.patients)
        log.info(patients)
        log.info(sorted_patients)
        for i in range(0, len(sorted_patients)):
            # patients have their own anonymized ids
            assert sorted_patients[i][Resource.IDENTIFIER_] == i + 1

    def test_create_ontology_resource_from_row(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1, profile=Profile.CLINICAL,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_CLINICAL_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_CLINICAL_DATA_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_CLINICAL_COL_CAT_PATH,
                             extracted_column_unit_path=TheTestFiles.EXTR_CLINICAL_UNITS_PATH,
                             extracted_domain_path=TheTestFiles.EXTR_CLINICAL_DOMAIN_PATH,
                             extracted_column_type_path=TheTestFiles.EXTR_CLINICAL_TYPE_PATH)
        # no associated ontology code
        onto_resource = transform.create_ontology_resource_from_row(column_name="molecule_b")
        assert onto_resource is None

        # one associated ontology code
        onto_resource = transform.create_ontology_resource_from_row(column_name="molecule_a")
        assert onto_resource.to_json() == OntologyResource(system=Ontologies.LOINC, code="1234", label=None, quality_stats=None).to_json()

    def test_ontology_resource(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1, profile=Profile.CLINICAL,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_CLINICAL_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_CLINICAL_DATA_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_CLINICAL_COL_CAT_PATH,
                             extracted_column_unit_path=TheTestFiles.EXTR_CLINICAL_UNITS_PATH,
                             extracted_column_type_path=TheTestFiles.EXTR_CLINICAL_TYPE_PATH,
                             extracted_domain_path=TheTestFiles.EXTR_CLINICAL_DOMAIN_PATH)
        # no associated ontology code (patient id line)
        first_row = transform.metadata.iloc[0]
        onto_resource = OntologyResource(system=Ontologies.get_enum_from_name(first_row[MetadataColumns.ONTO_NAME]),
                                         code=first_row[MetadataColumns.ONTO_CODE],
                                         label=None, quality_stats=None)
        assert onto_resource is not None
        assert onto_resource.system == {}
        assert onto_resource.code == ""
        assert onto_resource.label is None

        # one associated ontology code (molecule_g line)
        fourth = transform.metadata.iloc[4]
        onto_resource = OntologyResource(system=Ontologies.get_enum_from_name(fourth[MetadataColumns.ONTO_NAME]),
                                         code=fourth[MetadataColumns.ONTO_CODE],
                                         label=None, quality_stats=None)
        assert onto_resource is not None
        assert onto_resource.system == Ontologies.SNOMEDCT["url"]
        assert onto_resource.code == "421416008"
        assert onto_resource.label == "Gamma"

    def test_fairify_phen_value(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1, profile=Profile.PHENOTYPIC,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_PHENOTYPIC_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_PHENOTYPIC_DATA_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_PHENOTYPIC_COL_CAT_PATH,
                             extracted_column_unit_path=TheTestFiles.EXTR_PHENOTYPIC_UNITS_PATH,
                             extracted_column_type_path=TheTestFiles.EXTR_PHENOTYPIC_TYPE_PATH,
                             extracted_domain_path=TheTestFiles.EXTR_PHENOTYPIC_DOMAIN_PATH)

        assert transform.fairify_value(column_name="id", value=transform.data.iloc[0][0]) == "999999999"
        onto_resource = OntologyResource(system=Ontologies.SNOMEDCT, code="248152002", label=None, quality_stats=None)
        fairified_value = transform.fairify_value(column_name="sex", value=transform.data.iloc[0][1])
        assert fairified_value == onto_resource.to_json()
        assert transform.fairify_value(column_name="ethnicity", value=transform.data.iloc[0][2]) == "white"
        assert transform.fairify_value(column_name="date_of_birth", value=transform.data.iloc[0][3]) == ""  # no date here, we verify this returns empty string
        assert transform.fairify_value(column_name="date_of_birth", value=transform.data.iloc[5][3]) == cast_str_to_datetime(str_value="2021-12-22 11:58:38.881")

    def test_fairify_clin_value(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1, profile=Profile.CLINICAL,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_CLINICAL_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_CLINICAL_DATA_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_CLINICAL_COL_CAT_PATH,
                             extracted_column_type_path=TheTestFiles.EXTR_CLINICAL_TYPE_PATH,
                             extracted_column_unit_path=TheTestFiles.EXTR_CLINICAL_UNITS_PATH,
                             extracted_domain_path=TheTestFiles.EXTR_CLINICAL_DOMAIN_PATH)

        assert transform.fairify_value(column_name="sid", value=transform.data.iloc[0][0]) == "s1"
        assert transform.fairify_value(column_name="id", value=transform.data.iloc[0][1]) == "999999999"
        assert transform.fairify_value(column_name="molecule_a", value=transform.data.iloc[0][2]) == 0.001
        assert transform.fairify_value(column_name="molecule_b", value=transform.data.iloc[0][3]) == 100
        assert transform.fairify_value(column_name="molecule_g", value=transform.data.iloc[0][4]) is True
        assert transform.fairify_value(column_name="molecule_g", value=transform.data.iloc[4][4]) is False
        assert pd.isnull(transform.fairify_value(column_name="molecule_g", value=transform.data.iloc[6][4]))
        assert pd.isnull(transform.fairify_value(column_name="molecule_g", value=transform.data.iloc[7][4]))

    def test_patient_id_mapping(self):
        # test with no patient ID mapping
        # the mapping filename HAS TO BE "anonymized_patient_ids.json"
        delete_pid_file()
        extract = my_setup(hospital_name=HospitalNames.TEST_H1, profile=Profile.CLINICAL,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_CLINICAL_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_CLINICAL_DATA_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_CLINICAL_COL_CAT_PATH,
                             extracted_column_unit_path=TheTestFiles.EXTR_CLINICAL_UNITS_PATH,
                           extracted_domain_path=TheTestFiles.EXTR_CLINICAL_DOMAIN_PATH,
                           extracted_column_type_path=TheTestFiles.EXTR_CLINICAL_TYPE_PATH)
        extract.load_patient_id_mapping()

        # when the file is empty, the Execution should write an empty list into it
        assert os.stat(extract.execution.anonymized_patient_ids_filepath).st_size > 0
        assert extract.patient_ids_mapping == {}

        # test with non-empty patient ID mapping
        create_pid_file()
        transform = my_setup(hospital_name=HospitalNames.TEST_H1, profile=Profile.CLINICAL,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_CLINICAL_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_CLINICAL_DATA_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_CLINICAL_COL_CAT_PATH,
                             extracted_column_unit_path=TheTestFiles.EXTR_CLINICAL_UNITS_PATH,
                             extracted_domain_path=TheTestFiles.EXTR_CLINICAL_DOMAIN_PATH,
                             extracted_column_type_path=TheTestFiles.EXTR_CLINICAL_TYPE_PATH,
                             db_drop="False")
        transform.load_patient_id_mapping()

        # when the file is not empty, all mappings should be loaded in Extract
        assert os.stat(transform.execution.anonymized_patient_ids_filepath).st_size > 0
        with open(os.path.join(DOCKER_FOLDER_TEST, TheTestFiles.PIDS_PATH), "r") as f:
            expected_dict = json.load(f)
        assert expected_dict == transform.patient_ids_mapping
        delete_pid_file()  # get back to the original setting where no PID is provided
