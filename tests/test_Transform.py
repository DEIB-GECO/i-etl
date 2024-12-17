import json
import os
import re
import unittest

import numpy as np
import pytest

from constants.defaults import DEFAULT_ONTOLOGY_RESOURCE_LABEL
from constants.structure import TEST_DB_NAME, DOCKER_FOLDER_TEST
from database.Counter import Counter
from database.Database import Database
from database.Execution import Execution
from datatypes.Identifier import Identifier
from datatypes.OntologyResource import OntologyResource
from entities.Hospital import Hospital
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
from statistics.TimeStatistics import TimeStatistics
from utils.assertion_utils import is_not_nan
from utils.cast_utils import cast_str_to_datetime
from utils.file_utils import get_json_resource_file
from utils.file_utils import read_tabular_file_as_string
from utils.setup_logger import log
from utils.test_utils import compare_tuples, set_env_variables_from_dict, get_feature_by_text, \
    get_field_value_for_patient, get_records_for_patient


# personalized setup called at the beginning of each test
def my_setup(hospital_name: str, profile: str, extracted_metadata_path: str, extracted_data_paths: str,
             extracted_mapping_categorical_values_path: str,
             extracted_column_to_categorical_path: str,
             extracted_column_unit_path: str,
             extracted_patient_ids_mapping_path: str) -> Transform:
    args = {
        ParameterKeys.DB_NAME: TEST_DB_NAME,
        ParameterKeys.DB_DROP: "True",
        ParameterKeys.HOSPITAL_NAME: hospital_name,
        ParameterKeys.ANONYMIZED_PATIENT_IDS: extracted_patient_ids_mapping_path
        # no need to set the metadata and data filepaths as we get already the loaded data and metadata as arguments
    }
    set_env_variables_from_dict(env_vars=args)
    TestTransform.execution.internals_set_up()
    TestTransform.execution.file_set_up(setup_files=False)  # no need to set up the files, we get data and metadata as input
    database = Database(TestTransform.execution)
    # I load:
    # - the data and metadata from two CSV files that I obtained by running the Extract step
    # - and mapped_values as a JSON file that I obtained from the same Extract object
    metadata = read_tabular_file_as_string(os.path.join(DOCKER_FOLDER_TEST, extracted_metadata_path))
    data = read_tabular_file_as_string(os.path.join(DOCKER_FOLDER_TEST, extracted_data_paths))
    with open(os.path.join(DOCKER_FOLDER_TEST, extracted_mapping_categorical_values_path), "r") as f:
        mapping_categorical_values = json.load(f)
    with open(os.path.join(DOCKER_FOLDER_TEST, extracted_column_to_categorical_path), "r") as f:
        mapping_column_to_categorical_value = json.load(f)
    with open(os.path.join(DOCKER_FOLDER_TEST, extracted_column_unit_path), "r") as f:
        column_to_unit = json.load(f)

    transform = Transform(database=database, execution=TestTransform.execution, data=data, metadata=metadata,
                          profile=profile, dataset_number=get_dataset_number_from_profile(profile), file_counter=1,
                          mapping_categorical_value_to_onto_resource=mapping_categorical_values,
                          mapping_column_to_categorical_value=mapping_column_to_categorical_value,
                          mapping_column_to_unit=column_to_unit, load_patients=True,
                          quality_stats=QualityStatistics(record_stats=False), time_stats=TimeStatistics(record_stats=False))

    # create a hospital instance, to be able to create records with references to the hospital
    transform.database.insert_one_tuple(table_name=TableNames.HOSPITAL, one_tuple=Hospital(name=HospitalNames.TEST_H1, counter=Counter()).to_json())
    counter = Counter()
    counter.set_with_database(database=transform.database)
    return transform


def get_dataset_number_from_profile(profile: str):
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
    get_back_to_original_pid_files()


def get_back_to_original_pid_files():
    with open(os.path.join(DOCKER_FOLDER_TEST, TheTestFiles.ORIG_EMPTY_PIDS_PATH), "w") as f:
        f.write(json.dumps({}))
    with open(os.path.join(DOCKER_FOLDER_TEST, TheTestFiles.EXTR_EMPTY_PIDS_PATH), "w") as f:
        f.write(json.dumps({}))
    original_filled_pids = json.dumps({
                            "999999999": "h1:999",
                            "999999998": "h1:998",
                            "999999997": "h1:997",
                            "999999996": "h1:996",
                            "999999995": "h1:995",
                            "999999994": "h1:994",
                            "999999993": "h1:993",
                            "999999992": "h1:992",
                            "999999991": "h1:991",
                            "999999990": "h1:990"
                            })
    with open(os.path.join(DOCKER_FOLDER_TEST, TheTestFiles.ORIG_FILLED_PIDS_PATH), "w") as f:
        f.write(original_filled_pids)
    with open(os.path.join(DOCKER_FOLDER_TEST, TheTestFiles.EXTR_FILLED_PIDS_PATH), "w") as f:
        f.write(original_filled_pids)


class TestTransform(unittest.TestCase):
    execution = Execution()

    def test_set_resource_counter_id(self):
        # when there is nothing in the database, the counter should be 0
        transform = my_setup(hospital_name=HospitalNames.TEST_H1, profile=Profile.PHENOTYPIC,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_PHENOTYPIC_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_PHENOTYPIC_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_PHENOTYPIC_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_PHENOTYPIC_COL_CAT_PATH,
                             extracted_column_unit_path=TheTestFiles.EXTR_PHENOTYPIC_UNITS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH)
        log.info(transform.database.check_table_exists(TableNames.RECORD))
        log.info(transform.database.check_table_exists(TableNames.FEATURE))
        log.info(transform.database.check_table_exists(TableNames.PATIENT))
        log.info(transform.database.check_table_exists(TableNames.HOSPITAL))
        counter = Counter()
        counter.set_with_database(database=transform.database)

        assert transform.counter.resource_id == 0

        # when some tables already contain resources, it should not be 0
        # I manually insert some resources in the database
        database = Database(TestTransform.execution)
        my_tuples = [
            {"identifier": Identifier(id_value=1).to_json()},
            {"identifier": Identifier(id_value=2).to_json()},
            {"identifier": Identifier(id_value=7).to_json()},
            {"identifier": Identifier(id_value=3).to_json()},
            {"identifier": Identifier(id_value=124).to_json()},
            {"identifier": Identifier(id_value=9).to_json()},
            {"identifier": Identifier(id_value=123).to_json()},
            {"identifier": Identifier(id_value=6).to_json()}
        ]
        database.insert_many_tuples(table_name=TableNames.TEST, tuples=my_tuples)
        counter.set_with_database(database=transform.database)
        assert counter.resource_id == 125

    def test_phenotypic_data(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1, profile=Profile.PHENOTYPIC,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_PHENOTYPIC_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_PHENOTYPIC_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_PHENOTYPIC_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_PHENOTYPIC_COL_CAT_PATH,
                             extracted_column_unit_path=TheTestFiles.EXTR_PHENOTYPIC_UNITS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH)
        transform.create_patients()
        transform.counter.set_with_database(database=transform.database)
        transform.create_features()
        transform.counter.set_with_database(database=transform.database)
        transform.create_records()

        # CHECK FEATURES
        assert len(transform.features) == 4 - 1  # id does not count as a PhenFeature
        # assert the fourth and first PhenFeature instances:
        # lab_feature_a has one associated code
        # lab_feature_b has no associated code
        # because they may not be in the same order as in the metadata file, we get them based on their text
        # (which contains at least the column name, and maybe a description)
        # PhenFeature about sex
        lab_feature_a = get_feature_by_text(transform.features, "sex")
        assert len(
            lab_feature_a) == 8  # inherited fields (identifier, entity_type, timestamp), proper fields (name, ontology_resource, permitted_datatype, unit, visibility)
        assert "identifier" in lab_feature_a
        assert lab_feature_a["name"] == "sex"
        assert lab_feature_a["ontology_resource"] == {
            "system": Ontologies.SNOMEDCT["url"],
            "code": "123:789",
            "label": f"{DEFAULT_ONTOLOGY_RESOURCE_LABEL}:{DEFAULT_ONTOLOGY_RESOURCE_LABEL}"
            # the two codes do not exist for eal in SNOMED, thus using the empty label
        }
        assert lab_feature_a["datatype"] == DataTypes.CATEGORY
        assert "unit" not in lab_feature_a
        assert lab_feature_a["entity_type"] == f"{Profile.PHENOTYPIC}{TableNames.FEATURE}"

        # PhenFeature about date_of_birth
        lab_feature_b = get_feature_by_text(transform.features, "date_of_birth")
        assert len(lab_feature_b) == 6  # no ontology resource and no unit
        assert "identifier" in lab_feature_b
        assert lab_feature_b["name"] == "date_of_birth"
        assert "ontology_resource" not in lab_feature_b
        assert lab_feature_b["datatype"] == DataTypes.DATETIME
        assert "unit" not in lab_feature_b
        assert lab_feature_b["entity_type"] == f"{Profile.PHENOTYPIC}{TableNames.FEATURE}"

        # check that there are no duplicates in PhenFeature instances
        # for this, we get the set of their names (in the field "text")
        lab_features_names_list = []
        for lab_feature in transform.features:
            if lab_feature is not None:
                lab_features_names_list.append(lab_feature.name)
        lab_features_names_set = set(lab_features_names_list)
        assert len(lab_features_names_list) == len(lab_features_names_set)

        # CHECK RECORDS
        assert len(transform.records) == 17  # in total, 17 PhenRecord instances are created, between 2 and 5 per Patient
        # assert that PhenRecord instances have been correctly created for a given data row
        # we take the seventh row
        patient_id = transform.patient_ids_mapping["999999994"]
        assert patient_id == "h1:6"  # patient anonymized IDs start at h1:1, because no hospital has been created beforehand.
        phen_records_patient = get_records_for_patient(records=transform.records, patient_id=patient_id)
        female = OntologyResource(ontology=Ontologies.SNOMEDCT, full_code="248152002", label=None, quality_stats=None)
        assert phen_records_patient[0]["value"] == female.to_json()  # the value as been replaced by its ontology code (sex is a categorical value)
        assert phen_records_patient[1]["value"] == "black"
        assert phen_records_patient[2]["value"] == {"$date": "2021-12-01T00:00:00Z"}  # the value as been converted to a MongoDB-style datetime and anonymized (remove day and time)
        pattern_date = re.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2,3}Z")
        assert pattern_date.match(phen_records_patient[0]["timestamp"]["$date"])  # check the date is in datetime (CEST) form

        # we also check that string are normalized (no caps, etc.)
        recs = transform.records
        feats = transform.features
        assert transform.patient_ids_mapping["999999996"] == "h1:4"  # 0 Hospital before patients
        assert get_field_value_for_patient(records=recs, features=feats, patient_id=transform.patient_ids_mapping["999999999"], column_name="ethnicity") == "white"
        assert get_field_value_for_patient(records=recs, features=feats, patient_id=transform.patient_ids_mapping["999999998"], column_name="ethnicity") == "white"
        assert get_field_value_for_patient(records=recs, features=feats, patient_id=transform.patient_ids_mapping["999999997"], column_name="ethnicity") == "white"
        assert get_field_value_for_patient(records=recs, features=feats, patient_id=transform.patient_ids_mapping["999999996"], column_name="ethnicity") == "caucasian"
        assert get_field_value_for_patient(records=recs, features=feats, patient_id=transform.patient_ids_mapping["999999995"], column_name="ethnicity") is None
        assert get_field_value_for_patient(records=recs, features=feats, patient_id=transform.patient_ids_mapping["999999994"], column_name="ethnicity") == "black"
        assert get_field_value_for_patient(records=recs, features=feats, patient_id=transform.patient_ids_mapping["999999993"], column_name="ethnicity") == "caucasian"
        assert get_field_value_for_patient(records=recs, features=feats, patient_id=transform.patient_ids_mapping["999999992"], column_name="ethnicity") == "italian"
        assert get_field_value_for_patient(records=recs, features=feats, patient_id=transform.patient_ids_mapping["999999991"], column_name="ethnicity") is None
        assert get_field_value_for_patient(records=recs, features=feats, patient_id=transform.patient_ids_mapping["999999990"], column_name="ethnicity") is None

    def test_clinical_data(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1, profile=Profile.CLINICAL,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_CLINICAL_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_CLINICAL_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_CLINICAL_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_CLINICAL_COL_CAT_PATH,
                             extracted_column_unit_path=TheTestFiles.EXTR_CLINICAL_UNITS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_FILLED_PIDS_PATH)
        transform.load_patient_id_mapping()
        transform.create_patients()
        transform.counter.set_with_database(database=transform.database)
        transform.create_features()
        transform.counter.set_with_database(database=transform.database)
        transform.create_records()

        # CHECK FEATURES
        assert len(transform.features) == 6 - 2  # sid and id do not count as SamFeatures
        # assert the third and fourth SamFeature instances:
        # lab_feature_a has one associated code
        # lab_feature_b has no associated code
        # because they may not be in the same order as in the metadata file, we get them based on their text
        # (which contains at least the column name, and maybe a description)
        # SamFeature about molecule_a
        lab_feature_a = get_feature_by_text(transform.features, "molecule_a")
        assert len(
            lab_feature_a) == 8  # inherited fields (identifier, entity_type, timestamp), proper fields (original_name, ontology_resource, permitted_datatype, unit, visibility)
        assert "identifier" in lab_feature_a
        assert lab_feature_a["name"] == "molecule_a"
        assert lab_feature_a["ontology_resource"] == {
            "system": Ontologies.LOINC["url"],
            "code": "1234",
            "label": DEFAULT_ONTOLOGY_RESOURCE_LABEL
            # this resource does not exist for real in LOINC, thus display is empty
        }
        assert lab_feature_a["datatype"] == DataTypes.FLOAT
        assert lab_feature_a["unit"] == "mg/L"
        assert lab_feature_a["visibility"] == Visibility.PUBLIC
        assert lab_feature_a["entity_type"] == f"{Profile.CLINICAL}{TableNames.FEATURE}"
        # timestamp is not tested

        # LabFeature about molecule_b
        lab_feature_b = get_feature_by_text(transform.features, "molecule_b")
        assert len(lab_feature_b) == 7
        assert "identifier" in lab_feature_b
        assert lab_feature_b["name"] == "molecule_b"
        assert "ontology_resource" not in lab_feature_b
        assert lab_feature_b["datatype"] == DataTypes.INTEGER
        assert lab_feature_b["unit"] == "g"  # unit is gram
        assert lab_feature_b["entity_type"] == f"{Profile.CLINICAL}{TableNames.FEATURE}"

        # check that there are no duplicates in SamFeature instances
        # for this, we get the set of their names (in the field "text")
        lab_features_names_list = []
        for lab_feature in transform.features:
            if lab_feature is not None:
                lab_features_names_list.append(lab_feature.name)
        lab_features_names_set = set(lab_features_names_list)
        assert len(lab_features_names_list) == len(lab_features_names_set)

        # CHECK RECORDS
        assert len(transform.records) == 16  # in total, 16 ClinicalRecord instances are created, between 2 and 5 per Patient

        # assert that ClinicalRecord instances have been correctly created for a given data row
        # we take the seventh row
        patient_id = transform.patient_ids_mapping["999999994"]
        assert patient_id == "h1:994"
        records = get_records_for_patient(records=transform.records, patient_id=patient_id)
        log.info(records)
        hospitals = transform.database.find_operation(table_name=TableNames.HOSPITAL, filter_dict={}, projection={})
        log.info("existing hospitals")
        for h in hospitals:
            log.info(h)
        log.info("---")
        patients = transform.database.find_operation(table_name=TableNames.PATIENT, filter_dict={}, projection={})
        log.info("existing patients")
        for p in patients:
            log.info(p)
        log.info("---")
        assert len(records) == 2
        assert records[0]["value"] == -0.003  # the value as been converted to a float
        assert records[0]["has_subject"] == str(patient_id)  # this has not been converted to an integer
        assert records[0]["registered_by"] == 1  # Hospital:1
        assert records[0]["instantiates"] == 2  # LabRecord 2 is about molecule_a
        assert records[0]["entity_type"] == f"{Profile.CLINICAL}{TableNames.RECORD}"  #
        pattern_date = re.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2,3}Z")
        assert pattern_date.match(records[0]["timestamp"]["$date"])  # check the date is in datetime (CEST) form

        # check that all values are cast to the expected type
        assert records[0]["value"] == -0.003  # the value as been converted to an integer
        assert records[1]["value"] is False  # the value as been converted to a boolean

        # we also check that conversions str->int/float and category->bool worked
        recs = transform.records
        feats = transform.features
        assert get_field_value_for_patient(records=recs, features=feats, patient_id=transform.patient_ids_mapping["999999999"], column_name="molecule_b") == 100  # this has been cast as int because it matches the expected unit
        assert get_field_value_for_patient(records=recs, features=feats, patient_id=transform.patient_ids_mapping["999999998"], column_name="molecule_b") == 111  # this has been cast as int because it matches the expected unit
        assert get_field_value_for_patient(records=recs, features=feats, patient_id=transform.patient_ids_mapping["999999997"], column_name="molecule_b") == "231 grams"  # this has not been converted as this does not match the expected unit
        assert get_field_value_for_patient(records=recs, features=feats, patient_id=transform.patient_ids_mapping["999999996"], column_name="molecule_b") == 21  # this has been cast as int because it matches the expected unit
        assert get_field_value_for_patient(records=recs, features=feats, patient_id=transform.patient_ids_mapping["999999995"], column_name="molecule_b") == 100  # this has been cast as int even though there was no unit
        assert get_field_value_for_patient(records=recs, features=feats, patient_id=transform.patient_ids_mapping["999999994"], column_name="molecule_b") is None  # no value
        assert get_field_value_for_patient(records=recs, features=feats, patient_id=transform.patient_ids_mapping["999999993"], column_name="molecule_b") is None  # null value
        assert get_field_value_for_patient(records=recs, features=feats, patient_id=transform.patient_ids_mapping["999999992"], column_name="molecule_b") == "116 kg"  # this has not been converted as this does not match the expected unit
    # TODO Nelly: check there are no duplicates for SamFeature instances

    def test_create_patients_without_pid(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1, profile=Profile.PHENOTYPIC,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_PHENOTYPIC_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_PHENOTYPIC_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_PHENOTYPIC_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_PHENOTYPIC_COL_CAT_PATH,
                             extracted_column_unit_path=TheTestFiles.EXTR_PHENOTYPIC_UNITS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH)
        # this creates Patient resources (based on the data file) and insert them in a (JSON) temporary file
        transform.create_patients()

        assert len(transform.patients) == 10
        # we cannot simply order by identifier value because they are strings, not int
        # thus will need a bit more of processing to sort by the integer represented within the string
        # sorted_patients = sorted(transform.patients, key=lambda d: d.to_json()["identifier"]["value"])
        sorted_patients = sorted(transform.patients)
        for i in range(0, len(sorted_patients)):
            # patients have their own anonymized ids
            assert sorted_patients[i].to_json()["identifier"] == Identifier(id_value=i + 1).value

        # get back to the original file
        with open(self.execution.anonymized_patient_ids_filepath, "w") as f:
            f.write("{}")

    def test_create_patients_with_pid(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1, profile=Profile.PHENOTYPIC,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_PHENOTYPIC_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_PHENOTYPIC_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_PHENOTYPIC_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_PHENOTYPIC_COL_CAT_PATH,
                             extracted_column_unit_path=TheTestFiles.EXTR_PHENOTYPIC_UNITS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_FILLED_PIDS_PATH)
        # this creates Patient resources (based on the data file) and insert them in a (JSON) temporary file
        transform.load_patient_id_mapping()
        transform.create_patients()

        assert len(transform.patients) == 10
        # we cannot simply order by identifier value because they are strings, not int
        # thus will need a bit more of processing to sort by the integer represented within the string
        # sorted_patients = sorted(transform.patients, key=lambda d: d.to_json()["identifier"]["value"])
        sorted_patients = sorted(transform.patients)
        for i in range(0, len(sorted_patients)):
            # patients have their own anonymized ids
            assert sorted_patients[i].to_json()["identifier"] == Identifier(id_value=990 + i).value

    def test_create_ontology_resource_from_row(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1, profile=Profile.CLINICAL,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_CLINICAL_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_CLINICAL_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_CLINICAL_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_CLINICAL_COL_CAT_PATH,
                             extracted_column_unit_path=TheTestFiles.EXTR_CLINICAL_UNITS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH)
        # no associated ontology code
        onto_resource = transform.create_ontology_resource_from_row(column_name="molecule_b")
        assert onto_resource is None

        # one associated ontology code
        onto_resource = transform.create_ontology_resource_from_row(column_name="molecule_a")
        assert onto_resource.to_json() == OntologyResource(ontology=Ontologies.LOINC, full_code="1234", label=None, quality_stats=None).to_json()

    def test_ontology_resource(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1, profile=Profile.CLINICAL,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_CLINICAL_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_CLINICAL_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_CLINICAL_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_CLINICAL_COL_CAT_PATH,
                             extracted_column_unit_path=TheTestFiles.EXTR_CLINICAL_UNITS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH)
        # no associated ontology code (patient id line)
        first_row = transform.metadata.iloc[0]
        onto_resource = OntologyResource(ontology=Ontologies.get_enum_from_name(first_row[MetadataColumns.ONTO_NAME]),
                                         full_code=first_row[MetadataColumns.ONTO_CODE],
                                         label=None, quality_stats=None)
        assert onto_resource is not None
        assert onto_resource.label is None
        assert onto_resource.system is None
        assert onto_resource.code is None

        # one associated ontology code (molecule_g line)
        fourth = transform.metadata.iloc[4]
        onto_resource = OntologyResource(ontology=Ontologies.get_enum_from_name(fourth[MetadataColumns.ONTO_NAME]),
                                         full_code=fourth[MetadataColumns.ONTO_CODE],
                                         label=None, quality_stats=None)
        assert onto_resource is not None
        assert onto_resource.system == Ontologies.SNOMEDCT["url"]
        assert onto_resource.code == "421416008"
        assert onto_resource.label == "Gamma"

    def test_fairify_phen_value(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1, profile=Profile.PHENOTYPIC,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_PHENOTYPIC_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_PHENOTYPIC_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_PHENOTYPIC_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_PHENOTYPIC_COL_CAT_PATH,
                             extracted_column_unit_path=TheTestFiles.EXTR_PHENOTYPIC_UNITS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH)

        assert transform.fairify_value(column_name="id", value=transform.data.iloc[0][0]) == "999999999"
        onto_resource = OntologyResource(ontology=Ontologies.SNOMEDCT, full_code="248152002", label=None, quality_stats=None)
        fairified_value = transform.fairify_value(column_name="sex", value=transform.data.iloc[0][1])
        assert fairified_value == onto_resource.to_json()
        assert transform.fairify_value(column_name="ethnicity", value=transform.data.iloc[0][2]) == "white"
        assert not is_not_nan(transform.fairify_value(column_name="date_of_birth", value=transform.data.iloc[0][3]))  # no date here, we verify this is NaN
        assert transform.fairify_value(column_name="date_of_birth", value=transform.data.iloc[5][3]) == cast_str_to_datetime(str_value="2021-12-22 11:58:38.881")

    def test_fairify_clin_value(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1, profile=Profile.CLINICAL,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_CLINICAL_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_CLINICAL_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_CLINICAL_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_CLINICAL_COL_CAT_PATH,
                             extracted_column_unit_path=TheTestFiles.EXTR_CLINICAL_UNITS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH)

        assert transform.fairify_value(column_name="sid", value=transform.data.iloc[0][0]) == "s1"
        assert transform.fairify_value(column_name="id", value=transform.data.iloc[0][1]) == "999999999"
        assert transform.fairify_value(column_name="molecule_a", value=transform.data.iloc[0][2]) == 0.001
        assert transform.fairify_value(column_name="molecule_b", value=transform.data.iloc[0][3]) == 100
        assert transform.fairify_value(column_name="molecule_g", value=transform.data.iloc[0][4]) is True
        assert transform.fairify_value(column_name="molecule_g", value=transform.data.iloc[4][4]) is False
        # not variable == np.nan (https://stackoverflow.com/questions/44367557/why-does-assert-np-nan-np-nan-cause-an-error)
        assert np.isnan(transform.fairify_value(column_name="molecule_g", value=transform.data.iloc[6][4]))
        assert np.isnan(transform.fairify_value(column_name="molecule_g", value=transform.data.iloc[7][4]))

    def test_load_empty_patient_id_mapping(self):
        extract = my_setup(hospital_name=HospitalNames.TEST_H1, profile=Profile.CLINICAL,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_CLINICAL_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_CLINICAL_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_CLINICAL_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_CLINICAL_COL_CAT_PATH,
                             extracted_column_unit_path=TheTestFiles.EXTR_CLINICAL_UNITS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.ORIG_EMPTY_PIDS_PATH)
        extract.load_patient_id_mapping()

        # when the file is empty, the Execution should write an empty list into it
        assert os.stat(extract.execution.anonymized_patient_ids_filepath).st_size > 0
        assert extract.patient_ids_mapping == {}

        # get back to the original empty file
        with open(os.path.join(DOCKER_FOLDER_TEST, TheTestFiles.ORIG_EMPTY_PIDS_PATH), "w") as file:
            file.write("")

    def test_load_filled_patient_id_mapping(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1, profile=Profile.CLINICAL,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_CLINICAL_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_CLINICAL_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_CLINICAL_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_CLINICAL_COL_CAT_PATH,
                             extracted_column_unit_path=TheTestFiles.EXTR_CLINICAL_UNITS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.ORIG_FILLED_PIDS_PATH)
        transform.load_patient_id_mapping()

        # when the file is not empty, all mappings should be loaded in Extract
        assert os.stat(transform.execution.anonymized_patient_ids_filepath).st_size > 0
        with open(os.path.join(DOCKER_FOLDER_TEST, TheTestFiles.EXTR_FILLED_PIDS_PATH), "r") as f:
            # {
            #    "999999999": "h1:999",
            #    "999999998": "h1:998",
            #    "999999997": "h1:997",
            #    "999999996": "h1:996",
            #    "999999995": "h1:995",
            #    "999999994": "h1:994",
            #    "999999993": "h1:993",
            #    "999999992": "h1:992",
            #    "999999991": "h1:991",
            #    "999999990": "h1:990"
            # }
            expected_dict = json.load(f)
        assert expected_dict == transform.patient_ids_mapping
