import json
import os
import re
import unittest

import numpy as np
import pytest

from database.Database import Database
from database.Execution import Execution
from datatypes.CodeableConcept import CodeableConcept
from datatypes.Coding import Coding
from datatypes.OntologyResource import OntologyResource
from datatypes.PatientAnonymizedIdentifier import PatientAnonymizedIdentifier
from enums.DataTypes import DataTypes
from enums.HospitalNames import HospitalNames
from enums.LabFeatureCategories import LabFeatureCategories
from enums.MetadataColumns import MetadataColumns
from enums.Ontologies import Ontologies
from enums.ParameterKeys import ParameterKeys
from enums.TableNames import TableNames
from enums.TheTestFiles import TheTestFiles
from etl.Transform import Transform
from profiles.Hospital import Hospital
from constants.structure import TEST_DB_NAME, DOCKER_FOLDER_TEST
from constants.defaults import DEFAULT_CODING_DISPLAY
from utils.setup_logger import log
from utils.utils import (compare_tuples, get_json_resource_file, get_lab_feature_by_text, is_not_nan,
                         read_tabular_file_as_string, get_field_value_for_patient, get_lab_records_for_patient,
                         set_env_variables_from_dict, cast_str_to_datetime)


# personalized setup called at the beginning of each test
def my_setup(hospital_name: str, extracted_metadata_path: str, extracted_data_paths: str,
             extracted_mapping_categorical_values_path: str,
             extracted_column_to_categorical_path: str,
             extracted_column_dimension_path: str,
             extracted_patient_ids_mapping_path: str,
             extracted_diagnosis_classification_path: str,
             extracted_mapping_diagnosis_to_cc_path: str) -> Transform:
    args = {
        ParameterKeys.DB_NAME: TEST_DB_NAME,
        ParameterKeys.DB_DROP: "True",
        ParameterKeys.HOSPITAL_NAME: hospital_name,
        ParameterKeys.ANONYMIZED_PATIENT_IDS: extracted_patient_ids_mapping_path
        # no need to set the metadata and data filepaths as we get already the loaded data and metadata as arguments
    }
    set_env_variables_from_dict(env_vars=args)
    TestTransform.execution.set_up(setup_data_files=False)  # no need to setup the files, we get data and metadata as input
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
    with open(os.path.join(DOCKER_FOLDER_TEST, extracted_column_dimension_path), "r") as f:
        column_to_dimension = json.load(f)
    with open(os.path.join(DOCKER_FOLDER_TEST, extracted_patient_ids_mapping_path), "r") as f:
        patient_ids_mapping = json.load(f)
    with open(os.path.join(DOCKER_FOLDER_TEST, extracted_diagnosis_classification_path), "r") as f:
        diagnosis_classification = json.load(f)
    with open(os.path.join(DOCKER_FOLDER_TEST, extracted_mapping_diagnosis_to_cc_path), "r") as f:
        mapping_diagnosis_to_cc = json.load(f)
    transform = Transform(database=database, execution=TestTransform.execution, data=data, metadata=metadata,
                          mapping_categorical_value_to_cc=mapping_categorical_values,
                          mapping_column_to_categorical_value=mapping_column_to_categorical_value,
                          mapping_column_to_dimension=column_to_dimension,
                          patient_ids_mapping=patient_ids_mapping,
                          diagnosis_classification=diagnosis_classification,
                          mapping_diagnosis_to_cc=mapping_diagnosis_to_cc)
    return transform


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
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_LABORATORY_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_LABORATORY_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_LABORATORY_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_LABORATORY_COL_CAT_PATH,
                             extracted_column_dimension_path=TheTestFiles.EXTR_LABORATORY_DIMENSIONS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH,
                             extracted_diagnosis_classification_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_CLASSIFICATION_PATH,
                             extracted_mapping_diagnosis_to_cc_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_TO_CC_PATH)
        transform.set_resource_counter_id()

        assert transform.counter.resource_id == 0

        # when some tables already contain resources, it should not be 0
        # I manually insert some resources in the database
        database = Database(TestTransform.execution)
        my_tuples = [
            {"identifier": {"value": "h1_1"}},
            {"identifier": {"value": "h1_2"}},
            {"identifier": {"value": "h1_7"}},
            {"identifier": {"value": "h1_3"}},
            {"identifier": {"value": "h1_9"}},
            {"identifier": {"value": "h1_123a"}},
            {"identifier": {"value": "6"}},
            {"identifier": {"value": "123"}},
        ]
        database.insert_many_tuples(table_name=TableNames.TEST, tuples=my_tuples)
        transform.set_resource_counter_id()

        assert transform.counter.resource_id == 124

    def test_create_hospital(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_LABORATORY_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_LABORATORY_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_LABORATORY_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_LABORATORY_COL_CAT_PATH,
                             extracted_column_dimension_path=TheTestFiles.EXTR_LABORATORY_DIMENSIONS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH,
                             extracted_diagnosis_classification_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_CLASSIFICATION_PATH,
                             extracted_mapping_diagnosis_to_cc_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_TO_CC_PATH)
        # this creates a new Hospital resource and insert it in a (JSON) temporary file
        transform.create_hospital()

        # check that the in-memory hospital is correct
        assert len(transform.hospitals) == 1
        assert type(transform.hospitals[0]) is Hospital
        current_json_hospital = transform.hospitals[0].to_json()
        assert len(current_json_hospital.keys()) == 1 + 3  # name + inherited (identifier, resource_type, timestamp)
        assert current_json_hospital["identifier"]["value"] == "1"
        assert current_json_hospital["name"] == HospitalNames.TEST_H1
        assert current_json_hospital["timestamp"] is not None
        assert current_json_hospital["resource_type"] == TableNames.HOSPITAL

        # b. check that the in-file hospital is correct
        hospital_file = get_json_resource_file(current_working_dir=TestTransform.execution.working_dir_current, table_name=TableNames.HOSPITAL, count=1)
        with open(hospital_file) as f:
            written_hospitals = json.load(f)
        assert len(written_hospitals) == 1
        compare_tuples(original_tuple=current_json_hospital, inserted_tuple=written_hospitals[0])

    def test_create_lab_features_H1_D1(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_LABORATORY_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_LABORATORY_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_LABORATORY_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_LABORATORY_COL_CAT_PATH,
                             extracted_column_dimension_path=TheTestFiles.EXTR_LABORATORY_DIMENSIONS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH,
                             extracted_diagnosis_classification_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_CLASSIFICATION_PATH,
                             extracted_mapping_diagnosis_to_cc_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_TO_CC_PATH)
        # this creates LabFeature instances (based on the metadata file) and insert them in a (JSON) temporary file
        log.debug(transform.data.to_string())
        log.debug(transform.metadata.to_string())
        transform.create_laboratory_features()

        log.info(len(transform.laboratory_features))
        assert len(transform.laboratory_features) == 8 - 1  # id does not count as a LabFeature
        # assert the first, second and sixth LabFeature instances:
        # lab_feature_a has one associated code, and is clinical
        # lab_feature_b one has no associated code, and is clinical
        # lab_feature_ethnicity one has two associated codes, and is phenotypic
        # because they may not be in the same order as in the metadata file, we get them based on their text
        # (which contains at least the column name, and maybe a description)
        # LabFeature about molecule_a
        lab_feature_a = get_lab_feature_by_text(transform.laboratory_features, "molecule_a")
        assert len(lab_feature_a) == 8  # inherited fields (identifier, resource_type, timestamp), proper fields (code, permitted_datatype, dimension, category, visibility)
        assert "identifier" in lab_feature_a
        assert lab_feature_a["resource_type"] == TableNames.LABORATORY_FEATURE
        assert lab_feature_a["code"] == {
            "text": "molecule_a",
            "coding": [
                {
                    "system": Ontologies.LOINC["url"],
                    "code": "1234",
                    "display": DEFAULT_CODING_DISPLAY  # this resource does not exists for real in LOINC, thus display is empty
                }
            ]
        }
        assert lab_feature_a["category"] == LabFeatureCategories.get_clinical().to_json()
        assert lab_feature_a["permitted_datatype"] == DataTypes.FLOAT
        assert lab_feature_a["dimension"] == "mg/L"

        # LabFeature about molecule_b
        lab_feature_b = get_lab_feature_by_text(transform.laboratory_features, "molecule_b")
        assert len(lab_feature_b) == 8
        assert "identifier" in lab_feature_b
        assert lab_feature_b["resource_type"] == TableNames.LABORATORY_FEATURE
        assert lab_feature_b["code"] == {
            "text": "molecule_b",
            "coding": []
        }
        assert lab_feature_b["category"] == LabFeatureCategories.get_clinical().to_json()
        assert lab_feature_b["permitted_datatype"] == DataTypes.INTEGER
        assert lab_feature_b["dimension"] == "g"  # unit is gram

        # LabFeature about ethnicity
        # "loinc/46463-6" and "snomedct/397731000"
        lab_feature_ethnicity = get_lab_feature_by_text(transform.laboratory_features, "ethnicity")
        assert len(lab_feature_ethnicity) == 7  # only 6 (not 8) because dimension is None, thus not added
        assert "identifier" in lab_feature_ethnicity
        assert lab_feature_ethnicity["resource_type"] == TableNames.LABORATORY_FEATURE
        assert lab_feature_ethnicity["code"] == {
            "text": "ethnicity",
            "coding": [
                {
                    "system": Ontologies.LOINC["url"],
                    "code": "46463-6",
                    "display": "Race or ethnicity"
                }, {
                    "system": Ontologies.SNOMEDCT["url"],
                    "code": "397731000",
                    "display": "Ethnic group finding"
                }
            ]
        }
        assert lab_feature_ethnicity["category"] == LabFeatureCategories.get_phenotypic().to_json()
        assert lab_feature_ethnicity["permitted_datatype"] == DataTypes.STRING
        # assert lab_feature_ethnicity["dimension"] is None  # this field is not added as it is None

        # check that there are no duplicates in LabFeature instances
        # for this, we get the set of their names (in the field "text")
        lab_features_names_list = [lab_feature.code.text for lab_feature in transform.laboratory_features]
        lab_features_names_set = set(lab_features_names_list)
        assert len(lab_features_names_list) == len(lab_features_names_set)

    def test_create_samples(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_LABORATORY_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_LABORATORY_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_LABORATORY_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_LABORATORY_COL_CAT_PATH,
                             extracted_column_dimension_path=TheTestFiles.EXTR_LABORATORY_DIMENSIONS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH,
                             extracted_diagnosis_classification_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_CLASSIFICATION_PATH,
                             extracted_mapping_diagnosis_to_cc_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_TO_CC_PATH)
        # this creates LabFeature resources (based on the metadata file) and insert them in a (JSON) temporary file
        log.debug(transform.data.to_string())
        log.debug(transform.metadata.to_string())
        transform.create_samples()

        # TODO NELLY

    def test_create_lab_records_without_samples_without_pid(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_LABORATORY_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_LABORATORY_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_LABORATORY_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_LABORATORY_COL_CAT_PATH,
                             extracted_column_dimension_path=TheTestFiles.EXTR_LABORATORY_DIMENSIONS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH,
                             extracted_diagnosis_classification_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_CLASSIFICATION_PATH,
                             extracted_mapping_diagnosis_to_cc_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_TO_CC_PATH)
        # this loads references (Hospital+LabFeature resources), creates LabRecord resources (based on the data file) and insert them in a (JSON) temporary file
        log.debug(transform.data.to_string())
        log.debug(transform.metadata.to_string())
        transform.create_hospital()
        transform.create_laboratory_features()
        transform.create_patients()  # this step and the two above are required to create LabRecord instances
        transform.create_laboratory_records()

        log.debug(transform.mapping_hospital_to_hospital_id)
        log.debug(transform.mapping_column_to_labfeat_id)
        log.debug(transform.hospitals)
        log.debug(transform.laboratory_features)

        assert len(transform.mapping_hospital_to_hospital_id) == 1
        assert HospitalNames.TEST_H1 in transform.mapping_hospital_to_hospital_id
        log.debug(transform.laboratory_records)
        assert len(transform.laboratory_records) == 33  # in total, 33 LabRecord instances are created, between 2 and 5 per Patient

        # assert that LabRecord instances have been correctly created for a given data row
        # we take the seventh row
        log.debug(transform.laboratory_records)
        patient_id = transform.patient_ids_mapping["999999994"]
        assert patient_id == "h1:14"  # patient anonymized IDs start at h1:9, because 8 lab. feat. have been created beforehand.
        lab_records_patient = get_lab_records_for_patient(lab_records=transform.laboratory_records, patient_id=patient_id)
        log.debug(json.dumps(lab_records_patient))
        assert len(lab_records_patient) == 5
        assert lab_records_patient[0]["resource_type"] == TableNames.LABORATORY_RECORD
        assert lab_records_patient[0]["value"] == -0.003  # the value as been converted to an integer
        assert lab_records_patient[0]["subject"]["reference"] == str(patient_id)  # this has not been converted to an integer
        assert lab_records_patient[0]["recorded_by"]["reference"] == "1"
        assert lab_records_patient[0]["instantiate"]["reference"] == "2"  # LabRecord 2 is about molecule_a
        pattern_date = re.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2,3}Z")
        assert pattern_date.match(lab_records_patient[0]["timestamp"]["$date"])  # check the date is in datetime (CEST) form

        # check that all values are cast to the expected type
        assert lab_records_patient[0]["value"] == -0.003  # the value as been converted to an integer
        assert lab_records_patient[1]["value"] is False  # the value as been converted to a boolean
        assert lab_records_patient[3]["value"] == "black"
        assert lab_records_patient[4]["value"] == {"$date": "2021-12-22T11:58:38Z"}  # the value as been converted to a MongoDB-style datetime
        cc_female = CodeableConcept(original_name="f")
        cc_female.add_coding(one_coding=Coding(code=OntologyResource(ontology=Ontologies.SNOMEDCT, full_code="248152002"), display=None))
        assert lab_records_patient[2]["value"] == cc_female.to_json()  # the value as been replaced by its ontology code (sex is a categorical value)r

        # we also check that conversions str->int/float and category->bool worked
        lab_recs = transform.laboratory_records
        lab_feats = transform.laboratory_features
        assert transform.patient_ids_mapping["999999996"] == "h1:12"
        assert get_field_value_for_patient(lab_records=lab_recs, lab_features=lab_feats, patient_id=transform.patient_ids_mapping["999999999"], column_name="molecule_b") == 100  # this has been cast as int because it matches the expected unit
        assert get_field_value_for_patient(lab_records=lab_recs, lab_features=lab_feats, patient_id=transform.patient_ids_mapping["999999998"], column_name="molecule_b") == 111  # this has been cast as int because it matches the expected unit
        assert get_field_value_for_patient(lab_records=lab_recs, lab_features=lab_feats, patient_id=transform.patient_ids_mapping["999999997"], column_name="molecule_b") == "231 grams"  # this has not been converted as this does not match the expected dimension
        assert get_field_value_for_patient(lab_records=lab_recs, lab_features=lab_feats, patient_id=transform.patient_ids_mapping["999999996"], column_name="molecule_b") == 21  # this has been cast as int because it matches the expected unit
        assert get_field_value_for_patient(lab_records=lab_recs, lab_features=lab_feats, patient_id=transform.patient_ids_mapping["999999995"], column_name="molecule_b") == 100  # this has been cast as int even though there was no unit
        assert get_field_value_for_patient(lab_records=lab_recs, lab_features=lab_feats, patient_id=transform.patient_ids_mapping["999999994"], column_name="molecule_b") is None  # no value
        assert get_field_value_for_patient(lab_records=lab_recs, lab_features=lab_feats, patient_id=transform.patient_ids_mapping["999999993"], column_name="molecule_b") is None  # null value
        assert get_field_value_for_patient(lab_records=lab_recs, lab_features=lab_feats, patient_id=transform.patient_ids_mapping["999999992"], column_name="molecule_b") == "116 kg"  # this has not been converted as this does not match the expected dimension

    def test_create_lab_records_without_samples_with_pid(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_LABORATORY_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_LABORATORY_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_LABORATORY_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_LABORATORY_COL_CAT_PATH,
                             extracted_column_dimension_path=TheTestFiles.EXTR_LABORATORY_DIMENSIONS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_FILLED_PIDS_PATH,
                             extracted_diagnosis_classification_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_CLASSIFICATION_PATH,
                             extracted_mapping_diagnosis_to_cc_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_TO_CC_PATH)
        # this loads references (Hospital+LabFeature resources), creates LabRecord resources (based on the data file) and insert them in a (JSON) temporary file
        log.debug(transform.data.to_string())
        log.debug(transform.metadata.to_string())
        transform.create_hospital()
        transform.create_laboratory_features()
        transform.create_patients()  # this step and the two above are required to create LabRecord instances
        transform.create_laboratory_records()

        log.debug(transform.mapping_hospital_to_hospital_id)
        log.debug(transform.mapping_column_to_labfeat_id)
        log.debug(transform.hospitals)
        log.debug(transform.laboratory_features)

        assert len(transform.mapping_hospital_to_hospital_id) == 1
        assert HospitalNames.TEST_H1 in transform.mapping_hospital_to_hospital_id
        log.debug(transform.laboratory_records)
        assert len(transform.laboratory_records) == 33  # in total, 33 LabRecord instances are created, between 2 and 5 per Patient

        # assert that LabRecord instances have been correctly created for a given data row
        # we take the seventh row
        log.debug(transform.laboratory_records)
        patient_id = transform.patient_ids_mapping["999999994"]
        assert patient_id == "h1:994"
        lab_records_patient = get_lab_records_for_patient(lab_records=transform.laboratory_records, patient_id=patient_id)
        log.debug(json.dumps(lab_records_patient))
        assert len(lab_records_patient) == 5
        assert lab_records_patient[0]["resource_type"] == TableNames.LABORATORY_RECORD
        assert lab_records_patient[0]["value"] == -0.003  # the value as been converted to a float
        log.debug(lab_records_patient[0]["subject"]["reference"])
        log.debug(type(lab_records_patient[0]["subject"]["reference"]))
        log.debug(str(patient_id))
        log.debug(type(str(patient_id)))
        assert lab_records_patient[0]["subject"]["reference"] == str(patient_id)  # this has not been converted to an integer
        assert lab_records_patient[0]["recorded_by"]["reference"] == "1"
        assert lab_records_patient[0]["instantiate"]["reference"] == "2"  # LabRecord 2 is about molecule_a
        pattern_date = re.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2,3}Z")
        assert pattern_date.match(lab_records_patient[0]["timestamp"]["$date"])  # check the date is in datetime (CEST) form

        # check that all values are cast to the expected type
        assert lab_records_patient[0]["value"] == -0.003  # the value as been converted to an integer
        assert lab_records_patient[1]["value"] is False  # the value as been converted to a boolean
        assert lab_records_patient[3]["value"] == "black"
        assert lab_records_patient[4]["value"] == {"$date": "2021-12-22T11:58:38Z"}  # the value as been converted to a MongoDB-style datetime
        assert lab_records_patient[4]["anonymized_value"] == {"$date": "2021-12-01T00:00:00Z"}  # the value as been both fairified and anonymized
        cc_female = CodeableConcept(original_name="f")
        cc_female.add_coding(one_coding=Coding(code=OntologyResource(ontology=Ontologies.SNOMEDCT, full_code="248152002"), display=None))
        assert lab_records_patient[2]["value"] == cc_female.to_json()  # the value as been replaced by its ontology code (sex is a categorical value)r

        # we also check that conversions str->int/float and category->bool worked
        lab_recs = transform.laboratory_records
        lab_feats = transform.laboratory_features
        assert get_field_value_for_patient(lab_records=lab_recs, lab_features=lab_feats, patient_id=transform.patient_ids_mapping["999999999"], column_name="molecule_b") == 100  # this has been cast as int because it matches the expected unit
        assert get_field_value_for_patient(lab_records=lab_recs, lab_features=lab_feats, patient_id=transform.patient_ids_mapping["999999998"], column_name="molecule_b") == 111  # this has been cast as int because it matches the expected unit
        assert get_field_value_for_patient(lab_records=lab_recs, lab_features=lab_feats, patient_id=transform.patient_ids_mapping["999999997"], column_name="molecule_b") == "231 grams"  # this has not been converted as this does not match the expected dimension
        assert get_field_value_for_patient(lab_records=lab_recs, lab_features=lab_feats, patient_id=transform.patient_ids_mapping["999999996"], column_name="molecule_b") == 21  # this has been cast as int because it matches the expected unit
        assert get_field_value_for_patient(lab_records=lab_recs, lab_features=lab_feats, patient_id=transform.patient_ids_mapping["999999995"], column_name="molecule_b") == 100  # this has been cast as int even though there was no unit
        assert get_field_value_for_patient(lab_records=lab_recs, lab_features=lab_feats, patient_id=transform.patient_ids_mapping["999999994"], column_name="molecule_b") is None  # no value
        assert get_field_value_for_patient(lab_records=lab_recs, lab_features=lab_feats, patient_id=transform.patient_ids_mapping["999999993"], column_name="molecule_b") is None  # null value
        assert get_field_value_for_patient(lab_records=lab_recs, lab_features=lab_feats, patient_id=transform.patient_ids_mapping["999999992"], column_name="molecule_b") == "116 kg"  # this has not been converted as this does not match the expected dimension
    # TODO Nelly: check there are no duplicates for LabFeature instances

    def test_create_lab_records_with_samples(self):
        # TODO NELLY: code this test
        pass

    def test_create_patients_without_pid(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_LABORATORY_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_LABORATORY_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_LABORATORY_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_LABORATORY_COL_CAT_PATH,
                             extracted_column_dimension_path=TheTestFiles.EXTR_LABORATORY_DIMENSIONS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH,
                             extracted_diagnosis_classification_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_CLASSIFICATION_PATH,
                             extracted_mapping_diagnosis_to_cc_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_TO_CC_PATH)
        # this creates Patient resources (based on the data file) and insert them in a (JSON) temporary file
        log.debug(transform.data.to_string())
        log.debug(transform.metadata.to_string())
        log.debug(transform.patient_ids_mapping)
        transform.create_patients()

        log.debug(transform.patients)

        assert len(transform.patients) == 10
        # we cannot simply order by identifier value because they are strings, not int
        # thus will need a bit more of processing to sort by the integer represented within the string
        # sorted_patients = sorted(transform.patients, key=lambda d: d.to_json()["identifier"]["value"])
        sorted_patients = sorted(transform.patients, key=lambda p: p.get_identifier_as_int())
        log.debug(sorted_patients)
        for i in range(0, len(sorted_patients)):
            # patients have their own anonymized ids
            assert sorted_patients[i].to_json()["identifier"]["value"] == PatientAnonymizedIdentifier(id_value=str(i+1), hospital_name=HospitalNames.TEST_H1).value

        # get back to the original file
        with open(self.execution.anonymized_patient_ids_filepath, "w") as f:
            f.write("{}")

    def test_create_patients_with_pid(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_LABORATORY_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_LABORATORY_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_LABORATORY_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_LABORATORY_COL_CAT_PATH,
                             extracted_column_dimension_path=TheTestFiles.EXTR_LABORATORY_DIMENSIONS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_FILLED_PIDS_PATH,
                             extracted_diagnosis_classification_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_CLASSIFICATION_PATH,
                             extracted_mapping_diagnosis_to_cc_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_TO_CC_PATH)
        # this creates Patient resources (based on the data file) and insert them in a (JSON) temporary file
        log.debug(transform.data.to_string())
        log.debug(transform.metadata.to_string())
        log.debug(transform.patient_ids_mapping)
        transform.create_patients()

        log.debug(transform.patients)

        assert len(transform.patients) == 10
        # we cannot simply order by identifier value because they are strings, not int
        # thus will need a bit more of processing to sort by the integer represented within the string
        # sorted_patients = sorted(transform.patients, key=lambda d: d.to_json()["identifier"]["value"])
        sorted_patients = sorted(transform.patients, key=lambda p: p.get_identifier_as_int())
        log.debug(sorted_patients)
        log.debug(transform.patient_ids_mapping)
        for i in range(0, len(sorted_patients)):
            # patients have their own anonymized ids
            assert sorted_patients[i].to_json()["identifier"]["value"] == PatientAnonymizedIdentifier(id_value=str(990+i), hospital_name=HospitalNames.TEST_H1).value

    def test_create_codeable_concept_from_row(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_LABORATORY_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_LABORATORY_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_LABORATORY_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_LABORATORY_COL_CAT_PATH,
                             extracted_column_dimension_path=TheTestFiles.EXTR_LABORATORY_DIMENSIONS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH,
                             extracted_diagnosis_classification_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_CLASSIFICATION_PATH,
                             extracted_mapping_diagnosis_to_cc_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_TO_CC_PATH)
        # no associated ontology code
        cc = transform.create_codeable_concept_from_row(column_name="molecule_b")
        assert cc is not None
        assert cc.coding is not None
        assert len(cc.coding) == 0
        assert cc.text == "molecule_b"

        # one associated ontology code
        cc = transform.create_codeable_concept_from_row(column_name="molecule_a")
        assert cc is not None
        assert cc.coding is not None
        assert len(cc.coding) == 1
        assert cc.text == "molecule_a"
        assert cc.coding[0].to_json() == Coding(code=OntologyResource(ontology=Ontologies.LOINC, full_code="1234"), display=None).to_json()

        # two associated ontology codes
        cc = transform.create_codeable_concept_from_row(column_name="ethnicity")
        assert cc is not None
        assert cc.coding is not None
        assert len(cc.coding) == 2
        assert cc.text == "ethnicity"
        assert cc.coding[0].to_json() == Coding(code=OntologyResource(ontology=Ontologies.LOINC, full_code="46463-6"), display=None).to_json()
        assert cc.coding[1].to_json() == Coding(code=OntologyResource(ontology=Ontologies.SNOMEDCT, full_code="397731000"), display=None).to_json()

    def test_coding(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_LABORATORY_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_LABORATORY_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_LABORATORY_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_LABORATORY_COL_CAT_PATH,
                             extracted_column_dimension_path=TheTestFiles.EXTR_LABORATORY_DIMENSIONS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH,
                             extracted_diagnosis_classification_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_CLASSIFICATION_PATH,
                             extracted_mapping_diagnosis_to_cc_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_TO_CC_PATH)
        # no associated ontology code (patient id line)
        first_row = transform.metadata.iloc[0]
        log.info(first_row)
        coding = Coding(code=OntologyResource(ontology=Ontologies.get_enum_from_name(first_row[MetadataColumns.FIRST_ONTOLOGY_NAME]), full_code=first_row[MetadataColumns.FIRST_ONTOLOGY_CODE]),
                        display=None)
        assert coding.system is None

        # one associated ontology code (molecule_g line)
        third_row = transform.metadata.iloc[3]
        coding = Coding(code=OntologyResource(ontology=Ontologies.get_enum_from_name(third_row[MetadataColumns.FIRST_ONTOLOGY_NAME]), full_code=third_row[MetadataColumns.FIRST_ONTOLOGY_CODE]),
                        display=None)
        assert coding is not None
        assert coding.system == Ontologies.SNOMEDCT["url"]
        assert coding.code == "421416008"
        assert coding.display == "Gamma"

        # two associated ontology codes (ethnicity line)
        # but this method creates one coding at a time
        # so, we need to create them in two times
        fifth_row = transform.metadata.iloc[5]
        coding1 = Coding(code=OntologyResource(ontology=Ontologies.get_enum_from_name(fifth_row[MetadataColumns.FIRST_ONTOLOGY_NAME]), full_code=fifth_row[MetadataColumns.FIRST_ONTOLOGY_CODE]),
                         display=None)
        assert coding1 is not None
        assert coding1.system == Ontologies.LOINC["url"]
        assert coding1.code == "46463-6"
        assert coding1.display == "Race or ethnicity"
        coding2 = Coding(code=OntologyResource(ontology=Ontologies.get_enum_from_name(fifth_row[MetadataColumns.SEC_ONTOLOGY_NAME]), full_code=fifth_row[MetadataColumns.SEC_ONTOLOGY_CODE]),
                         display=None)
        assert coding2.system == Ontologies.SNOMEDCT["url"]
        assert coding2.code == "397731000"
        assert coding2.display == "Ethnic group finding"

    def test_determine_lab_feature_category(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_LABORATORY_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_LABORATORY_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_LABORATORY_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_LABORATORY_COL_CAT_PATH,
                             extracted_column_dimension_path=TheTestFiles.EXTR_LABORATORY_DIMENSIONS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH,
                             extracted_diagnosis_classification_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_CLASSIFICATION_PATH,
                             extracted_mapping_diagnosis_to_cc_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_TO_CC_PATH)

        # clinical variables
        cc = transform.get_lab_feature_category(column_name="molecule_a")
        assert cc is not None
        assert cc.to_json() == LabFeatureCategories.get_clinical().to_json()

        cc = transform.get_lab_feature_category(column_name="molecule_b")
        assert cc is not None
        assert cc.to_json() == LabFeatureCategories.get_clinical().to_json()

        cc = transform.get_lab_feature_category(column_name="molecule_g")
        assert cc is not None
        assert cc.to_json() == LabFeatureCategories.get_clinical().to_json()

        # phenotypic variables
        cc = transform.get_lab_feature_category(column_name="ethnicity")
        assert cc is not None
        assert cc.to_json() == LabFeatureCategories.get_phenotypic().to_json()

        cc = transform.get_lab_feature_category(column_name="sex")
        assert cc is not None
        assert cc.to_json() == LabFeatureCategories.get_phenotypic().to_json()

    def test_is_column_phenotypic(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_LABORATORY_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_LABORATORY_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_LABORATORY_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_LABORATORY_COL_CAT_PATH,
                             extracted_column_dimension_path=TheTestFiles.EXTR_LABORATORY_DIMENSIONS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH,
                             extracted_diagnosis_classification_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_CLASSIFICATION_PATH,
                             extracted_mapping_diagnosis_to_cc_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_TO_CC_PATH)

        # clinical variables
        assert transform.is_column_phenotypic(column_name="molecule_a") is False
        assert transform.is_column_phenotypic(column_name="molecule_b") is False
        assert transform.is_column_phenotypic(column_name="molecule_g") is False

        # phenotypic variables
        assert transform.is_column_phenotypic(column_name="ethnicity") is True
        assert transform.is_column_phenotypic(column_name="sex") is True

    def test_fairify_value(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_LABORATORY_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_LABORATORY_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_LABORATORY_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_LABORATORY_COL_CAT_PATH,
                             extracted_column_dimension_path=TheTestFiles.EXTR_LABORATORY_DIMENSIONS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH,
                             extracted_diagnosis_classification_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_CLASSIFICATION_PATH,
                             extracted_mapping_diagnosis_to_cc_path=TheTestFiles.EXTR_JSON_DIAGNOSIS_TO_CC_PATH)

        assert transform.fairify_value(column_name="id", value=transform.data.iloc[0][0]) == "999999999"
        assert transform.fairify_value(column_name="molecule_a", value=transform.data.iloc[0][1]) == 0.001
        assert transform.fairify_value(column_name="molecule_b", value=transform.data.iloc[0][2]) == 100
        assert transform.fairify_value(column_name="molecule_g", value=transform.data.iloc[0][3]) is True
        assert transform.fairify_value(column_name="molecule_g", value=transform.data.iloc[4][3]) is False
        # not variable == np.nan (https://stackoverflow.com/questions/44367557/why-does-assert-np-nan-np-nan-cause-an-error)
        assert np.isnan(transform.fairify_value(column_name="molecule_g", value=transform.data.iloc[6][3]))
        assert np.isnan(transform.fairify_value(column_name="molecule_g", value=transform.data.iloc[7][3]))
        cc_female = CodeableConcept(original_name="f")
        cc_female.add_coding(one_coding=Coding(code=OntologyResource(ontology=Ontologies.SNOMEDCT, full_code="248152002"), display=None))
        fairified_value = transform.fairify_value(column_name="sex", value=transform.data.iloc[0][4])
        assert fairified_value == cc_female.to_json()
        assert transform.fairify_value(column_name="ethnicity", value=transform.data.iloc[0][5]) == "white"
        assert not is_not_nan(transform.fairify_value(column_name="date_of_birth", value=transform.data.iloc[0][6]))  # no date here, we verify this is NaN
        assert transform.fairify_value(column_name="date_of_birth", value=transform.data.iloc[5][6]) == cast_str_to_datetime(str_value="2021-12-22 11:58:38.881")
