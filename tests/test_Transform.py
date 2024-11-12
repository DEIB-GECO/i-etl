import json
import os
import re
import unittest

import numpy as np
import pytest

from database.Database import Database
from database.Execution import Execution
from datatypes.OntologyResource import OntologyResource
from datatypes.Identifier import Identifier
from datatypes.PatientAnonymizedIdentifier import PatientAnonymizedIdentifier
from datatypes.ResourceIdentifier import ResourceIdentifier
from enums.DataTypes import DataTypes
from enums.HospitalNames import HospitalNames
from enums.MetadataColumns import MetadataColumns
from enums.Ontologies import Ontologies
from enums.ParameterKeys import ParameterKeys
from enums.TableNames import TableNames
from enums.TheTestFiles import TheTestFiles
from enums.Visibility import Visibility
from etl.Transform import Transform
from entities.Hospital import Hospital
from constants.structure import TEST_DB_NAME, DOCKER_FOLDER_TEST
from constants.defaults import DEFAULT_ONTOLOGY_RESOURCE_LABEL
from statistics.QualityStatistics import QualityStatistics
from statistics.TimeStatistics import TimeStatistics
from utils.assertion_utils import is_not_nan
from utils.cast_utils import cast_str_to_datetime
from utils.file_utils import read_tabular_file_as_string, get_json_resource_file
from utils.setup_logger import log
from utils.test_utils import set_env_variables_from_dict, compare_tuples, get_feature_by_text, \
    get_field_value_for_patient, get_records_for_patient


# personalized setup called at the beginning of each test
def my_setup(hospital_name: str, extracted_metadata_path: str, extracted_data_paths: str,
             extracted_mapping_categorical_values_path: str,
             extracted_column_to_categorical_path: str,
             extracted_column_dimension_path: str,
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
    with open(os.path.join(DOCKER_FOLDER_TEST, extracted_column_dimension_path), "r") as f:
        column_to_dimension = json.load(f)
    with open(os.path.join(DOCKER_FOLDER_TEST, extracted_patient_ids_mapping_path), "r") as f:
        patient_ids_mapping = json.load(f)
    transform = Transform(database=database, execution=TestTransform.execution, data=data, metadata=metadata,
                          mapping_categorical_value_to_onto_resource=mapping_categorical_values,
                          mapping_column_to_categorical_value=mapping_column_to_categorical_value,
                          mapping_column_to_dimension=column_to_dimension,
                          patient_ids_mapping=patient_ids_mapping,
                          quality_stats=QualityStatistics(record_stats=False), time_stats=TimeStatistics(record_stats=False))
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
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_PHENOTYPIC_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_PHENOTYPIC_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_PHENOTYPIC_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_PHENOTYPIC_COL_CAT_PATH,
                             extracted_column_dimension_path=TheTestFiles.EXTR_PHENOTYPIC_DIMENSIONS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH)
        transform.set_resource_counter_id()

        assert transform.counter.resource_id == 0

        # when some tables already contain resources, it should not be 0
        # I manually insert some resources in the database
        database = Database(TestTransform.execution)
        my_tuples = [
            {"identifier": ResourceIdentifier(id_value="1", resource_type=TableNames.PATIENT).to_json()},
            {"identifier": ResourceIdentifier(id_value="2", resource_type=TableNames.PATIENT).to_json()},
            {"identifier": ResourceIdentifier(id_value="7", resource_type=TableNames.PATIENT).to_json()},
            {"identifier": ResourceIdentifier(id_value="3", resource_type=TableNames.PATIENT).to_json()},
            {"identifier": ResourceIdentifier(id_value="124", resource_type=TableNames.PHENOTYPIC_FEATURE).to_json()},
            {"identifier": ResourceIdentifier(id_value="9", resource_type=TableNames.PATIENT).to_json()},
            {"identifier": Identifier(value="123LD456").to_json()},
            {"identifier": ResourceIdentifier(id_value="123", resource_type=TableNames.PATIENT).to_json()},
            {"identifier": ResourceIdentifier(id_value="6", resource_type=TableNames.PATIENT).to_json()}
        ]
        database.insert_many_tuples(table_name=TableNames.TEST, tuples=my_tuples)
        transform.set_resource_counter_id()

        assert transform.counter.resource_id == 125

    def test_create_hospital(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_PHENOTYPIC_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_PHENOTYPIC_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_PHENOTYPIC_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_PHENOTYPIC_COL_CAT_PATH,
                             extracted_column_dimension_path=TheTestFiles.EXTR_PHENOTYPIC_DIMENSIONS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH)
        # this creates a new Hospital resource and insert it in a (JSON) temporary file
        transform.create_hospital()

        # check that the in-memory hospital is correct
        assert len(transform.hospitals) == 1
        assert type(transform.hospitals[0]) is Hospital
        current_json_hospital = transform.hospitals[0].to_json()
        assert len(current_json_hospital.keys()) == 1 + 2  # name + inherited (identifier, timestamp)
        assert current_json_hospital["identifier"] == "Hospital:1"
        assert current_json_hospital["name"] == HospitalNames.TEST_H1
        assert current_json_hospital["timestamp"] is not None

        # b. check that the in-file hospital is correct
        hospital_file = get_json_resource_file(current_working_dir=TestTransform.execution.working_dir_current, table_name=TableNames.HOSPITAL, count=1)
        with open(hospital_file) as f:
            written_hospitals = json.load(f)
        assert len(written_hospitals) == 1
        compare_tuples(original_tuple=current_json_hospital, inserted_tuple=written_hospitals[0])

    def test_create_sam_features_H1_D1(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_CLINICAL_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_CLINICAL_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_CLINICAL_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_CLINICAL_COL_CAT_PATH,
                             extracted_column_dimension_path=TheTestFiles.EXTR_CLINICAL_DIMENSIONS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH)
        # this creates SamFeature instances (based on the metadata file) and insert them in a (JSON) temporary file
        transform.create_clinical_features()

        assert len(transform.features) == 6 - 2  # sid and id do not count as SamFeatures
        # assert the third and fourth SamFeature instances:
        # lab_feature_a has one associated code
        # lab_feature_b has no associated code
        # because they may not be in the same order as in the metadata file, we get them based on their text
        # (which contains at least the column name, and maybe a description)
        # SamFeature about molecule_a
        lab_feature_a = get_feature_by_text(transform.features, "molecule_a")
        assert len(lab_feature_a) == 7  # inherited fields (identifier, resource_type, timestamp), proper fields (original_name, ontology_resource, permitted_datatype, dimension, visibility)
        assert "identifier" in lab_feature_a
        assert lab_feature_a["name"] == "molecule_a"
        assert lab_feature_a["ontology_resource"] == {
            "system": Ontologies.LOINC["url"],
            "code": "1234",
            "label": DEFAULT_ONTOLOGY_RESOURCE_LABEL  # this resource does not exist for real in LOINC, thus display is empty
        }
        assert lab_feature_a["datatype"] == DataTypes.FLOAT
        assert lab_feature_a["dimension"] == "mg/L"
        assert lab_feature_a["visibility"] == Visibility.PUBLIC_WITHOUT_ANONYMIZATION
        # timestamp is not tested

        # LabFeature about molecule_b
        lab_feature_b = get_feature_by_text(transform.features, "molecule_b")
        assert len(lab_feature_b) == 6
        assert "identifier" in lab_feature_b
        assert lab_feature_b["name"] == "molecule_b"
        assert "ontology_resource" not in lab_feature_b
        assert lab_feature_b["datatype"] == DataTypes.INTEGER
        assert lab_feature_b["dimension"] == "g"  # unit is gram

        # check that there are no duplicates in SamFeature instances
        # for this, we get the set of their names (in the field "text")
        lab_features_names_list = []
        for lab_feature in transform.features:
            if lab_feature is not None:
                lab_features_names_list.append(lab_feature.name)
        lab_features_names_set = set(lab_features_names_list)
        assert len(lab_features_names_list) == len(lab_features_names_set)

    def test_create_phen_features_H1_D2(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_PHENOTYPIC_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_PHENOTYPIC_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_PHENOTYPIC_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_PHENOTYPIC_COL_CAT_PATH,
                             extracted_column_dimension_path=TheTestFiles.EXTR_PHENOTYPIC_DIMENSIONS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH)
        # this creates PhenFeature instances (based on the metadata file) and insert them in a (JSON) temporary file
        transform.create_phenotypic_features()

        assert len(transform.features) == 4 - 1  # id does not count as a PhenFeature
        # assert the fourth and first PhenFeature instances:
        # lab_feature_a has one associated code
        # lab_feature_b has no associated code
        # because they may not be in the same order as in the metadata file, we get them based on their text
        # (which contains at least the column name, and maybe a description)
        # PhenFeature about sex
        lab_feature_a = get_feature_by_text(transform.features, "sex")
        assert len(lab_feature_a) == 7  # inherited fields (identifier, resource_type, timestamp), proper fields (name, ontology_resource, permitted_datatype, dimension, visibility)
        assert "identifier" in lab_feature_a
        assert lab_feature_a["name"] == "sex"
        assert lab_feature_a["ontology_resource"] == {
            "system": Ontologies.SNOMEDCT["url"],
            "code": "123:789",
            "label": f"{DEFAULT_ONTOLOGY_RESOURCE_LABEL}:{DEFAULT_ONTOLOGY_RESOURCE_LABEL}"  # the two codes do not exist for eal in SNOMED, thus using the empty label
        }
        assert lab_feature_a["datatype"] == DataTypes.CATEGORY
        assert "dimension" not in lab_feature_a

        # PhenFeature about date_of_birth
        lab_feature_b = get_feature_by_text(transform.features, "date_of_birth")
        assert len(lab_feature_b) == 5  # no ontology resource and no dimension
        assert "identifier" in lab_feature_b
        assert lab_feature_b["name"] == "date_of_birth"
        assert "ontology_resource" not in lab_feature_b
        assert lab_feature_b["datatype"] == DataTypes.DATETIME
        assert "dimension" not in lab_feature_b

        # check that there are no duplicates in PhenFeature instances
        # for this, we get the set of their names (in the field "text")
        lab_features_names_list = []
        for lab_feature in transform.features:
            if lab_feature is not None:
                lab_features_names_list.append(lab_feature.name)
        lab_features_names_set = set(lab_features_names_list)
        assert len(lab_features_names_list) == len(lab_features_names_set)

    def test_create_phen_records(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_PHENOTYPIC_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_PHENOTYPIC_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_PHENOTYPIC_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_PHENOTYPIC_COL_CAT_PATH,
                             extracted_column_dimension_path=TheTestFiles.EXTR_PHENOTYPIC_DIMENSIONS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH)
        # this loads references (Hospital+PhenFeature resources), creates LabRecord resources (based on the data file) and insert them in a (JSON) temporary file
        transform.create_hospital()
        transform.create_phenotypic_features()
        transform.create_patients()
        # the three previous steps are required to create PhenRecord instances
        transform.create_phenotypic_records()

        assert len(transform.records) == 17  # in total, 17 PhenRecord instances are created, between 2 and 5 per Patient

        # assert that PhenRecord instances have been correctly created for a given data row
        # we take the seventh row
        patient_id = transform.patient_ids_mapping["999999994"]
        assert patient_id == "h1:10"  # patient anonymized IDs start at h1:4, because 3 phen. feat. + 1 hospital have been created beforehand.
        phen_records_patient = get_records_for_patient(records=transform.records, patient_id=patient_id)
        female = OntologyResource(ontology=Ontologies.SNOMEDCT, full_code="248152002", label=None, quality_stats=None)
        assert phen_records_patient[0]["value"] == female.to_json()  # the value as been replaced by its ontology code (sex is a categorical value)
        assert phen_records_patient[1]["value"] == "black"
        assert phen_records_patient[2]["value"] == {"$date": "2021-12-22T11:58:38Z"}  # the value as been converted to a MongoDB-style datetime
        pattern_date = re.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2,3}Z")
        assert pattern_date.match(phen_records_patient[0]["timestamp"]["$date"])  # check the date is in datetime (CEST) form

        # we also check that string are normalized (no caps, etc.)
        recs = transform.records
        feats = transform.features
        assert transform.patient_ids_mapping["999999996"] == "h1:8"  # 1 Hospital + 3 Phen. Rec
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

    def test_create_sam_records(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_CLINICAL_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_CLINICAL_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_CLINICAL_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_CLINICAL_COL_CAT_PATH,
                             extracted_column_dimension_path=TheTestFiles.EXTR_CLINICAL_DIMENSIONS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_FILLED_PIDS_PATH)
        # this loads references (Hospital+LabFeature resources), creates LabRecord resources (based on the data file) and insert them in a (JSON) temporary file
        transform.create_hospital()
        transform.create_clinical_features()
        transform.create_patients()
        # the three above steps are required to create SamRecord instances
        transform.create_clinical_records()

        assert len(transform.records) == 16  # in total, 16 SamRecord instances are created, between 2 and 5 per Patient

        # assert that LabRecord instances have been correctly created for a given data row
        # we take the seventh row
        patient_id = transform.patient_ids_mapping["999999994"]
        assert patient_id == "h1:994"
        records = get_records_for_patient(records=transform.records, patient_id=patient_id)
        assert len(records) == 2
        assert records[0]["value"] == -0.003  # the value as been converted to a float
        assert records[0]["has_subject"] == str(patient_id)  # this has not been converted to an integer
        assert records[0]["registered_by"] == "Hospital:1"
        assert records[0]["instantiates"] == "ClinicalFeature:2"  # LabRecord 2 is about molecule_a
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
        assert get_field_value_for_patient(records=recs, features=feats, patient_id=transform.patient_ids_mapping["999999997"], column_name="molecule_b") == "231 grams"  # this has not been converted as this does not match the expected dimension
        assert get_field_value_for_patient(records=recs, features=feats, patient_id=transform.patient_ids_mapping["999999996"], column_name="molecule_b") == 21  # this has been cast as int because it matches the expected unit
        assert get_field_value_for_patient(records=recs, features=feats, patient_id=transform.patient_ids_mapping["999999995"], column_name="molecule_b") == 100  # this has been cast as int even though there was no unit
        assert get_field_value_for_patient(records=recs, features=feats, patient_id=transform.patient_ids_mapping["999999994"], column_name="molecule_b") is None  # no value
        assert get_field_value_for_patient(records=recs, features=feats, patient_id=transform.patient_ids_mapping["999999993"], column_name="molecule_b") is None  # null value
        assert get_field_value_for_patient(records=recs, features=feats, patient_id=transform.patient_ids_mapping["999999992"], column_name="molecule_b") == "116 kg"  # this has not been converted as this does not match the expected dimension
    # TODO Nelly: check there are no duplicates for SamFeature instances

    def test_create_patients_without_pid(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_PHENOTYPIC_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_PHENOTYPIC_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_PHENOTYPIC_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_PHENOTYPIC_COL_CAT_PATH,
                             extracted_column_dimension_path=TheTestFiles.EXTR_PHENOTYPIC_DIMENSIONS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH)
        # this creates Patient resources (based on the data file) and insert them in a (JSON) temporary file
        transform.create_patients()

        assert len(transform.patients) == 10
        # we cannot simply order by identifier value because they are strings, not int
        # thus will need a bit more of processing to sort by the integer represented within the string
        # sorted_patients = sorted(transform.patients, key=lambda d: d.to_json()["identifier"]["value"])
        sorted_patients = sorted(transform.patients, key=lambda p: p.get_identifier_as_int())
        for i in range(0, len(sorted_patients)):
            # patients have their own anonymized ids
            assert sorted_patients[i].to_json()["identifier"] == PatientAnonymizedIdentifier(id_value=str(i+1), hospital_name=HospitalNames.TEST_H1).value

        # get back to the original file
        with open(self.execution.anonymized_patient_ids_filepath, "w") as f:
            f.write("{}")

    def test_create_patients_with_pid(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_PHENOTYPIC_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_PHENOTYPIC_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_PHENOTYPIC_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_PHENOTYPIC_COL_CAT_PATH,
                             extracted_column_dimension_path=TheTestFiles.EXTR_PHENOTYPIC_DIMENSIONS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_FILLED_PIDS_PATH)
        # this creates Patient resources (based on the data file) and insert them in a (JSON) temporary file
        transform.create_patients()

        assert len(transform.patients) == 10
        # we cannot simply order by identifier value because they are strings, not int
        # thus will need a bit more of processing to sort by the integer represented within the string
        # sorted_patients = sorted(transform.patients, key=lambda d: d.to_json()["identifier"]["value"])
        sorted_patients = sorted(transform.patients, key=lambda p: p.get_identifier_as_int())
        for i in range(0, len(sorted_patients)):
            # patients have their own anonymized ids
            assert sorted_patients[i].to_json()["identifier"] == PatientAnonymizedIdentifier(id_value=str(990+i), hospital_name=HospitalNames.TEST_H1).value

    def test_create_ontology_resource_from_row(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_CLINICAL_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_CLINICAL_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_CLINICAL_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_CLINICAL_COL_CAT_PATH,
                             extracted_column_dimension_path=TheTestFiles.EXTR_CLINICAL_DIMENSIONS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH)
        # no associated ontology code
        onto_resource = transform.create_ontology_resource_from_row(column_name="molecule_b")
        assert onto_resource is None

        # one associated ontology code
        onto_resource = transform.create_ontology_resource_from_row(column_name="molecule_a")
        assert onto_resource.to_json() == OntologyResource(ontology=Ontologies.LOINC, full_code="1234", label=None, quality_stats=None).to_json()

    def test_ontology_resource(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_CLINICAL_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_CLINICAL_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_CLINICAL_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_CLINICAL_COL_CAT_PATH,
                             extracted_column_dimension_path=TheTestFiles.EXTR_CLINICAL_DIMENSIONS_PATH,
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
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_PHENOTYPIC_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_PHENOTYPIC_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_PHENOTYPIC_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_PHENOTYPIC_COL_CAT_PATH,
                             extracted_column_dimension_path=TheTestFiles.EXTR_PHENOTYPIC_DIMENSIONS_PATH,
                             extracted_patient_ids_mapping_path=TheTestFiles.EXTR_EMPTY_PIDS_PATH)

        assert transform.fairify_value(column_name="id", value=transform.data.iloc[0][0]) == "999999999"
        onto_resource = OntologyResource(ontology=Ontologies.SNOMEDCT, full_code="248152002", label=None, quality_stats=None)
        fairified_value = transform.fairify_value(column_name="sex", value=transform.data.iloc[0][1])
        assert fairified_value == onto_resource.to_json()
        assert transform.fairify_value(column_name="ethnicity", value=transform.data.iloc[0][2]) == "white"
        assert not is_not_nan(transform.fairify_value(column_name="date_of_birth", value=transform.data.iloc[0][3]))  # no date here, we verify this is NaN
        assert transform.fairify_value(column_name="date_of_birth", value=transform.data.iloc[5][3]) == cast_str_to_datetime(str_value="2021-12-22 11:58:38.881")

    def test_fairify_clin_value(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.EXTR_METADATA_CLINICAL_PATH,
                             extracted_data_paths=TheTestFiles.EXTR_CLINICAL_DATA_PATH,
                             extracted_mapping_categorical_values_path=TheTestFiles.EXTR_CLINICAL_CATEGORICAL_PATH,
                             extracted_column_to_categorical_path=TheTestFiles.EXTR_CLINICAL_COL_CAT_PATH,
                             extracted_column_dimension_path=TheTestFiles.EXTR_CLINICAL_DIMENSIONS_PATH,
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
