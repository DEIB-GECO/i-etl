import json
import re
import unittest

import pandas as pd

from database.Database import Database
from database.Execution import Execution
from datatypes.CodeableConcept import CodeableConcept
from datatypes.Coding import Coding
from enums.DataTypes import DataTypes
from enums.PhenotypicColumns import PhenotypicColumns
from etl.Transform import Transform
from profiles.Hospital import Hospital
from enums.LabFeatureCategories import LabFeatureCategories
from enums.HospitalNames import HospitalNames
from enums.MetadataColumns import MetadataColumns
from enums.Ontologies import Ontologies
from enums.TableNames import TableNames
from enums.TheTestFiles import TheTestFiles
from utils.constants import DEFAULT_DB_CONNECTION, TEST_DB_NAME
from utils.setup_logger import log
from utils.utils import compare_tuples, get_json_resource_file, get_lab_feature_by_text, \
    normalize_ontology_system, normalize_ontology_code, is_not_nan, cast_value, read_csv_file_as_string, \
    get_field_value_for_patient, get_lab_records_for_patient


# personalized setup called at the beginning of each test
def my_setup(hospital_name: str, extracted_metadata_path: str, extracted_data_paths: str, extracted_mapped_values_path: str, extracted_column_dimension_path: str) -> Transform:
    args = {
        Execution.DB_CONNECTION_KEY: DEFAULT_DB_CONNECTION,
        Execution.DB_DROP_KEY: True,
        Execution.HOSPITAL_NAME_KEY: hospital_name
        # no need to set the metadata and data filepaths as we get already the loaded data and metadata as arguments
    }
    TestTransform.execution.set_up(args_as_dict=args, setup_data_files=False)  # no need to setup the files, we get data and metadata as input
    database = Database(TestTransform.execution)
    # I load:
    # - the data and metadata from two CSV files that I obtained by running the Extract step
    # - and mapped_values as a JSON file that I obtained from the same Extract object
    metadata = read_csv_file_as_string(extracted_metadata_path)
    data = read_csv_file_as_string(extracted_data_paths)
    mapped_values = json.load(open(extracted_mapped_values_path))
    column_to_dimension = json.load(open(extracted_column_dimension_path))
    transform = Transform(database=database, execution=TestTransform.execution, data=data, metadata=metadata, mapped_values=mapped_values, column_to_dimension=column_to_dimension)
    return transform


class TestTransform(unittest.TestCase):
    execution = Execution(TEST_DB_NAME)

    def test_run(self):
        pass

    def test_set_resource_counter_id(self):
        # when there is nothing in the database, the counter should be 0
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.TEST_EXTR_METADATA_LABORATORY_PATH,
                             extracted_data_paths=TheTestFiles.TEST_EXTR_LABORATORY_DATA_PATH,
                             extracted_mapped_values_path=TheTestFiles.TEST_EXTR_LABORATORY_MAPPED_PATH,
                             extracted_column_dimension_path=TheTestFiles.TEST_EXTR_LABORATORY_DIMENSIONS_PATH)
        transform.set_resource_counter_id()

        assert transform.counter.resource_id == 0

        # when some tables already contain resources, it should not be 0
        # I manually insert some resources in the database
        database = Database(TestTransform.execution)
        my_tuples = [
            {"identifier": {"value": "1"}},
            {"identifier": {"value": "2"}},
            {"identifier": {"value": "4"}},
            {"identifier": {"value": "3"}},
            {"identifier": {"value": "123a"}},
        ]
        database.insert_many_tuples(table_name=TableNames.TEST, tuples=my_tuples)
        transform.set_resource_counter_id()

        assert transform.counter.resource_id == 5

    def test_create_hospital(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.TEST_EXTR_METADATA_LABORATORY_PATH,
                             extracted_data_paths=TheTestFiles.TEST_EXTR_LABORATORY_DATA_PATH,
                             extracted_mapped_values_path=TheTestFiles.TEST_EXTR_LABORATORY_MAPPED_PATH,
                             extracted_column_dimension_path=TheTestFiles.TEST_EXTR_LABORATORY_DIMENSIONS_PATH)
        # this creates a new Hospital resource and insert it in a (JSON) temporary file
        transform.create_hospital(HospitalNames.TEST_H1)

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
        written_hospitals = json.load(open(hospital_file))
        assert len(written_hospitals) == 1
        compare_tuples(original_tuple=current_json_hospital, inserted_tuple=written_hospitals[0])

    def test_create_lab_features_H1_D1(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.TEST_EXTR_METADATA_LABORATORY_PATH,
                             extracted_data_paths=TheTestFiles.TEST_EXTR_LABORATORY_DATA_PATH,
                             extracted_mapped_values_path=TheTestFiles.TEST_EXTR_LABORATORY_MAPPED_PATH,
                             extracted_column_dimension_path=TheTestFiles.TEST_EXTR_LABORATORY_DIMENSIONS_PATH)
        # this creates LabFeature instances (based on the metadata file) and insert them in a (JSON) temporary file
        log.debug(transform.data.to_string())
        log.debug(transform.metadata.to_string())
        transform.create_laboratory_features()

        assert len(transform.laboratory_features) == 8 - 1  # id does not count as a LabFeature
        # assert the first, second and sixth LabFeature instances:
        # lab_feature_a has one associated code, and is clinical
        # lab_feature_b one has no associated code, and is clinical
        # lab_feature_ethnicity one has two associated codes, and is phenotypic
        # because they may not be in the same order as in the metadata file, we get them based on their text
        # (which contains at least the column name, and maybe a description)
        # LabFeature about molecule_a
        lab_feature_a = get_lab_feature_by_text(transform.laboratory_features, "molecule_a")
        assert len(lab_feature_a) == 7  # inherited fields (identifier, resource_type, timestamp), proper fields (code, permitted_datatype, dimension, category)
        assert "identifier" in lab_feature_a
        assert lab_feature_a["resource_type"] == TableNames.LABORATORY_FEATURE
        assert lab_feature_a["code"] == {
            "text": "molecule_a",
            "coding": [
                {
                    "system": Ontologies.LOINC["url"],
                    "code": "1234",
                    "display": "molecule_a (The molecule Alpha)"
                }
            ]
        }
        assert lab_feature_a["category"] == LabFeatureCategories.get_clinical().to_json()
        assert lab_feature_a["permitted_datatype"] == DataTypes.FLOAT
        assert lab_feature_a["dimension"] == "mg/L"

        # LabFeature about molecule_b
        lab_feature_b = get_lab_feature_by_text(transform.laboratory_features, "molecule_b")
        log.info(lab_feature_b)
        assert len(lab_feature_b) == 7
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
        # "loinc/45678" and "snomedct/345:678"
        lab_feature_ethnicity = get_lab_feature_by_text(transform.laboratory_features, "ethnicity")
        assert len(lab_feature_ethnicity) == 6  # only 6 (not 7) because dimension is None, thus not added
        assert "identifier" in lab_feature_ethnicity
        assert lab_feature_ethnicity["resource_type"] == TableNames.LABORATORY_FEATURE
        assert lab_feature_ethnicity["code"] == {
            "text": "ethnicity",
            "coding": [
                {
                    "system": Ontologies.LOINC["url"],
                    "code": "45678",
                    "display": "ethnicity (The ethnicity)"
                }, {
                    "system": Ontologies.SNOMEDCT["url"],
                    "code": "345:678",
                    "display": "ethnicity (The ethnicity)"
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
                             extracted_metadata_path=TheTestFiles.TEST_EXTR_METADATA_LABORATORY_PATH,
                             extracted_data_paths=TheTestFiles.TEST_EXTR_LABORATORY_DATA_PATH,
                             extracted_mapped_values_path=TheTestFiles.TEST_EXTR_LABORATORY_MAPPED_PATH,
                             extracted_column_dimension_path=TheTestFiles.TEST_EXTR_LABORATORY_DIMENSIONS_PATH)
        # this creates LabFeature resources (based on the metadata file) and insert them in a (JSON) temporary file
        log.debug(transform.data.to_string())
        log.debug(transform.metadata.to_string())
        transform.create_samples()

        # TODO NELLY

    def test_create_lab_records_without_samples(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.TEST_EXTR_METADATA_LABORATORY_PATH,
                             extracted_data_paths=TheTestFiles.TEST_EXTR_LABORATORY_DATA_PATH,
                             extracted_mapped_values_path=TheTestFiles.TEST_EXTR_LABORATORY_MAPPED_PATH,
                             extracted_column_dimension_path=TheTestFiles.TEST_EXTR_LABORATORY_DIMENSIONS_PATH)
        # this loads references (Hospital+LabFeature resources), creates LabRecord resources (based on the data file) and insert them in a (JSON) temporary file
        log.debug(transform.data.to_string())
        log.debug(transform.metadata.to_string())
        transform.create_hospital(HospitalNames.TEST_H1)
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
        patient_id = "999999994"
        lab_records_patient = get_lab_records_for_patient(lab_records=transform.laboratory_records, patient_id=patient_id)
        log.debug(json.dumps(lab_records_patient))
        assert len(lab_records_patient) == 5
        assert lab_records_patient[0]["resource_type"] == TableNames.LABORATORY_RECORD
        assert lab_records_patient[0]["value"] == -0.003  # the value as been converted to an integer
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
        cc_female = CodeableConcept()
        cc_female.add_coding(one_coding=Coding(system=normalize_ontology_system(ontology_system=Ontologies.SNOMEDCT["url"]),
                                               code=normalize_ontology_code(ontology_code="248152002"),
                                               name="f",
                                               description="Female"))
        assert lab_records_patient[2]["value"] == cc_female.to_json()  # the value as been replaced by its ontology code (sex is a categorical value)r

        # we also check that conversions str->int/float and category->bool worked
        assert get_field_value_for_patient(lab_records=transform.laboratory_records, lab_features=transform.laboratory_features, patient_id="999999999", column_name="molecule_b") == 100  # this has been cast as int because it matches the expected unit
        assert get_field_value_for_patient(lab_records=transform.laboratory_records, lab_features=transform.laboratory_features, patient_id="999999998", column_name="molecule_b") == 111  # this has been cast as int because it matches the expected unit
        assert get_field_value_for_patient(lab_records=transform.laboratory_records, lab_features=transform.laboratory_features, patient_id="999999997", column_name="molecule_b") == "231 grams"  # this has not been converted as this does not match the expected dimension
        assert get_field_value_for_patient(lab_records=transform.laboratory_records, lab_features=transform.laboratory_features, patient_id="999999996", column_name="molecule_b") == 21  # this has been cast as int because it matches the expected unit
        assert get_field_value_for_patient(lab_records=transform.laboratory_records, lab_features=transform.laboratory_features, patient_id="999999995", column_name="molecule_b") == 100  # this has been cast as int even though there was no unit
        assert get_field_value_for_patient(lab_records=transform.laboratory_records, lab_features=transform.laboratory_features, patient_id="999999994", column_name="molecule_b") is None  # no value
        assert get_field_value_for_patient(lab_records=transform.laboratory_records, lab_features=transform.laboratory_features, patient_id="999999993", column_name="molecule_b") is None  # null value
        assert get_field_value_for_patient(lab_records=transform.laboratory_records, lab_features=transform.laboratory_features, patient_id="999999992", column_name="molecule_b") == "116 kg"  # this has not been converted as this does not match the expected dimension

    # TODO Nelly: check there are no duplicates for LabFeature instances
    def test_create_lab_records_with_samples(self):
        pass
        # TODO NELLY: continue

    def test_create_patients(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.TEST_EXTR_METADATA_LABORATORY_PATH,
                             extracted_data_paths=TheTestFiles.TEST_EXTR_LABORATORY_DATA_PATH,
                             extracted_mapped_values_path=TheTestFiles.TEST_EXTR_LABORATORY_MAPPED_PATH,
                             extracted_column_dimension_path=TheTestFiles.TEST_EXTR_LABORATORY_DIMENSIONS_PATH)
        # this creates Patient resources (based on the data file) and insert them in a (JSON) temporary file
        log.debug(transform.data.to_string())
        log.debug(transform.metadata.to_string())
        transform.create_patients()

        log.debug(transform.patients)

        assert len(transform.patients) == 10
        sorted_patients = sorted(transform.patients, key=lambda d: d.to_json()["identifier"]["value"], reverse=True)
        log.debug(sorted_patients)
        for i in range(0, len(sorted_patients)):
            assert sorted_patients[i].to_json()["identifier"]["value"] == str(999999999-i)

    def test_create_codeable_concept_from_row(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.TEST_EXTR_METADATA_LABORATORY_PATH,
                             extracted_data_paths=TheTestFiles.TEST_EXTR_LABORATORY_DATA_PATH,
                             extracted_mapped_values_path=TheTestFiles.TEST_EXTR_LABORATORY_MAPPED_PATH,
                             extracted_column_dimension_path=TheTestFiles.TEST_EXTR_LABORATORY_DIMENSIONS_PATH)
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
        assert cc.coding[0].to_json() == Coding(system=Ontologies.LOINC["url"], code="1234", name="molecule_a", description="The molecule Alpha").to_json()

        # two associated ontology codes
        cc = transform.create_codeable_concept_from_row(column_name="ethnicity")
        assert cc is not None
        assert cc.coding is not None
        assert len(cc.coding) == 2
        assert cc.text == "ethnicity"
        assert cc.coding[0].to_json() == Coding(system=Ontologies.LOINC["url"], code="45678", name="ethnicity", description="The ethnicity").to_json()
        assert cc.coding[1].to_json() == Coding(system=Ontologies.SNOMEDCT["url"], code="345:678", name="ethnicity", description="The ethnicity").to_json()

    def test_coding(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.TEST_EXTR_METADATA_LABORATORY_PATH,
                             extracted_data_paths=TheTestFiles.TEST_EXTR_LABORATORY_DATA_PATH,
                             extracted_mapped_values_path=TheTestFiles.TEST_EXTR_LABORATORY_MAPPED_PATH,
                             extracted_column_dimension_path=TheTestFiles.TEST_EXTR_LABORATORY_DIMENSIONS_PATH)
        # no associated ontology code (patient id line)
        first_row = transform.metadata.iloc[0]
        coding = Coding(system=Ontologies.get_ontology_system(first_row[MetadataColumns.FIRST_ONTOLOGY_SYSTEM]),
                        code=first_row[MetadataColumns.FIRST_ONTOLOGY_CODE],
                        name=first_row[MetadataColumns.COLUMN_NAME],
                        description=first_row[MetadataColumns.SIGNIFICATION_EN])
        log.debug(coding)
        log.debug(type(coding))
        assert coding.system is None

        # one associated ontology code (molecule_g line)
        third_row = transform.metadata.iloc[3]
        coding = Coding(system=Ontologies.get_ontology_system(third_row[MetadataColumns.FIRST_ONTOLOGY_SYSTEM]),
                        code=third_row[MetadataColumns.FIRST_ONTOLOGY_CODE],
                        name=third_row[MetadataColumns.COLUMN_NAME],
                        description=third_row[MetadataColumns.SIGNIFICATION_EN])
        assert coding is not None
        assert coding.system == Ontologies.SNOMEDCT["url"]
        assert coding.code == "123:678"
        assert coding.display == "molecule_g (The molecule Gamma)"

        # two associated ontology codes (ethnicity line)
        # but this method creates one coding at a time
        # so, we need to create them in two times
        fifth_row = transform.metadata.iloc[5]
        coding1 = Coding(system=Ontologies.get_ontology_system(fifth_row[MetadataColumns.FIRST_ONTOLOGY_SYSTEM]),
                         code=fifth_row[MetadataColumns.FIRST_ONTOLOGY_CODE],
                         name=fifth_row[MetadataColumns.COLUMN_NAME],
                         description=fifth_row[MetadataColumns.SIGNIFICATION_EN])
        assert coding1 is not None
        assert coding1.system == Ontologies.LOINC["url"]
        assert coding1.code == "45678"
        assert coding1.display == "ethnicity (The ethnicity)"
        coding2 = Coding(system=Ontologies.get_ontology_system(fifth_row[MetadataColumns.SEC_ONTOLOGY_SYSTEM]),
                         code=fifth_row[MetadataColumns.SEC_ONTOLOGY_CODE],
                         name=fifth_row[MetadataColumns.COLUMN_NAME],
                         description=fifth_row[MetadataColumns.SIGNIFICATION_EN])
        assert coding2.system == Ontologies.SNOMEDCT["url"]
        assert coding2.code == "345:678"
        assert coding2.display == "ethnicity (The ethnicity)"

    def test_determine_lab_feature_category(self):
        _ = my_setup(hospital_name=HospitalNames.TEST_H1,
                     extracted_metadata_path=TheTestFiles.TEST_EXTR_METADATA_LABORATORY_PATH,
                     extracted_data_paths=TheTestFiles.TEST_EXTR_LABORATORY_DATA_PATH,
                     extracted_mapped_values_path=TheTestFiles.TEST_EXTR_LABORATORY_MAPPED_PATH,
                     extracted_column_dimension_path=TheTestFiles.TEST_EXTR_LABORATORY_DIMENSIONS_PATH)

        # clinical variables
        cc = Transform.get_lab_feature_category(column_name="molecule_a")
        assert cc is not None
        assert cc.to_json() == LabFeatureCategories.get_clinical().to_json()

        cc = Transform.get_lab_feature_category(column_name="molecule_b")
        assert cc is not None
        assert cc.to_json() == LabFeatureCategories.get_clinical().to_json()

        cc = Transform.get_lab_feature_category(column_name="molecule_g")
        assert cc is not None
        assert cc.to_json() == LabFeatureCategories.get_clinical().to_json()

        # phenotypic variables
        cc = Transform.get_lab_feature_category(column_name="ethnicity")
        assert cc is not None
        assert cc.to_json() == LabFeatureCategories.get_phenotypic().to_json()

        cc = Transform.get_lab_feature_category(column_name="sex")
        assert cc is not None
        assert cc.to_json() == LabFeatureCategories.get_phenotypic().to_json()

    def test_is_column_phenotypic(self):
        _ = my_setup(hospital_name=HospitalNames.TEST_H1,
                     extracted_metadata_path=TheTestFiles.TEST_EXTR_METADATA_LABORATORY_PATH,
                     extracted_data_paths=TheTestFiles.TEST_EXTR_LABORATORY_DATA_PATH,
                     extracted_mapped_values_path=TheTestFiles.TEST_EXTR_LABORATORY_MAPPED_PATH,
                     extracted_column_dimension_path=TheTestFiles.TEST_EXTR_LABORATORY_DIMENSIONS_PATH)

        # clinical variables
        assert PhenotypicColumns.is_column_phenotypic(column_name="molecule_a") is False
        assert PhenotypicColumns.is_column_phenotypic(column_name="molecule_b") is False
        assert PhenotypicColumns.is_column_phenotypic(column_name="molecule_g") is False

        # phenotypic variables
        assert PhenotypicColumns.is_column_phenotypic(column_name="ethnicity") is True
        assert PhenotypicColumns.is_column_phenotypic(column_name="sex") is True

    def test_fairify_value(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1,
                             extracted_metadata_path=TheTestFiles.TEST_EXTR_METADATA_LABORATORY_PATH,
                             extracted_data_paths=TheTestFiles.TEST_EXTR_LABORATORY_DATA_PATH,
                             extracted_mapped_values_path=TheTestFiles.TEST_EXTR_LABORATORY_MAPPED_PATH,
                             extracted_column_dimension_path=TheTestFiles.TEST_EXTR_LABORATORY_DIMENSIONS_PATH)

        assert transform.fairify_value(column_name="id", value=transform.data.iloc[0][0]) == 999999999  # TODO NELLY: do not convert IDs to int, keep it as strings!
        assert transform.fairify_value(column_name="molecule_a", value=transform.data.iloc[0][1]) == 0.001
        assert transform.fairify_value(column_name="molecule_b", value=transform.data.iloc[0][2]) == 100
        assert transform.fairify_value(column_name="molecule_g", value=transform.data.iloc[0][3]) is True
        cc_female = CodeableConcept()
        cc_female.add_coding(one_coding=Coding(system=Ontologies.SNOMEDCT["url"], code="248152002", name="f", description="Female"))
        assert transform.fairify_value(column_name="sex", value=transform.data.iloc[0][4]).to_json() == cc_female.to_json()
        assert transform.fairify_value(column_name="ethnicity", value=transform.data.iloc[0][5]) == "white"
        assert not is_not_nan(transform.fairify_value(column_name="date_of_birth", value=transform.data.iloc[0][6]))
        assert transform.fairify_value(column_name="date_of_birth", value=transform.data.iloc[5][6]) == cast_value("2021-12-22 11:58:38.881")
        