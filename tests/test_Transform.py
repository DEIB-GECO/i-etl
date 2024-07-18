import json
import re
import unittest

import pandas as pd

from database.Database import Database
from database.Execution import Execution
from datatypes.CodeableConcept import CodeableConcept
from datatypes.Coding import Coding
from etl.Transform import Transform
from profiles.Examination import Examination
from profiles.Hospital import Hospital
from enums.ExaminationCategory import ExaminationCategory
from enums.HospitalNames import HospitalNames
from enums.MetadataColumns import MetadataColumns
from enums.Ontologies import Ontologies
from enums.TableNames import TableNames
from enums.TheTestFiles import TheTestFiles
from utils.constants import DEFAULT_DB_CONNECTION, TEST_DB_NAME
from utils.setup_logger import log
from utils.utils import compare_tuples, get_json_resource_file, get_examination_by_text_in_list, \
    get_examination_records_by_patient_id_in_list, normalize_ontology_system, normalize_ontology_code, is_not_nan, \
    cast_value, read_csv_file_as_string


# personalized setup called at the beginning of each test
def my_setup(hospital_name: str, extracted_metadata_path: str, extracted_data_paths: str, extracted_mapped_values_path: str) -> Transform:
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
    transform = Transform(database=database, execution=TestTransform.execution, data=data, metadata=metadata, mapped_values=mapped_values)
    return transform


class TestTransform(unittest.TestCase):
    execution = Execution(TEST_DB_NAME)

    def test_run(self):
        pass

    def test_set_resource_counter_id(self):
        # when there is nothing in the database, the counter should be 0
        transform = my_setup(hospital_name=HospitalNames.TEST_H1.value,
                             extracted_metadata_path=TheTestFiles.TEST_EXTR_METADATA_CLINICAL_PATH.value,
                             extracted_data_paths=TheTestFiles.TEST_EXTR_CLINICAL_DATA_PATH.value,
                             extracted_mapped_values_path=TheTestFiles.TEST_EXTR_CLINICAL_MAPPED_PATH.value)
        transform.set_resource_counter_id()

        assert transform.counter.resource_id == 0

        # when some tables already contain resources, it should not be 0
        # I manually insert some resources in the database
        database = Database(TestTransform.execution)
        my_tuples = [
            {"identifier": {"value": 1}},
            {"identifier": {"value": 2}},
            {"identifier": {"value": 4}},
            {"identifier": {"value": 3}},
            {"identifier": {"value": 123}},
        ]
        database.insert_many_tuples(table_name=TableNames.TEST.value, tuples=my_tuples)
        transform.set_resource_counter_id()

        assert transform.counter.resource_id == 124

    def test_create_hospital(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1.value,
                             extracted_metadata_path=TheTestFiles.TEST_EXTR_METADATA_CLINICAL_PATH.value,
                             extracted_data_paths=TheTestFiles.TEST_EXTR_CLINICAL_DATA_PATH.value,
                             extracted_mapped_values_path=TheTestFiles.TEST_EXTR_CLINICAL_MAPPED_PATH.value)
        # this creates a new Hospital resource and insert it in a (JSON) temporary file
        transform.create_hospital(HospitalNames.TEST_H1.value)

        # check that the in-memory hospital is correct
        assert len(transform.hospitals) == 1
        assert type(transform.hospitals[0]) is Hospital
        current_json_hospital = transform.hospitals[0].to_json()
        assert len(current_json_hospital.keys()) == 1 + 3  # name + inherited (identifier, resourceType, createdAt)
        assert current_json_hospital["identifier"]["value"] == "1"
        assert current_json_hospital["name"] == HospitalNames.TEST_H1.value
        assert current_json_hospital["createdAt"] is not None
        assert current_json_hospital["resourceType"] == Hospital.get_type()

        # b. check that the in-file hospital is correct
        hospital_file = get_json_resource_file(current_working_dir=TestTransform.execution.working_dir_current, table_name=TableNames.HOSPITAL.value, count=1)
        written_hospitals = json.load(open(hospital_file))
        assert len(written_hospitals) == 1
        compare_tuples(original_tuple=current_json_hospital, inserted_tuple=written_hospitals[0])

    def test_create_examinations_H1_D1(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1.value,
                             extracted_metadata_path=TheTestFiles.TEST_EXTR_METADATA_CLINICAL_PATH.value,
                             extracted_data_paths=TheTestFiles.TEST_EXTR_CLINICAL_DATA_PATH.value,
                             extracted_mapped_values_path=TheTestFiles.TEST_EXTR_CLINICAL_MAPPED_PATH.value)
        # this creates Examination resources (based on the metadata file) and insert them in a (JSON) temporary file
        log.debug(transform.data.to_string())
        log.debug(transform.metadata.to_string())
        transform.create_examinations()

        assert len(transform.examinations) == 8-1  # id does not count as an examination
        # assert the first, second and sixth examinations:
        # examination_a has one associated code, and is clinical
        # examination_b one has no associated code, and is clinical
        # examination_ethnicity one has two associated codes, and is phenotypic
        # because they may not be in the same order as in the metadata file, we get them based on their text
        # (which contains at least the column name, and maybe a description)
        # examination about molecule_a
        examination_a = get_examination_by_text_in_list(transform.examinations, "molecule_a")
        assert len(examination_a) == 3 + 3  # 3 inherited + 3 proper fields
        assert "identifier" in examination_a
        assert examination_a["resourceType"] == Examination.get_type()
        assert examination_a["code"] == {
            "text": "molecule_a",
            "coding": [
                {
                    "system": Ontologies.LOINC.value["url"],
                    "code": "1234",
                    "display": "molecule_a (The molecule Alpha)"
                }
            ]
        }
        assert examination_a["category"] == ExaminationCategory.get_clinical().to_json()
        assert examination_a["permittedDatatype"] == []

        # examination about molecule_b
        examination_b = get_examination_by_text_in_list(transform.examinations, "molecule_b")
        assert len(examination_b) == 3 + 3  # 3 inherited + 3 proper fields
        assert "identifier" in examination_b
        assert examination_b["resourceType"] == Examination.get_type()
        assert examination_b["code"] == {
            "text": "molecule_b",
            "coding": []
        }
        assert examination_b["category"] == ExaminationCategory.get_clinical().to_json()
        assert examination_b["permittedDatatype"] == []

        # examination about ethnicity
        # "loinc/45678" and "snomedct/345:678"
        examination_ethnicity = get_examination_by_text_in_list(transform.examinations, "ethnicity")
        assert len(examination_ethnicity) == 3 + 3  # 3 inherited + 3 proper fields
        assert "identifier" in examination_ethnicity
        assert examination_ethnicity["resourceType"] == Examination.get_type()
        assert examination_ethnicity["code"] == {
            "text": "ethnicity",
            "coding": [
                {
                    "system": Ontologies.LOINC.value["url"],
                    "code": "45678",
                    "display": "ethnicity (The ethnicity)"
                }, {
                    "system": Ontologies.SNOMEDCT.value["url"],
                    "code": "345:678",
                    "display": "ethnicity (The ethnicity)"
                }
            ]
        }
        assert examination_ethnicity["category"] == ExaminationCategory.get_phenotypic().to_json()
        assert examination_ethnicity["permittedDatatype"] == []  # TODO NElly: add datatypes

        # check that there are no duplicates in Examination instances
        # for this, we get the set of their names (in the field "text")
        examination_names_list = [examination.code.text for examination in transform.examinations]
        examination_names_set = set(examination_names_list)
        assert len(examination_names_list) == len(examination_names_set)

    def test_create_samples(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1.value,
                             extracted_metadata_path=TheTestFiles.TEST_EXTR_METADATA_CLINICAL_PATH.value,
                             extracted_data_paths=TheTestFiles.TEST_EXTR_CLINICAL_DATA_PATH.value,
                             extracted_mapped_values_path=TheTestFiles.TEST_EXTR_CLINICAL_MAPPED_PATH.value)
        # this creates Examination resources (based on the metadata file) and insert them in a (JSON) temporary file
        log.debug(transform.data.to_string())
        log.debug(transform.metadata.to_string())
        transform.create_samples()

        # TODO NELLY

    def test_create_examination_records_without_samples(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1.value,
                             extracted_metadata_path=TheTestFiles.TEST_EXTR_METADATA_CLINICAL_PATH.value,
                             extracted_data_paths=TheTestFiles.TEST_EXTR_CLINICAL_DATA_PATH.value,
                             extracted_mapped_values_path=TheTestFiles.TEST_EXTR_CLINICAL_MAPPED_PATH.value)
        # this loads references (hospital+examination resources), creates ExaminationRecord resources (based on the data file) and insert them in a (JSON) temporary file
        log.debug(transform.data.to_string())
        log.debug(transform.metadata.to_string())
        transform.create_hospital(HospitalNames.TEST_H1.value)
        transform.create_examinations()
        transform.create_patients()  # this step and the two above are required to create ExaminationRecord instances
        transform.create_examination_records()

        log.debug(transform.mapping_hospital_to_hospital_id)
        log.debug(transform.mapping_column_to_examination_id)
        log.debug(transform.hospitals)
        log.debug(transform.examinations)

        assert len(transform.mapping_hospital_to_hospital_id) == 1
        assert HospitalNames.TEST_H1.value in transform.mapping_hospital_to_hospital_id
        log.debug(transform.examination_records)
        assert len(transform.examination_records) == 33  # in total, 33 ExaminationRecord instances are created, between 2 and 5 per Patient

        # assert that ExaminationRecord instances have been correctly created for a given data row
        # we take the seventh row
        log.debug(transform.examination_records)
        patient_id = "999999994"
        examination_records_patient = get_examination_records_by_patient_id_in_list(examination_records_list=transform.examination_records, patient_id=patient_id)
        log.debug(json.dumps(examination_records_patient))
        assert len(examination_records_patient) == 5
        assert examination_records_patient[0]["resourceType"] == TableNames.EXAMINATION_RECORD.value
        assert examination_records_patient[0]["value"] == -0.003  # the value as been converted to an integer
        log.debug(examination_records_patient[0]["subject"]["reference"])
        log.debug(type(examination_records_patient[0]["subject"]["reference"]))
        log.debug(str(patient_id))
        log.debug(type(str(patient_id)))
        assert examination_records_patient[0]["subject"]["reference"] == str(patient_id)  # this has not been converted to an integer
        assert examination_records_patient[0]["recordedBy"]["reference"] == "1"
        assert examination_records_patient[0]["instantiate"]["reference"] == "2"  # Examination 2 about molecule_a
        pattern_date = re.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2,3}Z")
        assert pattern_date.match(examination_records_patient[0]["createdAt"]["$date"])  # check the date is in datetime (CEST) form

        # check that all values are cast to the expected type
        assert examination_records_patient[0]["value"] == -0.003  # the value as been converted to an integer
        assert examination_records_patient[1]["value"] is False  # the value as been converted to a boolean
        assert examination_records_patient[3]["value"] == "black"
        assert examination_records_patient[4]["value"] == {"$date": "2021-12-22T11:58:38Z"}  # the value as been converted to a MongoDB-style datetime
        cc_female = CodeableConcept()
        cc_female.add_coding(coding=Coding(system=normalize_ontology_system(ontology_system=Ontologies.SNOMEDCT.value["url"]),
                                           code=normalize_ontology_code(ontology_code="248152002"),
                                           display="f (Female)"))
        assert examination_records_patient[2]["value"] == cc_female.to_json()  # the value as been replaced by its ontology code (sex is a categorical value)r

        # check the selected ExaminationRecord instances more generally:
        # for examination_record in examination_records_patient: TODO NELLY

    # TODO Nelly: check there are no duplicates for Examination instances
    def test_create_examination_records_with_samples(self):
        pass
        # TODO NELLY: continue

    def test_create_patients(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1.value,
                             extracted_metadata_path=TheTestFiles.TEST_EXTR_METADATA_CLINICAL_PATH.value,
                             extracted_data_paths=TheTestFiles.TEST_EXTR_CLINICAL_DATA_PATH.value,
                             extracted_mapped_values_path=TheTestFiles.TEST_EXTR_CLINICAL_MAPPED_PATH.value)
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

    def test_create_codeable_concept_from_column(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1.value,
                             extracted_metadata_path=TheTestFiles.TEST_EXTR_METADATA_CLINICAL_PATH.value,
                             extracted_data_paths=TheTestFiles.TEST_EXTR_CLINICAL_DATA_PATH.value,
                             extracted_mapped_values_path=TheTestFiles.TEST_EXTR_CLINICAL_MAPPED_PATH.value)
        # no associated ontology code
        cc = transform.create_codeable_concept_from_column("molecule_b")
        assert cc is not None
        assert cc.codings is not None
        assert len(cc.codings) == 0
        assert cc.text == "molecule_b"

        # one associated ontology code
        cc = transform.create_codeable_concept_from_column("molecule_a")
        assert cc is not None
        assert cc.codings is not None
        assert len(cc.codings) == 1
        assert cc.text == "molecule_a"
        assert cc.codings[0].to_json() == Coding(system=Ontologies.LOINC.value["url"], code="1234", display="molecule_a (The molecule Alpha)").to_json()

        # two associated ontology codes
        cc = transform.create_codeable_concept_from_column("ethnicity")
        assert cc is not None
        assert cc.codings is not None
        assert len(cc.codings) == 2
        assert cc.text == "ethnicity"
        assert cc.codings[0].to_json() == Coding(system=Ontologies.LOINC.value["url"], code="45678", display="ethnicity (The ethnicity)").to_json()
        assert cc.codings[1].to_json() == Coding(system=Ontologies.SNOMEDCT.value["url"], code="345:678", display="ethnicity (The ethnicity)").to_json()

    def test_create_coding_from_metadata(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1.value,
                             extracted_metadata_path=TheTestFiles.TEST_EXTR_METADATA_CLINICAL_PATH.value,
                             extracted_data_paths=TheTestFiles.TEST_EXTR_CLINICAL_DATA_PATH.value,
                             extracted_mapped_values_path=TheTestFiles.TEST_EXTR_CLINICAL_MAPPED_PATH.value)
        # no associated ontology code (patient id line)
        coding = transform.create_coding_from_metadata(row=transform.metadata.iloc[0], ontology_column=MetadataColumns.FIRST_ONTOLOGY_SYSTEM.value, code_column=MetadataColumns.FIRST_ONTOLOGY_CODE.value)
        assert coding is None

        # one associated ontology code (molecule_g line)
        coding = transform.create_coding_from_metadata(row=transform.metadata.iloc[3], ontology_column=MetadataColumns.FIRST_ONTOLOGY_SYSTEM.value, code_column=MetadataColumns.FIRST_ONTOLOGY_CODE.value)
        assert coding is not None
        assert coding.system == Ontologies.SNOMEDCT.value["url"]
        assert coding.code == "123:678"
        assert coding._display == "molecule_g (The molecule Gamma)"

        # two associated ontology codes (ethnicity line)
        # but this method creates one coding at a time
        # so, we need to create them in two times
        coding1 = transform.create_coding_from_metadata(row=transform.metadata.iloc[5], ontology_column=MetadataColumns.FIRST_ONTOLOGY_SYSTEM.value, code_column=MetadataColumns.FIRST_ONTOLOGY_CODE.value)
        assert coding1 is not None
        assert coding1.system == Ontologies.LOINC.value["url"]
        assert coding1.code == "45678"
        assert coding1.display == "ethnicity (The ethnicity)"
        coding2 = transform.create_coding_from_metadata(row=transform.metadata.iloc[5], ontology_column=MetadataColumns.SEC_ONTOLOGY_SYSTEM.value, code_column=MetadataColumns.SEC_ONTOLOGY_CODE.value)
        assert coding2.system == Ontologies.SNOMEDCT.value["url"]
        assert coding2.code == "345:678"
        assert coding2.display == "ethnicity (The ethnicity)"

    def test_determine_examination_category(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1.value,
                             extracted_metadata_path=TheTestFiles.TEST_EXTR_METADATA_CLINICAL_PATH.value,
                             extracted_data_paths=TheTestFiles.TEST_EXTR_CLINICAL_DATA_PATH.value,
                             extracted_mapped_values_path=TheTestFiles.TEST_EXTR_CLINICAL_MAPPED_PATH.value)

        # clinical variables
        cc = transform.determine_examination_category(column_name="molecule_a")
        assert cc is not None
        assert cc.to_json() == ExaminationCategory.get_clinical().to_json()

        cc = transform.determine_examination_category(column_name="molecule_b")
        assert cc is not None
        assert cc.to_json() == ExaminationCategory.get_clinical().to_json()

        cc = transform.determine_examination_category(column_name="molecule_g")
        assert cc is not None
        assert cc.to_json() == ExaminationCategory.get_clinical().to_json()

        # phenotypic variables
        cc = transform.determine_examination_category(column_name="ethnicity")
        assert cc is not None
        assert cc.to_json() == ExaminationCategory.get_phenotypic().to_json()

        cc = transform.determine_examination_category(column_name="sex")
        assert cc is not None
        assert cc.to_json() == ExaminationCategory.get_phenotypic().to_json()

    def test_is_column_phenotypic(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1.value,
                             extracted_metadata_path=TheTestFiles.TEST_EXTR_METADATA_CLINICAL_PATH.value,
                             extracted_data_paths=TheTestFiles.TEST_EXTR_CLINICAL_DATA_PATH.value,
                             extracted_mapped_values_path=TheTestFiles.TEST_EXTR_CLINICAL_MAPPED_PATH.value)

        # clinical variables
        assert transform.is_column_name_phenotypic(column_name="molecule_a") is False
        assert transform.is_column_name_phenotypic(column_name="molecule_b") is False
        assert transform.is_column_name_phenotypic(column_name="molecule_g") is False

        # phenotypic variables
        assert transform.is_column_name_phenotypic(column_name="ethnicity") is True
        assert transform.is_column_name_phenotypic(column_name="sex") is True

    def test_fairify_value(self):
        transform = my_setup(hospital_name=HospitalNames.TEST_H1.value,
                             extracted_metadata_path=TheTestFiles.TEST_EXTR_METADATA_CLINICAL_PATH.value,
                             extracted_data_paths=TheTestFiles.TEST_EXTR_CLINICAL_DATA_PATH.value,
                             extracted_mapped_values_path=TheTestFiles.TEST_EXTR_CLINICAL_MAPPED_PATH.value)

        assert transform.fairify_value(column_name="id", value=transform.data.iloc[0][0]) == 999999999  # TODO NELLY: do not convert IDs to int, keep it as strings!
        assert transform.fairify_value(column_name="molecule_a", value=transform.data.iloc[0][1]) == 0.001
        assert transform.fairify_value(column_name="molecule_b", value=transform.data.iloc[0][2]) == 100
        assert transform.fairify_value(column_name="molecule_g", value=transform.data.iloc[0][3]) is True
        cc_female = CodeableConcept()
        cc_female.add_coding(Coding(system=Ontologies.SNOMEDCT.value["url"], code="248152002", display="f (Female)"))
        assert transform.fairify_value(column_name="sex", value=transform.data.iloc[0][4]).to_json() == cc_female.to_json()
        assert transform.fairify_value(column_name="ethnicity", value=transform.data.iloc[0][5]) == "white"
        assert not is_not_nan(transform.fairify_value(column_name="date_of_birth", value=transform.data.iloc[0][6]))
        assert transform.fairify_value(column_name="date_of_birth", value=transform.data.iloc[5][6]) == cast_value("2021-12-22 11:58:38.881")
        