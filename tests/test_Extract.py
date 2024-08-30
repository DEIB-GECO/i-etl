import json
import os
import unittest

from database.Database import Database
from database.Execution import Execution
from enums.DataTypes import DataTypes
from enums.ParameterKeys import ParameterKeys
from enums.FileTypes import FileTypes
from enums.HospitalNames import HospitalNames
from enums.MetadataColumns import MetadataColumns
from enums.Ontologies import Ontologies
from enums.TheTestFiles import TheTestFiles
from etl.Extract import Extract
from utils.constants import TEST_DB_NAME, DOCKER_FOLDER_TEST
from utils.setup_logger import log
from utils.utils import is_not_nan, set_env_variables_from_dict


# personalized setup called at the beginning of each test
def my_setup(metadata_path: str, data_paths: str, data_type: FileTypes, pids_path: str, hospital_name: str) -> Extract:
    key_data_paths = FileTypes.get_execution_key(data_type)
    log.info(f"{key_data_paths} for data type {data_type}")
    args = {
        ParameterKeys.HOSPITAL_NAME: hospital_name,
        ParameterKeys.DB_NAME: TEST_DB_NAME,
        ParameterKeys.DB_DROP: "True",
        ParameterKeys.METADATA_PATH: metadata_path,
        key_data_paths: data_paths,
        ParameterKeys.ANONYMIZED_PATIENT_IDS: pids_path
    }
    set_env_variables_from_dict(env_vars=args)
    TestExtract.execution.set_up(setup_data_files=True)
    TestExtract.execution.current_filepath = get_filepath_from_execution(datatype=data_type)
    log.debug(TestExtract.execution.current_filepath)
    database = Database(TestExtract.execution)
    extract = Extract(database=database, execution=TestExtract.execution)
    return extract


def get_filepath_from_execution(datatype: FileTypes):
    if datatype == FileTypes.LABORATORY:
        return TestExtract.execution.laboratory_filepaths[0]
    elif datatype == FileTypes.DIAGNOSIS:
        return TestExtract.execution.diagnosis_filepaths[0]
    elif datatype == FileTypes.MEDICINE:
        return TestExtract.execution.medicine_filepaths[0]
    elif datatype == FileTypes.GENOMIC:
        return TestExtract.execution.genomic_filepaths[0]
    elif datatype == FileTypes.IMAGING:
        return TestExtract.execution.imaging_filepaths[0]
    else:
        return None


class TestExtract(unittest.TestCase):
    execution = Execution()

    def test_load_metadata_file_H1_D1(self):
        extract = my_setup(metadata_path=TheTestFiles.ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.ORIG_LABORATORY_PATH,
                           data_type=FileTypes.LABORATORY,
                           pids_path=TheTestFiles.ORIG_EMPTY_PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H1)
        extract.load_metadata_file()

        log.debug(extract.metadata.columns)

        # a. general size checks
        assert extract.metadata is not None, "Metadata is None, while it should not."
        assert len(extract.metadata.columns) == 11, "The expected number of columns is 11."
        assert len(extract.metadata) == 8, "The expected number of lines is 8."

        # b. checking the first line completely
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_NAME][1] == "loinc"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][1] == "1234"
        assert not is_not_nan(extract.metadata[MetadataColumns.SEC_ONTOLOGY_NAME][1])  # this should be nan as the cell is empty
        assert not is_not_nan(extract.metadata[MetadataColumns.SEC_ONTOLOGY_CODE][1])  # this should be empty too
        assert extract.metadata[MetadataColumns.COLUMN_NAME][1] == "molecule_a"  # all lower case
        assert extract.metadata[MetadataColumns.SIGNIFICATION_EN][1] == "The molecule Alpha"  # kept as it is in the metadata for more clarity
        assert extract.metadata[MetadataColumns.VAR_TYPE][1] == "float"
        assert not is_not_nan(extract.metadata[MetadataColumns.JSON_VALUES][1])  # empty cell thus nan
        # test JSON values in the fourth line (sex)
        # pandas dataframe does not allow json objects, so we have to store them as JSON-like string
        # expected_json_values = [{"value": "m", "explanation": "Male", "snomedct": "248152002"}, {"value": "f", "explanation": "Female", "snomedct": "248152002"}]
        # assert extract.metadata[MetadataColumns.JSON_VALUES][4] == json.dumps(expected_json_values)

        # c. test the first (Patient ID) line, because there are no ontologies for this one
        assert extract.metadata[MetadataColumns.COLUMN_NAME][0] == "id"  # normalized column name
        assert extract.metadata[MetadataColumns.SIGNIFICATION_EN][0] == "The Patient ID"  # non-normalized description

        # d. test normalization of ontology codes
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_NAME][1] == "loinc"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_NAME][5] == "loinc"
        assert not is_not_nan(extract.metadata[MetadataColumns.FIRST_ONTOLOGY_NAME][2])
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_NAME][3] == "snomedct"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_NAME][4] == "snomedct"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_NAME][6] == "snomedct"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_NAME][7] == "snomedct"
        assert not is_not_nan(extract.metadata[MetadataColumns.FIRST_ONTOLOGY_NAME][0])

        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][1] == "1234"
        assert not is_not_nan(extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][2])
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][3] == "421416008"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][4] == "123: 789"  # this will be normalized later while building OntologyCode objects
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][5] == " 46463-6 "  # same as above
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][6] == "456:7z9"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][7] == "124678"
        assert not is_not_nan(extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][0])

        # e. check var type normalization
        log.debug(extract.metadata.columns)
        log.debug(extract.metadata.to_string())
        assert extract.metadata[MetadataColumns.VAR_TYPE][0] == DataTypes.INTEGER  # patient id
        assert extract.metadata[MetadataColumns.VAR_TYPE][1] == DataTypes.FLOAT  # molecule A
        assert extract.metadata[MetadataColumns.VAR_TYPE][2] == DataTypes.STRING  # molecule B
        assert extract.metadata[MetadataColumns.VAR_TYPE][3] == DataTypes.CATEGORY  # molecule G  # this is later converted to bool
        assert extract.metadata[MetadataColumns.VAR_TYPE][4] == DataTypes.CATEGORY  # sex
        assert extract.metadata[MetadataColumns.VAR_TYPE][5] == DataTypes.STRING  # ethnicity
        assert extract.metadata[MetadataColumns.VAR_TYPE][6] == DataTypes.DATETIME  # date of birth

        # f. check ETL type normalization
        log.debug(extract.metadata.columns)
        log.debug(MetadataColumns.ETL_TYPE)
        assert extract.metadata[MetadataColumns.ETL_TYPE][0] == DataTypes.INTEGER  # patient id
        assert extract.metadata[MetadataColumns.ETL_TYPE][1] == DataTypes.FLOAT  # molecule A
        assert extract.metadata[MetadataColumns.ETL_TYPE][2] == DataTypes.INTEGER  # molecule B
        assert extract.metadata[MetadataColumns.ETL_TYPE][3] == DataTypes.BOOLEAN  # molecule G
        assert extract.metadata[MetadataColumns.ETL_TYPE][4] == DataTypes.CATEGORY  # sex
        assert extract.metadata[MetadataColumns.ETL_TYPE][5] == DataTypes.STRING  # ethnicity
        assert extract.metadata[MetadataColumns.ETL_TYPE][6] == DataTypes.DATETIME  # date of birth

        # g. more general checks
        # DATASET: this should be the dataset name, and there should be no other datasets in that column
        unique_dataset_names = list(extract.metadata[MetadataColumns.DATASET_NAME].unique())
        assert len(unique_dataset_names) == 1
        assert unique_dataset_names[0] == TheTestFiles.ORIG_LABORATORY_PATH.split(os.sep)[-1]

    def test_load_metadata_file_H1_D2(self):
        extract = my_setup(metadata_path=TheTestFiles.ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.ORIG_DISEASE_PATH,
                           data_type=FileTypes.DIAGNOSIS,
                           pids_path=TheTestFiles.ORIG_EMPTY_PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H1)
        extract.load_metadata_file()

        # a. general size checks
        assert extract.metadata is not None, "Metadata is None, while it should not."
        assert len(extract.metadata.columns) == 11, "The expected number of columns is 11."
        assert len(extract.metadata) == 3, "The expected number of lines is 3."

        # b. checking the first line completely
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_NAME][2] == "omim"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][2] == "1245/   983 "  # this will be normalized later when building OntologyCode objects
        assert not is_not_nan(extract.metadata[MetadataColumns.SEC_ONTOLOGY_NAME][2])  # this should be nan as the cell is empty
        assert not is_not_nan(extract.metadata[MetadataColumns.SEC_ONTOLOGY_CODE][2])  # this should be empty too
        assert extract.metadata[MetadataColumns.COLUMN_NAME][2] == "disease_form"  # all lower case, with an underscore
        assert extract.metadata[MetadataColumns.SIGNIFICATION_EN][2] == "The form of the disease"  # kept as it is in the metadata for more clarity
        assert extract.metadata[MetadataColumns.VAR_TYPE][2] == "category"  # all lower case
        assert extract.metadata[MetadataColumns.ETL_TYPE][2] == "category"  # all lower case
        # test JSON values in the second line (disease form)
        # pandas dataframe does not allow json objects, so we have to store them as JSON-like string
        # expected_json_values = [{"value": "start", "explanation": "< 1 year", "pubchem": "023468"}, {"value": "middle", "explanation": "1 year <= ... <= 3 years", "pubchem": "203:468"}, {"value": "end", "explanation": "> 3 years", "pubchem": "4097625"}]
        # assert extract.metadata[MetadataColumns.JSON_VALUES][2] == json.dumps(expected_json_values)

        # c. test the first (Patient ID) line, because there are no ontologies for this one
        assert extract.metadata[MetadataColumns.COLUMN_NAME][0] == "id"  # normalized column name
        assert extract.metadata[MetadataColumns.SIGNIFICATION_EN][0] == "The Patient ID"  # non-normalized description

        # d. test normalization of ontology codes
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_NAME][1] == "omim"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_NAME][2] == "omim"
        assert not is_not_nan(extract.metadata[MetadataColumns.FIRST_ONTOLOGY_NAME][0])

        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][1] == "1569 - 456"  # this will be normalized later with OntologyCode
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][2] == "1245/   983 "
        assert not is_not_nan(extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][0])

        # e. more general checks
        # DATASET: this should be the dataset name, and there should be no other datasets in that column
        unique_dataset_names = list(extract.metadata[MetadataColumns.DATASET_NAME].unique())
        assert len(unique_dataset_names) == 1
        assert unique_dataset_names[0] == TheTestFiles.ORIG_DISEASE_PATH.split(os.sep)[-1]

    def test_load_metadata_file_H3_D1(self):
        extract = my_setup(metadata_path=TheTestFiles.ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.ORIG_GENOMICS_PATH,
                           data_type=FileTypes.GENOMIC,
                           pids_path=TheTestFiles.ORIG_EMPTY_PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H3)
        extract.load_metadata_file()

        # a. general size checks
        assert extract.metadata is not None, "Metadata is None, while it should not."
        assert len(extract.metadata.columns) == 11, "The expected number of columns is 11."
        assert len(extract.metadata) == 3, "The expected number of lines is 3."

        # b. checking the first line completely
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_NAME][2] == "loinc"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][2] == "3265970"
        assert not is_not_nan(extract.metadata[MetadataColumns.SEC_ONTOLOGY_NAME][2])  # this should be nan as the cell is empty
        assert not is_not_nan(extract.metadata[MetadataColumns.SEC_ONTOLOGY_CODE][2])  # this should be empty too
        assert extract.metadata[MetadataColumns.COLUMN_NAME][2] == "is_inherited"  # all lower case, with an underscore
        assert extract.metadata[MetadataColumns.SIGNIFICATION_EN][2] == "Whether the gene is inherited"  # kept as it is in the metadata for more clarity
        assert extract.metadata[MetadataColumns.VAR_TYPE][2] == "bool"  # all lower case
        assert extract.metadata[MetadataColumns.ETL_TYPE][2] == "bool"  # all lower case
        assert not is_not_nan(extract.metadata[MetadataColumns.JSON_VALUES][2])

        # c. test the first (Patient ID) line, because there are no ontologies for this one
        assert extract.metadata[MetadataColumns.COLUMN_NAME][0] == "id"  # normalized column name
        assert extract.metadata[MetadataColumns.SIGNIFICATION_EN][0] == "The Patient ID"  # non-normalized description

        # d. test normalization of ontology codes
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_NAME][1] == "loinc"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_NAME][2] == "loinc"
        assert not is_not_nan(extract.metadata[MetadataColumns.FIRST_ONTOLOGY_NAME][0])

        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][1] == "326597056"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][2] == "3265970"
        assert not is_not_nan(extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][0])

        # e. more general checks
        # DATASET: this should be the dataset name, and there should be no other datasets in that column
        unique_dataset_names = list(extract.metadata[MetadataColumns.DATASET_NAME].unique())
        assert len(unique_dataset_names) == 1
        assert unique_dataset_names[0] == TheTestFiles.ORIG_GENOMICS_PATH.split(os.sep)[-1]

    def test_load_data_file_H1_D1(self):
        extract = my_setup(metadata_path=TheTestFiles.ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.ORIG_LABORATORY_PATH,
                           data_type=FileTypes.LABORATORY,
                           pids_path=TheTestFiles.ORIG_EMPTY_PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H1)
        extract.load_csv_data_file()

        # a. general size checks
        assert extract.data is not None, "Data is None, while it should not."
        assert len(extract.data.columns) == 8, "The expected number of columns is 17."
        assert len(extract.data) == 10, "The expected number of lines is 10."

        # b. checking the first line completely
        # recall that in dataframes (and Pandas):
        # - everything is a string (we will cast them to their true type when building resources in the Transform step)
        # - when a column contains at least one empty cell, the column type is float, thus numbers are read as floats
        #   for instance 100 is read 100 if there are no empty cells in that column, 100.0 otherwise
        assert extract.data["id"][0] == "999999999", "The expected id is '999999999'."
        assert extract.data["molecule_a"][0] == "0.001", "The expected value is '0.001'."
        assert extract.data["molecule_b"][0] == "100g", "The expected value is '100'."
        assert extract.data["molecule_g"][0] == "1", "The expected value is '1'."  # this will be later converted to bool
        assert extract.data["molecule_z"][0] == "abc", "The expected value is 'abc'."
        assert extract.data["sex"][0] == "f"
        assert extract.data["ethnicity"][0] == "white"
        assert not is_not_nan(extract.data["date_of_birth"][0]) is True

    def test_load_data_file_H3_D1(self):
        extract = my_setup(metadata_path=TheTestFiles.ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.ORIG_GENOMICS_PATH,
                           data_type=FileTypes.GENOMIC,
                           pids_path=TheTestFiles.ORIG_EMPTY_PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H3)
        extract.load_csv_data_file()

        # a. general size checks
        assert extract.data is not None, "Data is None, while it should not."
        assert len(extract.data.columns) == 3, "The expected number of columns is 3."
        assert len(extract.data) == 10, "The expected number of lines is 10."

        # b. checking the first line completely
        assert extract.data["id"][0] == "999999999"
        assert extract.data["gene"][0] == "abc-123"
        assert extract.data["is_inherited"][0] == "true"
        assert extract.data["gene"][1] == "abc-123"
        assert extract.data["is_inherited"][1] == "true"
        assert extract.data["gene"][2] == "abc-128"
        assert extract.data["is_inherited"][2] == "false"
        assert extract.data["gene"][3] == "abd-123"
        assert extract.data["is_inherited"][3] == "true"
        assert extract.data["gene"][4] == "ade-183"
        assert not is_not_nan(extract.data["is_inherited"][4])  # the "n/a" value is indeed converted to a NaN
        assert extract.data["gene"][5] == "sdr-125"
        assert extract.data["is_inherited"][5] == "false"
        assert extract.data["gene"][6] == "dec-123"
        assert extract.data["is_inherited"][6] == "false"
        assert extract.data["gene"][7] == "gft-568"
        assert not is_not_nan(extract.data["is_inherited"][7])  # the "NaN" value is indeed converted to a NaN
        assert extract.data["gene"][8] == "plo-719"
        assert not is_not_nan(extract.data["is_inherited"][8])
        assert not is_not_nan(extract.data["gene"][9])
        assert not is_not_nan(extract.data["is_inherited"][9])

    def test_compute_mapped_values(self):
        extract = my_setup(metadata_path=TheTestFiles.ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.ORIG_LABORATORY_PATH,
                           data_type=FileTypes.LABORATORY,
                           pids_path=TheTestFiles.ORIG_EMPTY_PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H1)
        extract.load_metadata_file()  # required to compute mapped values
        extract.compute_mapping_categorical_value_to_cc()

        assert len(extract.mapping_categorical_value_to_cc.keys()) == 2  # 2 categorical values only (F/M)
        # checking "male" mapping
        assert "m" in extract.mapping_categorical_value_to_cc  # normalized categorical value
        cc_male = extract.mapping_categorical_value_to_cc["m"].to_json()
        assert "coding" in cc_male
        assert "text" in cc_male
        assert len(cc_male["coding"][0]) == 3  # system, code, and display keys
        assert "system" in cc_male["coding"][0]
        assert "code" in cc_male["coding"][0]
        assert "display" in cc_male["coding"][0]
        assert cc_male["coding"][0]["display"] == "Male"  # display got from the API
        assert cc_male["coding"][0]["system"] == Ontologies.SNOMEDCT["url"]  # normalized (ontology) key
        assert cc_male["coding"][0]["code"] == "248153007"  # normalized ontology code
        # checking "female" mapping
        assert "f" in extract.mapping_categorical_value_to_cc  # normalized categorical value
        cc_female = extract.mapping_categorical_value_to_cc["f"].to_json()
        assert "coding" in cc_female
        assert "text" in cc_female
        assert len(cc_female["coding"][0]) == 3  # system, code, and display keys
        assert "system" in cc_female["coding"][0]
        assert "code" in cc_female["coding"][0]
        assert "display" in cc_female["coding"][0]
        assert cc_female["coding"][0]["display"] == "Female"  # display computed with the API
        assert cc_female["coding"][0]["system"] == Ontologies.SNOMEDCT["url"]  # normalized (ontology) key
        assert cc_female["coding"][0]["code"] == "248152002"  # normalized ontology code

    def test_removed_unused_columns(self):
        extract = my_setup(metadata_path=TheTestFiles.ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.ORIG_LABORATORY_PATH,
                           data_type=FileTypes.LABORATORY,
                           pids_path=TheTestFiles.ORIG_EMPTY_PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H1)
        extract.load_metadata_file()
        extract.load_csv_data_file()  # both operations are needed to run the method
        extract.remove_unused_csv_columns()

        # we assert that we get rid of the 'molecule_z' column (because it was not described in the metadata
        # other columns are kept
        remaining_data_columns = list(extract.data.columns)
        remaining_data_columns.sort()
        assert len(remaining_data_columns) == 7  # molecule_z has been removed
        assert remaining_data_columns == ['date_of_birth', 'ethnicity', 'id', 'molecule_a', 'molecule_b', 'molecule_g', 'sex']

        # we assert that we kept 'molecule_y' column (even though it was not in the data, but still described in the metadata)
        # other columns are kept
        described_columns = list(extract.metadata[MetadataColumns.COLUMN_NAME])
        described_columns.sort()
        assert len(described_columns) == 8  # molecule_y has been kept
        assert described_columns == ['date_of_birth', 'ethnicity', 'id', 'molecule_a', 'molecule_b', 'molecule_g', 'molecule_y', 'sex']

    def test_compute_column_to_dimension(self):
        extract = my_setup(metadata_path=TheTestFiles.ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.ORIG_LABORATORY_PATH,
                           data_type=FileTypes.LABORATORY,
                           pids_path=TheTestFiles.ORIG_EMPTY_PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H1)
        extract.load_metadata_file()
        extract.load_csv_data_file()  # both operations are needed to run the method
        extract.remove_unused_csv_columns()
        extract.compute_column_to_dimension()

        # {'id': [], 'molecule_a': ['mg/L'], 'molecule_b': ['kg', 'grams', 'g'], 'molecule_g': [], 'sex': [], 'ethnicity': [], 'date_of_birth': []}
        assert len(extract.mapping_column_to_dimension.keys()) == 8
        assert "id" in extract.mapping_column_to_dimension
        assert extract.mapping_column_to_dimension["id"] is None
        assert "molecule_a" in extract.mapping_column_to_dimension
        assert extract.mapping_column_to_dimension["molecule_a"] == "mg/L"
        assert "molecule_b" in extract.mapping_column_to_dimension
        assert extract.mapping_column_to_dimension["molecule_b"] == "g"
        assert "molecule_g" in extract.mapping_column_to_dimension
        assert extract.mapping_column_to_dimension["molecule_g"] is None
        assert "sex" in extract.mapping_column_to_dimension
        assert extract.mapping_column_to_dimension["sex"] is None
        assert "ethnicity" in extract.mapping_column_to_dimension
        assert extract.mapping_column_to_dimension["ethnicity"] is None
        assert "date_of_birth" in extract.mapping_column_to_dimension
        assert extract.mapping_column_to_dimension["date_of_birth"] is None
        assert "molecule_y" in extract.mapping_column_to_dimension
        assert extract.mapping_column_to_dimension["molecule_y"] is None

    def test_load_empty_patient_id_mapping(self):
        extract = my_setup(metadata_path=TheTestFiles.ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.ORIG_LABORATORY_PATH,
                           data_type=FileTypes.LABORATORY,
                           pids_path=TheTestFiles.ORIG_EMPTY_PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H1)
        extract.load_patient_id_mapping()

        # when the file is empty, the Execution should write an empty list into it
        assert os.stat(extract.execution.anonymized_patient_ids_filepath).st_size > 0
        assert extract.patient_ids_mapping == {}

        # get back to the original empty file
        with open(os.path.join(DOCKER_FOLDER_TEST, TheTestFiles.ORIG_EMPTY_PIDS_PATH), "w") as file:
            file.write("")

    def test_load_filled_patient_id_mapping(self):
        extract = my_setup(metadata_path=TheTestFiles.ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.ORIG_LABORATORY_PATH,
                           data_type=FileTypes.LABORATORY,
                           pids_path=TheTestFiles.ORIG_FILLED_PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H1)
        extract.load_patient_id_mapping()

        # when the file is not empty, all mappings should be loaded in Extract
        assert os.stat(extract.execution.anonymized_patient_ids_filepath).st_size > 0
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
        assert expected_dict == extract.patient_ids_mapping

    def test_compute_mapping_disease_to_classification(self):
        extract = my_setup(metadata_path=TheTestFiles.ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.ORIG_DIAGNOSIS_CLASSIFICATION_PATH,
                           data_type=FileTypes.DIAGNOSIS_CLASSIFICATION,
                           pids_path=TheTestFiles.ORIG_FILLED_PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H1)
        extract.compute_mapping_disease_to_classification()

        with open(os.path.join(DOCKER_FOLDER_TEST, TheTestFiles.EXTR_JSON_DIAGNOSIS_CLASSIFICATION_PATH), "r") as f:
            expected_dict = json.load(f)
        assert expected_dict == extract.mapping_disease_to_classification  # == performs a deep equality

    def test_compute_mapping_disease_to_cc(self):
        extract = my_setup(metadata_path=TheTestFiles.ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.ORIG_DIAGNOSIS_TO_CC_PATH,
                           data_type=FileTypes.DIAGNOSIS_REGEX,
                           pids_path=TheTestFiles.ORIG_FILLED_PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H1)
        extract.compute_mapping_disease_to_cc()

        with open(os.path.join(DOCKER_FOLDER_TEST, TheTestFiles.EXTR_JSON_DIAGNOSIS_TO_CC_PATH), "r") as f:
            expected_dict = json.load(f)
        log.info(expected_dict)
        log.info(extract.mapping_disease_to_cc)
        assert expected_dict == extract.mapping_disease_to_cc  # == performs a deep equality

    # def test_run_value_analysis(self):
    #     self.fail()
    #
    # def test_run_variable_analysis(self):
    #     pass
