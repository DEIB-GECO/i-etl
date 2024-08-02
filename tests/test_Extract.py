import json
import os
import unittest

from database.Database import Database
from database.Execution import Execution
from enums.DataTypes import DataTypes
from etl.Extract import Extract
from enums.HospitalNames import HospitalNames
from enums.MetadataColumns import MetadataColumns
from enums.TheTestFiles import TheTestFiles
from utils.constants import DEFAULT_DB_CONNECTION, TEST_DB_NAME
from utils.setup_logger import log
from utils.utils import is_not_nan


# personalized setup called at the beginning of each test
def my_setup(metadata_path: str, data_paths: str, hospital_name: str) -> Extract:
    args = {
        Execution.DB_CONNECTION_KEY: DEFAULT_DB_CONNECTION,
        Execution.DB_DROP_KEY: True,
        Execution.CLINICAL_METADATA_PATH_KEY: metadata_path,
        Execution.CLINICAL_DATA_PATHS_KEY: data_paths,
        Execution.HOSPITAL_NAME_KEY: hospital_name
    }
    TestExtract.execution.set_up(args_as_dict=args, setup_data_files=True)
    TestExtract.execution.current_filepath = data_paths
    database = Database(TestExtract.execution)
    extract = Extract(database=database, execution=TestExtract.execution)
    return extract


class TestExtract(unittest.TestCase):
    execution = Execution(TEST_DB_NAME)

    def test_load_metadata_file_H1_D1(self):
        extract = my_setup(metadata_path=TheTestFiles.TEST_ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.TEST_ORIG_CLINICAL_PATH,
                           hospital_name=HospitalNames.TEST_H1)
        extract.load_metadata_file()

        log.debug(extract.metadata.columns)

        # a. general size checks
        assert extract.metadata is not None, "Metadata is None, while it should not."
        assert len(extract.metadata.columns) == 11, "The expected number of columns is 11."
        assert len(extract.metadata) == 8, "The expected number of lines is 8."

        # b. checking the first line completely
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_SYSTEM][1] == "loinc"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][1] == "1234"
        assert not is_not_nan(extract.metadata[MetadataColumns.SEC_ONTOLOGY_SYSTEM][1])  # this should be nan as the cell is empty
        assert not is_not_nan(extract.metadata[MetadataColumns.SEC_ONTOLOGY_CODE][1])  # this should be empty too
        assert extract.metadata[MetadataColumns.COLUMN_NAME][1] == "molecule_a"  # all lower case
        assert extract.metadata[MetadataColumns.SIGNIFICATION_EN][1] == "The molecule Alpha"  # kept as it is in the metadata for more clarity
        assert extract.metadata[MetadataColumns.VAR_TYPE][1] == "float"
        assert not is_not_nan(extract.metadata[MetadataColumns.JSON_VALUES][1])  # empty cell thus nan
        # test JSON values in the fourth line (sex)
        # pandas dataframe does not allow json objects, so we have to store them as JSON-like string
        expected_json_values = [{"value": "m", "explanation": "Male", "snomedct": "24815:3007"}, {"value": "f", "explanation": "Female", "snomedct": "248152002"}]
        assert extract.metadata[MetadataColumns.JSON_VALUES][4] == json.dumps(expected_json_values)

        # c. test the first (Patient ID) line, because there are no ontologies for this one
        assert extract.metadata[MetadataColumns.COLUMN_NAME][0] == "id"  # normalized column name
        assert extract.metadata[MetadataColumns.SIGNIFICATION_EN][0] == "The Patient ID"  # non-normalized description

        # d. test normalization of ontology codes
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_SYSTEM][1] == "loinc"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_SYSTEM][5] == "loinc"
        assert not is_not_nan(extract.metadata[MetadataColumns.FIRST_ONTOLOGY_SYSTEM][2])
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_SYSTEM][3] == "snomedct"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_SYSTEM][4] == "snomedct"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_SYSTEM][6] == "snomedct"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_SYSTEM][7] == "snomedct"
        assert not is_not_nan(extract.metadata[MetadataColumns.FIRST_ONTOLOGY_SYSTEM][0])

        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][1] == "1234"
        assert not is_not_nan(extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][2])
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][3] == "123:678"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][4] == "123:789"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][5] == "45678"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][6] == "456:7z9"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][7] == "124678"
        assert not is_not_nan(extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][0])

        # e. check var type normalization
        log.debug(extract.metadata.columns)
        log.debug(extract.metadata.to_string())
        assert extract.metadata[MetadataColumns.VAR_TYPE][0] == DataTypes.INTEGER  # patient id
        assert extract.metadata[MetadataColumns.VAR_TYPE][1] == DataTypes.FLOAT  # molecule A
        assert extract.metadata[MetadataColumns.VAR_TYPE][2] == DataTypes.STRING  # molecule B
        assert extract.metadata[MetadataColumns.VAR_TYPE][3] == DataTypes.BOOLEAN  # molecule G
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
        log.debug(unique_dataset_names)
        log.debug(TheTestFiles.TEST_ORIG_CLINICAL_PATH.split(os.sep)[-1])
        assert len(unique_dataset_names) == 1
        assert unique_dataset_names[0] == TheTestFiles.TEST_ORIG_CLINICAL_PATH.split(os.sep)[-1]

        log.debug(extract.metadata.to_string())

    def test_load_metadata_file_H1_D2(self):
        extract = my_setup(metadata_path=TheTestFiles.TEST_ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.TEST_ORIG_DISEASE_PATH,
                           hospital_name=HospitalNames.TEST_H1)
        extract.load_metadata_file()

        log.debug(extract.metadata.columns)

        # a. general size checks
        assert extract.metadata is not None, "Metadata is None, while it should not."
        assert len(extract.metadata.columns) == 11, "The expected number of columns is 11."
        assert len(extract.metadata) == 3, "The expected number of lines is 3."

        log.debug(extract.metadata.to_string())

        # b. checking the first line completely
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_SYSTEM][2] == "omim"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][2] == "1245/983"
        assert not is_not_nan(extract.metadata[MetadataColumns.SEC_ONTOLOGY_SYSTEM][2])  # this should be nan as the cell is empty
        assert not is_not_nan(extract.metadata[MetadataColumns.SEC_ONTOLOGY_CODE][2])  # this should be empty too
        assert extract.metadata[MetadataColumns.COLUMN_NAME][2] == "disease_form"  # all lower case, with an underscore
        assert extract.metadata[MetadataColumns.SIGNIFICATION_EN][2] == "The form of the disease"  # kept as it is in the metadata for more clarity
        assert extract.metadata[MetadataColumns.VAR_TYPE][2] == "category"  # all lower case
        assert extract.metadata[MetadataColumns.ETL_TYPE][2] == "category"  # all lower case
        # test JSON values in the second line (disease form)
        # pandas dataframe does not allow json objects, so we have to store them as JSON-like string
        expected_json_values = [{"value": "start", "explanation": "< 1 year", "pubchem": "023468"}, {"value": "middle", "explanation": "1 year <= ... <= 3 years", "pubchem": "203:468"}, {"value": "end", "explanation": "> 3 years", "pubchem": "4097625"}]
        assert extract.metadata[MetadataColumns.JSON_VALUES][2] == json.dumps(expected_json_values)

        # c. test the first (Patient ID) line, because there are no ontologies for this one
        assert extract.metadata[MetadataColumns.COLUMN_NAME][0] == "id"  # normalized column name
        assert extract.metadata[MetadataColumns.SIGNIFICATION_EN][0] == "The Patient ID"  # non-normalized description

        # d. test normalization of ontology codes
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_SYSTEM][1] == "omim"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_SYSTEM][2] == "omim"
        assert not is_not_nan(extract.metadata[MetadataColumns.FIRST_ONTOLOGY_SYSTEM][0])

        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][1] == "1569-456"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][2] == "1245/983"
        assert not is_not_nan(extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][0])

        # e. more general checks
        # DATASET: this should be the dataset name, and there should be no other datasets in that column
        unique_dataset_names = list(extract.metadata[MetadataColumns.DATASET_NAME].unique())
        log.debug(unique_dataset_names)
        log.debug(TheTestFiles.TEST_ORIG_DISEASE_PATH.split(os.sep)[-1])
        assert len(unique_dataset_names) == 1
        assert unique_dataset_names[0] == TheTestFiles.TEST_ORIG_DISEASE_PATH.split(os.sep)[-1]

        log.debug(extract.metadata.to_string())

    def test_load_metadata_file_H3_D1(self):
        extract = my_setup(metadata_path=TheTestFiles.TEST_ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.TEST_ORIG_GENOMICS_PATH,
                           hospital_name=HospitalNames.TEST_H3)
        extract.load_metadata_file()

        log.debug(extract.metadata.columns)

        # a. general size checks
        assert extract.metadata is not None, "Metadata is None, while it should not."
        assert len(extract.metadata.columns) == 11, "The expected number of columns is 11."
        assert len(extract.metadata) == 3, "The expected number of lines is 3."

        log.debug(extract.metadata.to_string())

        # b. checking the first line completely
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_SYSTEM][2] == "loinc"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][2] == "3265970"
        assert not is_not_nan(extract.metadata[MetadataColumns.SEC_ONTOLOGY_SYSTEM][2])  # this should be nan as the cell is empty
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
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_SYSTEM][1] == "loinc"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_SYSTEM][2] == "loinc"
        assert not is_not_nan(extract.metadata[MetadataColumns.FIRST_ONTOLOGY_SYSTEM][0])

        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][1] == "326597056"
        assert extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][2] == "3265970"
        assert not is_not_nan(extract.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE][0])

        # e. more general checks
        # DATASET: this should be the dataset name, and there should be no other datasets in that column
        unique_dataset_names = list(extract.metadata[MetadataColumns.DATASET_NAME].unique())
        log.debug(unique_dataset_names)
        log.debug(TheTestFiles.TEST_ORIG_GENOMICS_PATH.split(os.sep)[-1])
        assert len(unique_dataset_names) == 1
        assert unique_dataset_names[0] == TheTestFiles.TEST_ORIG_GENOMICS_PATH.split(os.sep)[-1]

        log.debug(extract.metadata.to_string())

    def test_load_data_file_H1_D1(self):
        extract = my_setup(metadata_path=TheTestFiles.TEST_ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.TEST_ORIG_CLINICAL_PATH,
                           hospital_name=HospitalNames.TEST_H1)
        extract.execution.current_filepath = TheTestFiles.TEST_ORIG_CLINICAL_PATH  # set the test data as the currently processed file
        extract.load_data_file()

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
        assert extract.data["molecule_g"][0] == "true", "The expected value is 'true'."
        assert extract.data["molecule_z"][0] == "abc", "The expected value is "
        assert extract.data["sex"][0] == "f"
        assert extract.data["ethnicity"][0] == "white"
        assert not is_not_nan(extract.data["date_of_birth"][0]) is True
        log.debug(extract.data.to_string())

    def test_load_data_file_H3_D1(self):
        extract = my_setup(metadata_path=TheTestFiles.TEST_ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.TEST_ORIG_GENOMICS_PATH,
                           hospital_name=HospitalNames.TEST_H3)
        extract.execution.current_filepath = TheTestFiles.TEST_ORIG_GENOMICS_PATH  # set the test data as the currently processed file
        extract.load_data_file()

        # a. general size checks
        assert extract.data is not None, "Data is None, while it should not."
        assert len(extract.data.columns) == 3, "The expected number of columns is 3."
        assert len(extract.data) == 10, "The expected number of lines is 10."

        log.debug(extract.data.to_string())

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
        log.debug(extract.data.to_string())

    def test_compute_mapped_values(self):
        extract = my_setup(metadata_path=TheTestFiles.TEST_ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.TEST_ORIG_CLINICAL_PATH,
                           hospital_name=HospitalNames.TEST_H1)
        extract.load_metadata_file()  # required to compute mapped values
        extract.compute_mapped_values()

        log.debug(extract.mapped_values)

        assert len(extract.mapped_values.keys()) == 1  # only sex has categorical values in the test data
        assert "sex" in extract.mapped_values.keys()
        assert len(extract.mapped_values["sex"]) == 2  # mappings for female, male
        # checking "male" mapping
        assert len(extract.mapped_values["sex"][0]) == 3  # value, explanation, and ontology keys
        assert "value" in extract.mapped_values["sex"][0]
        assert extract.mapped_values["sex"][0]["value"] == "m"  # normalized categorical value
        assert "explanation" in extract.mapped_values["sex"][0]
        assert extract.mapped_values["sex"][0]["explanation"] == "Male"  # not normalized (human) description
        assert "snomedct" in extract.mapped_values["sex"][0]  # normalized (ontology) key
        assert extract.mapped_values["sex"][0]["snomedct"] == "24815:3007"  # normalized ontology code
        # checking "female" mapping
        assert len(extract.mapped_values["sex"][1]) == 3
        assert "value" in extract.mapped_values["sex"][1]
        assert extract.mapped_values["sex"][1]["value"] == "f"
        assert "explanation" in extract.mapped_values["sex"][1]
        assert extract.mapped_values["sex"][1]["explanation"] == "Female"
        assert "snomedct" in extract.mapped_values["sex"][1]
        assert extract.mapped_values["sex"][1]["snomedct"] == "248152002"

    def test_removed_unused_columns(self):
        extract = my_setup(metadata_path=TheTestFiles.TEST_ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.TEST_ORIG_CLINICAL_PATH,
                           hospital_name=HospitalNames.TEST_H1)
        extract.load_metadata_file()
        extract.load_data_file()  # both operations are needed to run the method
        extract.remove_unused_columns()

        log.debug(extract.metadata[MetadataColumns.COLUMN_NAME])

        # we assert that we get rid of the 'molecule_z' column (because it was not described in the metadata
        # other columns are kept
        remaining_data_columns = list(extract.data.columns)
        remaining_data_columns.sort()
        log.debug(remaining_data_columns)
        assert len(remaining_data_columns) == 7  # molecule_z has been removed
        assert remaining_data_columns == ['date_of_birth', 'ethnicity', 'id', 'molecule_a', 'molecule_b', 'molecule_g', 'sex']

        # we assert that we kept 'molecule_y' column (even though it was not in the data, but still described in the metadata)
        # other columns are kept
        described_columns = list(extract.metadata[MetadataColumns.COLUMN_NAME])
        described_columns.sort()
        assert len(described_columns) == 8  # molecule_y has been kept
        assert described_columns == ['date_of_birth', 'ethnicity', 'id', 'molecule_a', 'molecule_b', 'molecule_g', 'molecule_y', 'sex']

    def test_compute_column_to_dimension(self):
        extract = my_setup(metadata_path=TheTestFiles.TEST_ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.TEST_ORIG_CLINICAL_PATH,
                           hospital_name=HospitalNames.TEST_H1)
        extract.load_metadata_file()
        extract.load_data_file()  # both operations are needed to run the method
        log.debug(extract.metadata.to_string())
        extract.remove_unused_columns()
        log.debug(extract.metadata.to_string())
        extract.compute_column_to_dimension()

        # {'id': [], 'molecule_a': ['mg/L'], 'molecule_b': ['kg', 'grams', 'g'], 'molecule_g': [], 'sex': [], 'ethnicity': [], 'date_of_birth': []}
        assert len(extract.column_to_dimension.keys()) == 8
        assert "id" in extract.column_to_dimension
        assert extract.column_to_dimension["id"] is None
        assert "molecule_a" in extract.column_to_dimension
        assert extract.column_to_dimension["molecule_a"] == "mg/L"
        assert "molecule_b" in extract.column_to_dimension
        assert extract.column_to_dimension["molecule_b"] == "g"
        assert "molecule_g" in extract.column_to_dimension
        assert extract.column_to_dimension["molecule_g"] is None
        assert "sex" in extract.column_to_dimension
        assert extract.column_to_dimension["sex"] is None
        assert "ethnicity" in extract.column_to_dimension
        assert extract.column_to_dimension["ethnicity"] is None
        assert "date_of_birth" in extract.column_to_dimension
        assert extract.column_to_dimension["date_of_birth"] is None
        assert "molecule_y" in extract.column_to_dimension
        assert extract.column_to_dimension["molecule_y"] is None

    # def test_run_value_analysis(self):
    #     self.fail()
    #
    # def test_run_variable_analysis(self):
    #     pass
