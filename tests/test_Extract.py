import json
import os
import unittest

import numpy as np
import pandas as pd

from constants.structure import TEST_DB_NAME, DOCKER_FOLDER_TEST
from database.Database import Database
from database.Execution import Execution
from enums.DataTypes import DataTypes
from enums.HospitalNames import HospitalNames
from enums.MetadataColumns import MetadataColumns
from enums.Ontologies import Ontologies
from enums.ParameterKeys import ParameterKeys
from enums.Profile import Profile
from enums.TheTestFiles import TheTestFiles
from etl.Extract import Extract
from statistics.QualityStatistics import QualityStatistics
from utils.file_utils import read_tabular_file_as_string
from utils.test_utils import set_env_variables_from_dict


# personalized setup called at the beginning of each test
def my_setup(metadata_path: str, data_paths: str, data_type: str, pids_path: str, hospital_name: str) -> Extract:
    args = {
        ParameterKeys.HOSPITAL_NAME: hospital_name,
        ParameterKeys.DB_NAME: TEST_DB_NAME,
        ParameterKeys.DB_DROP: "True",
        ParameterKeys.METADATA_PATH: metadata_path,
        ParameterKeys.DATA_FILES: data_paths,
        ParameterKeys.ANONYMIZED_PATIENT_IDS: pids_path
    }
    set_env_variables_from_dict(env_vars=args)
    TestExtract.execution.internals_set_up()
    TestExtract.execution.file_set_up(setup_files=True)  # we need to set up the metadata file at least
    # in the dev mode, the current_filepath variable is set during the ETL
    # for tests, we need to manually set it because we have no ETL instance
    TestExtract.execution.current_filepath = os.path.join(DOCKER_FOLDER_TEST, data_paths)
    metadata = read_tabular_file_as_string(filepath=os.path.join(DOCKER_FOLDER_TEST, metadata_path))
    database = Database(execution=TestExtract.execution)
    extract = Extract(metadata=metadata, profile=str(data_type), database=database, execution=TestExtract.execution, quality_stats=QualityStatistics(record_stats=False))
    extract.filter_metadata_file()
    extract.normalize_metadata_file()
    extract.load_tabular_data()
    extract.pre_process_data_file()
    extract.filter_data_file()
    extract.normalize_data_file()
    return extract


class TestExtract(unittest.TestCase):
    execution = Execution()

    def test_load_metadata_file_H1_D1(self):
        extract = my_setup(metadata_path=TheTestFiles.ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.ORIG_CLINICAL_PATH,
                           data_type=Profile.CLINICAL,
                           pids_path=TheTestFiles.PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H1)

        # a. general size checks
        assert extract.metadata is not None, "Metadata is None, while it should not."
        assert len(extract.metadata.columns) == 21, "The expected number of columns is 21."
        assert len(extract.metadata) == 6, "The expected number of lines is 6."

        # b. checking the "sid" line completely
        assert extract.metadata[MetadataColumns.ONTO_NAME][1] == ""
        assert extract.metadata[MetadataColumns.ONTO_CODE][1] == ""
        assert extract.metadata[MetadataColumns.COLUMN_NAME][1] == "sid"  # all lower case
        assert extract.metadata[MetadataColumns.SIGNIFICATION_EN][1] == "The Sample ID"  # kept as it is in the metadata for more clarity
        assert extract.metadata[MetadataColumns.ETL_TYPE][1] == DataTypes.STRING
        assert extract.metadata[MetadataColumns.JSON_VALUES][1] == ""
        assert extract.metadata[MetadataColumns.VISIBILITY][1] == "PUBLIC"
        assert extract.metadata[MetadataColumns.DOMAIN][1] == ""

        # g. more general checks
        # DATASET: this should be the dataset name, and there should be no other datasets in that column
        unique_dataset_names = list(extract.metadata[MetadataColumns.DATASET_NAME].unique())
        assert len(unique_dataset_names) == 1
        assert unique_dataset_names[0] == TheTestFiles.ORIG_CLINICAL_PATH.split(os.sep)[-1]

    def test_load_metadata_file_H1_D2(self):
        extract = my_setup(metadata_path=TheTestFiles.ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.ORIG_PHENOTYPIC_PATH,
                           data_type=Profile.PHENOTYPIC,
                           pids_path=TheTestFiles.PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H1)

        # a. general size checks
        assert extract.metadata is not None, "Metadata is None, while it should not."
        assert len(extract.metadata.columns) == 21, "The expected number of columns is 21."
        assert len(extract.metadata) == 4, "The expected number of lines is 4."

        # b. checking the "sex" line completely
        assert extract.metadata[MetadataColumns.ONTO_NAME][1] == "snomedct"
        assert extract.metadata[MetadataColumns.ONTO_CODE][1] == "123: 789"
        assert extract.metadata[MetadataColumns.COLUMN_NAME][1] == "sex"  # all lower case
        assert extract.metadata[MetadataColumns.SIGNIFICATION_EN][1] == "The sex at birth"  # kept as it is in the metadata for more clarity
        # pandas dataframe does not allow json objects, so we have to store them as JSON-like string
        expected_json_values = [{"value": "M", "snomedct": "248153007"}, {"value": "F", "snomedct": "248152002"}]
        # "[{""value"": ""M"", ""snomedct"": ""248153007""}, {""value"": ""F"", ""snomedct"": ""248152002""}]"
        assert extract.metadata[MetadataColumns.JSON_VALUES][1] == json.dumps(expected_json_values)
        assert extract.metadata[MetadataColumns.VISIBILITY][1] == "PUBLIC"
        assert extract.metadata[MetadataColumns.DOMAIN][1] == ""

        # c. test the first (Patient ID) line, because there are no ontologies for this one
        assert extract.metadata[MetadataColumns.COLUMN_NAME][0] == "id"  # normalized column name
        assert extract.metadata[MetadataColumns.SIGNIFICATION_EN][0] == "The Patient ID"  # non-normalized description

        # d. test normalization of ontology names
        assert extract.metadata[MetadataColumns.ONTO_NAME][0] == ""
        assert extract.metadata[MetadataColumns.ONTO_NAME][1] == "snomedct"
        assert extract.metadata[MetadataColumns.ONTO_NAME][2] == "loinc"
        assert extract.metadata[MetadataColumns.ONTO_NAME][3] == ""

        # assert that codes are not normalized yet (they will be normalized within OntoResource)
        assert extract.metadata[MetadataColumns.ONTO_CODE][0] == ""
        assert extract.metadata[MetadataColumns.ONTO_CODE][1] == "123: 789"
        assert extract.metadata[MetadataColumns.ONTO_CODE][2] == " 46463-6 "
        assert extract.metadata[MetadataColumns.ONTO_CODE][3] == ""

        # e. check ETL type normalization
        assert extract.metadata[MetadataColumns.ETL_TYPE][0] == DataTypes.INTEGER  # patient id
        assert extract.metadata[MetadataColumns.ETL_TYPE][1] == DataTypes.CATEGORY  # sex
        assert extract.metadata[MetadataColumns.ETL_TYPE][2] == DataTypes.STRING  # ethnicity
        assert extract.metadata[MetadataColumns.ETL_TYPE][3] == DataTypes.DATETIME  # date of birth

        # f. more general checks
        # DATASET: this should be the dataset name, and there should be no other datasets in that column
        unique_dataset_names = list(extract.metadata[MetadataColumns.DATASET_NAME].unique())
        assert len(unique_dataset_names) == 1
        assert unique_dataset_names[0] == TheTestFiles.ORIG_PHENOTYPIC_PATH.split(os.sep)[-1]

    def test_load_metadata_file_H1_D3(self):
        extract = my_setup(metadata_path=TheTestFiles.ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.ORIG_DIAGNOSIS_PATH,
                           data_type=Profile.DIAGNOSIS,
                           pids_path=TheTestFiles.PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H1)

        # a. general size checks
        assert extract.metadata is not None, "Metadata is None, while it should not."
        assert len(extract.metadata.columns) == 21, "The expected number of columns is 21."
        assert len(extract.metadata) == 3, "The expected number of lines is 3."

        # b. checking the first line completely
        assert extract.metadata[MetadataColumns.ONTO_NAME][2] == "omim"
        assert extract.metadata[MetadataColumns.ONTO_CODE][2] == "1245/   983 "  # this will be normalized later when building OntologyResource objects
        assert extract.metadata[MetadataColumns.COLUMN_NAME][2] == "disease_form"  # all lower case, with an underscore
        assert extract.metadata[MetadataColumns.SIGNIFICATION_EN][2] == "The form of the disease"  # kept as it is in the metadata for more clarity
        assert extract.metadata[MetadataColumns.ETL_TYPE][2] == "category"  # all lower case
        # test JSON values in the second line (disease form)
        # pandas dataframe does not allow json objects, so we have to store them as JSON-like string
        # expected_json_values = [{"value": "start", "pubchem": "023468"}, {"value": "middle", "pubchem": "203:468"}, {"value": "end", "pubchem": "4097625"}]
        # assert extract.metadata[MetadataColumns.JSON_VALUES][2] == json.dumps(expected_json_values)

        # c. test the first (Patient ID) line, because there are no ontologies for this one
        assert extract.metadata[MetadataColumns.COLUMN_NAME][0] == "id"  # normalized column name
        assert extract.metadata[MetadataColumns.SIGNIFICATION_EN][0] == "The Patient ID"  # non-normalized description

        # d. test normalization of ontology names
        assert extract.metadata[MetadataColumns.ONTO_NAME][1] == "omim"
        assert extract.metadata[MetadataColumns.ONTO_NAME][2] == "omim"
        assert extract.metadata[MetadataColumns.ONTO_NAME][0] == ""

        # d. test normalization of ontology codes (later normalized)
        assert extract.metadata[MetadataColumns.ONTO_CODE][1] == "1569 - 456"  # this will be normalized later with OntologyResource
        assert extract.metadata[MetadataColumns.ONTO_CODE][2] == "1245/   983 "
        assert extract.metadata[MetadataColumns.ONTO_CODE][0] == ""

        # e. more general checks
        # DATASET: this should be the dataset name, and there should be no other datasets in that column
        unique_dataset_names = list(extract.metadata[MetadataColumns.DATASET_NAME].unique())
        assert len(unique_dataset_names) == 1
        assert unique_dataset_names[0] == TheTestFiles.ORIG_DIAGNOSIS_PATH.split(os.sep)[-1]

    def test_load_metadata_file_H3_D1(self):
        extract = my_setup(metadata_path=TheTestFiles.ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.ORIG_GENOMICS_PATH,
                           data_type=Profile.GENOMIC,
                           pids_path=TheTestFiles.PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H3)

        # a. general size checks
        assert extract.metadata is not None, "Metadata is None, while it should not."
        assert len(extract.metadata.columns) == 21, "The expected number of columns is 21."
        assert len(extract.metadata) == 3, "The expected number of lines is 3."

        # b. checking the first line completely
        assert extract.metadata[MetadataColumns.ONTO_NAME][2] == "loinc"
        assert extract.metadata[MetadataColumns.ONTO_CODE][2] == "3265970"
        assert extract.metadata[MetadataColumns.COLUMN_NAME][2] == "is_inherited"  # all lower case, with an underscore
        assert extract.metadata[MetadataColumns.SIGNIFICATION_EN][2] == "Whether the gene is inherited"  # kept as it is in the metadata for more clarity
        assert extract.metadata[MetadataColumns.ETL_TYPE][2] == "bool"  # all lower case
        assert extract.metadata[MetadataColumns.JSON_VALUES][2] == ""

        # c. test the first (Patient ID) line, because there are no ontologies for this one
        assert extract.metadata[MetadataColumns.COLUMN_NAME][0] == "id"  # normalized column name
        assert extract.metadata[MetadataColumns.SIGNIFICATION_EN][0] == "The Patient ID"  # non-normalized description

        # d. test normalization of ontology codes
        assert extract.metadata[MetadataColumns.ONTO_NAME][1] == "loinc"
        assert extract.metadata[MetadataColumns.ONTO_NAME][2] == "loinc"
        assert extract.metadata[MetadataColumns.ONTO_NAME][0] == ""

        assert extract.metadata[MetadataColumns.ONTO_CODE][1] == "326597056"
        assert extract.metadata[MetadataColumns.ONTO_CODE][2] == "3265970"
        assert extract.metadata[MetadataColumns.ONTO_CODE][0] == ""

        # e. more general checks
        # DATASET: this should be the dataset name, and there should be no other datasets in that column
        unique_dataset_names = list(extract.metadata[MetadataColumns.DATASET_NAME].unique())
        assert len(unique_dataset_names) == 1
        assert unique_dataset_names[0] == TheTestFiles.ORIG_GENOMICS_PATH.split(os.sep)[-1]

    def test_load_data_file_H1_D1(self):
        extract = my_setup(metadata_path=TheTestFiles.EXTR_METADATA_CLINICAL_PATH,
                           data_paths=TheTestFiles.ORIG_CLINICAL_PATH,
                           data_type=Profile.CLINICAL,
                           pids_path=TheTestFiles.PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H1)
        # a. general size checks
        assert extract.data is not None, "Data is None, while it should not."
        assert len(extract.data.columns) == 5, "The expected number of columns is 5."  # because molecule_y is in the metadata but not the data
        assert len(extract.data) == 10, "The expected number of lines is 10."

        # b. checking the first line completely
        # recall that in dataframes (and Pandas):
        # - everything is a string (we will cast them to their true type when building resources in the Transform step)
        # - when a column contains at least one empty cell, the column type is float, thus numbers are read as floats
        #   for instance 100 is read 100 if there are no empty cells in that column, 100.0 otherwise
        assert extract.data["sid"][0] == "s1", "The expected id is 's1'."
        assert extract.data["id"][0] == "999999999", "The expected id is '999999999'."
        assert extract.data["molecule_a"][0] == "0.001", "The expected value is '0.001'."
        assert extract.data["molecule_b"][0] == "100g", "The expected value is '100'."
        assert extract.data["molecule_g"][0] == "1", "The expected value is '1'."  # this will be later converted to bool
        # assert extract.data["molecule_z"][0] == "abc", "The expected value is 'abc'."  # this has been removed because not described in the metadata

    def test_load_data_file_H1_D2(self):
        extract = my_setup(metadata_path=TheTestFiles.EXTR_METADATA_PHENOTYPIC_PATH,
                           data_paths=TheTestFiles.ORIG_PHENOTYPIC_PATH,
                           data_type=Profile.PHENOTYPIC,
                           pids_path=TheTestFiles.PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H1)

        # a. general size checks
        assert extract.data is not None, "Data is None, while it should not."
        assert len(extract.data.columns) == 4, "The expected number of columns is 4."
        assert len(extract.data) == 10, "The expected number of lines is 10."

        # b. checking the first line completely
        # recall that in dataframes (and Pandas):
        # - everything is a string (we will cast them to their true type when building resources in the Transform step)
        # - when a column contains at least one empty cell, the column type is float, thus numbers are read as floats
        #   for instance 100 is read 100 if there are no empty cells in that column, 100.0 otherwise
        assert extract.data["id"][0] == "999999999", "The expected id is '999999999'."
        assert extract.data["sex"][0] == "f"
        assert extract.data["ethnicity"][0] == "white"
        assert extract.data["date_of_birth"][0] == ""

    def test_load_data_file_H3_D1(self):
        extract = my_setup(metadata_path=TheTestFiles.ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.ORIG_GENOMICS_PATH,
                           data_type=Profile.GENOMIC,
                           pids_path=TheTestFiles.PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H3)

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
        assert pd.isnull(extract.data["is_inherited"][4])  # the "n/a" value is indeed converted to a NaN
        assert extract.data["gene"][5] == "sdr-125"
        assert extract.data["is_inherited"][5] == "false"
        assert extract.data["gene"][6] == "dec-123"
        assert extract.data["is_inherited"][6] == "false"
        assert extract.data["gene"][7] == "gft-568"
        assert pd.isnull(extract.data["is_inherited"][7])  # the "NaN" value is indeed converted to a NaN
        assert extract.data["gene"][8] == "plo-719"
        assert extract.data["is_inherited"][8] == ""
        assert extract.data["gene"][9] == ""
        assert extract.data["is_inherited"][9] == ""

    def test_compute_sam_mapped_values(self):
        extract = my_setup(metadata_path=TheTestFiles.EXTR_METADATA_CLINICAL_PATH,
                           data_paths=TheTestFiles.EXTR_CLINICAL_DATA_PATH,
                           data_type=Profile.CLINICAL,
                           pids_path=TheTestFiles.PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H1)
        extract.compute_mapping_categorical_value_to_onto_resource()

        print(extract.mapping_column_to_categorical_value)
        assert "molecule_g" in extract.mapping_column_to_categorical_value  # only one categorical feature
        assert len(extract.mapping_column_to_categorical_value) == 1
        assert len(extract.mapping_column_to_categorical_value["molecule_g"].keys()) == 3  # 0, 1, NA
        # checking "0" mapping
        assert "0" in extract.mapping_column_to_categorical_value["molecule_g"]  # normalized categorical value
        cc_0 = extract.mapping_column_to_categorical_value["molecule_g"]["0"]
        assert len(cc_0) == 3  # system, code, and label keys
        assert "system" in cc_0
        assert "code" in cc_0
        assert "label" in cc_0
        assert cc_0["label"] == "No"  # display got from the API
        assert cc_0["system"] == Ontologies.SNOMEDCT["url"]  # normalized (ontology) key
        assert cc_0["code"] == "373067005"  # normalized ontology code
        # checking "1" mapping
        assert "1" in extract.mapping_column_to_categorical_value["molecule_g"]  # normalized categorical value
        cc_1 = extract.mapping_column_to_categorical_value["molecule_g"]["1"]
        assert len(cc_1) == 3  # system, code, and label keys
        assert "system" in cc_1
        assert "code" in cc_1
        assert "label" in cc_1
        assert cc_1["label"] == "Yes"  # display computed with the API
        assert cc_1["system"] == Ontologies.SNOMEDCT["url"]  # normalized (ontology) key
        assert cc_1["code"] == "373066001"  # normalized ontology code
        # checking "na" mapping
        assert np.nan in extract.mapping_column_to_categorical_value["molecule_g"]  # normalized categorical value; np.nan is the key name
        cc_na = extract.mapping_column_to_categorical_value["molecule_g"][np.nan]
        assert len(cc_na) == 3  # system, code, and label keys
        assert "system" in cc_na
        assert "code" in cc_na
        assert "label" in cc_na
        assert cc_na["label"] == "Null"  # display computed with the API
        assert cc_na["system"] == Ontologies.SNOMEDCT["url"]  # normalized (ontology) key
        assert cc_na["code"] == "276727009"  # normalized ontology code

    def test_compute_phen_mapped_values(self):
        extract = my_setup(metadata_path=TheTestFiles.EXTR_METADATA_PHENOTYPIC_PATH,
                           data_paths=TheTestFiles.EXTR_PHENOTYPIC_DATA_PATH,
                           data_type=Profile.PHENOTYPIC,
                           pids_path=TheTestFiles.PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H1)
        extract.compute_mapping_categorical_value_to_onto_resource()

        print(extract.mapping_column_to_categorical_value)
        assert "sex" in extract.mapping_column_to_categorical_value
        assert len(extract.mapping_column_to_categorical_value.keys()) == 1  # only sex feature is here
        # checking "male" mapping
        assert "m" in extract.mapping_column_to_categorical_value["sex"]  # normalized categorical value
        cc_male = extract.mapping_column_to_categorical_value["sex"]["m"]
        assert len(cc_male) == 3  # system, code, and label keys
        assert "system" in cc_male
        assert "code" in cc_male
        assert "label" in cc_male
        assert cc_male["label"] == "Male"  # display got from the API
        assert cc_male["system"] == Ontologies.SNOMEDCT["url"]  # normalized (ontology) key
        assert cc_male["code"] == "248153007"  # normalized ontology code
        # checking "female" mapping
        assert "f" in extract.mapping_column_to_categorical_value["sex"]  # normalized categorical value
        cc_female = extract.mapping_column_to_categorical_value["sex"]["f"]
        assert len(cc_female) == 3  # system, code, and label keys
        assert "system" in cc_female
        assert "code" in cc_female
        assert "label" in cc_female
        assert cc_female["label"] == "Female"  # display computed with the API
        assert cc_female["system"] == Ontologies.SNOMEDCT["url"]  # normalized (ontology) key
        assert cc_female["code"] == "248152002"  # normalized ontology code

    def test_removed_unused_columns(self):
        extract = my_setup(metadata_path=TheTestFiles.ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.ORIG_CLINICAL_PATH,
                           data_type=Profile.CLINICAL,
                           pids_path=TheTestFiles.PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H1)

        # we assert that we get rid of the 'molecule_z' column (because it was not described in the metadata)
        # other columns are kept
        remaining_data_columns = list(extract.data.columns)
        remaining_data_columns.sort()
        expected_columns = ["id", "molecule_a", "molecule_b", "molecule_g", "sid"]  # do not change order, otherwise the comparison fails
        assert len(remaining_data_columns) == len(expected_columns)  # molecule_z has been removed
        assert remaining_data_columns == expected_columns

        # we assert that we kept 'molecule_y' column (even though it was not in the data, but still described in the metadata)
        # other columns are kept
        described_columns = list(extract.metadata[MetadataColumns.COLUMN_NAME])
        described_columns.sort()
        expected_columns = ["id", "molecule_a", "molecule_b", "molecule_g", "molecule_y", "sid"]
        assert len(described_columns) == len(expected_columns)  # molecule_y has been kept
        assert described_columns == expected_columns

    def test_compute_clin_column_to_unit(self):
        extract = my_setup(metadata_path=TheTestFiles.ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.ORIG_CLINICAL_PATH,
                           data_type=Profile.CLINICAL,
                           pids_path=TheTestFiles.PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H1)
        extract.compute_column_to_unit()

        # {'id': None, 'molecule_a': "mg/L", 'molecule_b': "g", 'molecule_g': None, 'molecule_z': None, "molecule_y": None}
        assert len(extract.mapping_column_to_unit.keys()) == 6
        assert "id" in extract.mapping_column_to_unit
        assert extract.mapping_column_to_unit["id"] is None
        assert "molecule_a" in extract.mapping_column_to_unit
        assert extract.mapping_column_to_unit["molecule_a"] == "mg/L"
        assert "molecule_b" in extract.mapping_column_to_unit
        assert extract.mapping_column_to_unit["molecule_b"] is None  # because we do not extract units from data anymore
        assert "molecule_g" in extract.mapping_column_to_unit
        assert extract.mapping_column_to_unit["molecule_g"] is None
        assert "molecule_y" in extract.mapping_column_to_unit
        assert extract.mapping_column_to_unit["molecule_y"] is None

    def test_compute_phen_column_to_unit(self):
        extract = my_setup(metadata_path=TheTestFiles.ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.ORIG_PHENOTYPIC_PATH,
                           data_type=Profile.PHENOTYPIC,
                           pids_path=TheTestFiles.PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H1)
        extract.compute_column_to_unit()

        # {'id': None, 'sex': [], 'ethnicity': [], 'date_of_birth': []}
        assert len(extract.mapping_column_to_unit.keys()) == 4
        assert "id" in extract.mapping_column_to_unit
        assert extract.mapping_column_to_unit["id"] is None
        assert "sex" in extract.mapping_column_to_unit
        assert extract.mapping_column_to_unit["sex"] is None
        assert "ethnicity" in extract.mapping_column_to_unit
        assert extract.mapping_column_to_unit["ethnicity"] is None
        assert "date_of_birth" in extract.mapping_column_to_unit
        assert extract.mapping_column_to_unit["date_of_birth"] is None

    def test_compute_phen_column_to_domain(self):
        extract = my_setup(metadata_path=TheTestFiles.ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.ORIG_PHENOTYPIC_PATH,
                           data_type=Profile.PHENOTYPIC,
                           pids_path=TheTestFiles.PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H1)
        extract.compute_column_to_domain()

        # {'id': None, 'sex': [], 'ethnicity': [], 'date_of_birth': []}
        assert len(extract.mapping_column_to_domain.keys()) == 4
        assert "id" in extract.mapping_column_to_domain
        assert extract.mapping_column_to_domain["id"] is None
        assert "sex" in extract.mapping_column_to_domain
        assert extract.mapping_column_to_domain["sex"] is None
        assert "ethnicity" in extract.mapping_column_to_domain
        assert extract.mapping_column_to_domain["ethnicity"] is None
        assert "date_of_birth" in extract.mapping_column_to_domain
        assert extract.mapping_column_to_domain["date_of_birth"] is None

    def test_compute_clin_column_to_domain(self):
        extract = my_setup(metadata_path=TheTestFiles.ORIG_METADATA_PATH,
                           data_paths=TheTestFiles.ORIG_CLINICAL_PATH,
                           data_type=Profile.CLINICAL,
                           pids_path=TheTestFiles.PIDS_PATH,
                           hospital_name=HospitalNames.TEST_H1)
        extract.compute_column_to_domain()

        # {'id': None,
        #  'sid': None,
        #  'molecule_a': {""min"": 0},
        #  'molecule_b': None,
        #  'molecule_g': None,
        #  'molecule_y': {""min"": 0, ""max"": 5}
        # }
        assert len(extract.mapping_column_to_domain.keys()) == 6
        assert "id" in extract.mapping_column_to_domain
        assert extract.mapping_column_to_domain["id"] is None
        assert "sid" in extract.mapping_column_to_domain
        assert extract.mapping_column_to_domain["sid"] is None
        assert "molecule_a" in extract.mapping_column_to_domain
        assert len(extract.mapping_column_to_domain["molecule_a"]) == 1
        assert "min" in extract.mapping_column_to_domain["molecule_a"]
        assert extract.mapping_column_to_domain["molecule_a"]["min"] == 0
        assert "molecule_b" in extract.mapping_column_to_domain
        assert extract.mapping_column_to_domain["molecule_b"] is None
        assert "molecule_g" in extract.mapping_column_to_domain
        assert extract.mapping_column_to_domain["molecule_g"] is None
        assert "molecule_y" in extract.mapping_column_to_domain
        assert len(extract.mapping_column_to_domain["molecule_y"]) == 2
        assert "min" in extract.mapping_column_to_domain["molecule_y"]
        assert extract.mapping_column_to_domain["molecule_y"]["min"] == 0
        assert "max" in extract.mapping_column_to_domain["molecule_y"]
        assert extract.mapping_column_to_domain["molecule_y"]["max"] == 5
