import json
import os
import re

import pandas as pd
from pandas import DataFrame

from analysis.ValueAnalysis import ValueAnalysis
from analysis.VariableAnalysis import VariableAnalysis
from database.Database import Database
from database.Execution import Execution
from utils.HospitalNames import HospitalNames
from utils.MetadataColumns import MetadataColumns
from utils.Ontologies import Ontologies
from utils.constants import METADATA_VARIABLES
from utils.setup_logger import log
from utils.utils import is_not_nan, convert_value, get_values_from_json_values


class Extract:

    def __init__(self, database: Database, execution: Execution):
        self._metadata = None
        self._data = None
        self._mapped_values = {}  # accepted values for some categorical columns (column "JSON_values" in metadata)
        self._mapped_types = {}  # expected data type for columns (column "vartype" in metadata)

        self._execution = execution
        self._database = database

    def run(self) -> None:
        self.load_metadata_file()
        self.load_data_file()
        self.compute_mapped_values()
        self.compute_mapped_types()

        if self._execution.is_analyze:
            self.run_value_analysis()
            self.run_variable_analysis()

    def load_metadata_file(self) -> None:
        log.info(f"Metadata filepath is {self._execution.clinical_metadata_filepath}")

        # index_col is False to not add a column with line numbers
        self._metadata = pd.read_csv(self._execution.clinical_metadata_filepath, index_col=False)

        # For UC2 and UC3 metadata files, we need to keep only the variables for the current hospital
        # and remove the columns for other hospitals
        if self._execution.hospital_name not in [HospitalNames.IT_BUZZI_UC1.value, HospitalNames.RS_IMGGE.value, HospitalNames.ES_HSJD.value]:
            # for those metadata files, we need to split them to obtain one per
            self.preprocess_metadata_file()
        else:
            pass

        # For any metadata file, we need to keep only the variables that concern the current dataset
        filename = os.path.basename(self._execution.clinical_filepaths[0])
        log.debug(f"{filename}")
        log.debug(f"{self._metadata[MetadataColumns.DATASET_NAME.value].unique()}")
        if filename not in self._metadata[MetadataColumns.DATASET_NAME.value].unique():
            raise ValueError(f"The current dataset ({filename}) is not described in the provided metadata file.")
        else:
            self._metadata = self._metadata[self._metadata[MetadataColumns.DATASET_NAME.value] == filename]

        # lower case all column names to avoid inconsistencies
        self._metadata[MetadataColumns.COLUMN_NAME.value] = self._metadata[MetadataColumns.COLUMN_NAME.value].apply(lambda x: x.lower())

        # remove spaces in ontology names and codes
        self._metadata[MetadataColumns.FIRST_ONTOLOGY_SYSTEM.value] = self._metadata[MetadataColumns.FIRST_ONTOLOGY_SYSTEM.value].apply(lambda value: str(value).replace(" ", ""))
        self._metadata[MetadataColumns.FIRST_ONTOLOGY_CODE.value] = self._metadata[MetadataColumns.FIRST_ONTOLOGY_CODE.value].apply(lambda value: str(value).replace(" ", ""))
        self._metadata[MetadataColumns.SEC_ONTOLOGY_SYSTEM.value] = self._metadata[MetadataColumns.SEC_ONTOLOGY_SYSTEM.value].apply(lambda value: str(value).replace(" ", ""))
        self._metadata[MetadataColumns.SEC_ONTOLOGY_CODE.value] = self._metadata[MetadataColumns.SEC_ONTOLOGY_CODE.value].apply(lambda value: str(value).replace(" ", ""))

        # the non-NaN JSON_values values are of the form: "{...}, {...}, ..."
        # thus, we need to
        # 1. create an empty list
        # 2. add each JSON dict of JSON_values in that list
        # Note: we cannot simply add brackets around the dicts because it would add a string with the dicts in the list
        # we cannot either use json.loads on row["JSON_values"] because it is not parsable (it lacks the brackets)
        for index, row in self._metadata.iterrows():
            if is_not_nan(row[MetadataColumns.JSON_VALUES.value]):
                values_dicts = []
                json_dicts = re.split('}, {', row[MetadataColumns.JSON_VALUES.value])
                for json_dict in json_dicts:
                    if not json_dict.startswith("{"):
                        json_dict = "{" + json_dict
                    if not json_dict.endswith("}"):
                        json_dict = json_dict + "}"
                    values_dicts.append(json.loads(json_dict))
                self._metadata.loc[index, MetadataColumns.JSON_VALUES.value] = json.dumps(values_dicts)  # set the new JSON values as a string

        log.info(f"{len(self._metadata.columns)} columns and {len(self._metadata)} lines in the metadata file.")

    def preprocess_metadata_file(self) -> None:
        # 1. capitalize and replace spaces in column names
        self._metadata.rename(columns=lambda x: x.upper().replace(" ", "_"), inplace=True)

        # 2. for each hospital, get its associated metadata
        log.debug(f"working on hospital {self._execution.hospital_name}")
        # a. we remove columns that are talking about other hospitals, and keep metadata variables + the column for the current hospital
        columns_to_keep = []
        columns_to_keep.extend([meta_variable.upper().replace(" ", "_") for meta_variable in METADATA_VARIABLES])
        columns_to_keep.append(self._execution.hospital_name)
        log.debug(f"{self._metadata.columns}")
        log.debug(f"{columns_to_keep}")
        self._metadata = self._metadata[columns_to_keep]
        # b. we filter metadata that is not part of the current hospital (to avoid having the whole metadata for each hospital)
        self._metadata = self._metadata[self.metadata[self._execution.hospital_name] == 1]
        # c. we remove the column for the hospital, now that we have filtered the rows using it
        log.debug(f"will drop {self._execution.hospital_name} in {self.metadata.columns}")
        self._metadata = self._metadata.drop(self._execution.hospital_name, axis=1)
        log.debug(f"{self._metadata}")

    def load_data_file(self) -> None:
        log.info(f"{self._execution.current_filepath}")
        assert os.path.exists(self._execution.current_filepath), "The provided samples file could not be found. Please check the filepath you specify when running this script."

        log.info(f"Data filepath is {self._execution.current_filepath}")

        # index_col is False to not add a column with line numbers
        self._data = pd.read_csv(self._execution.current_filepath, index_col=False)

        # lower case all column names to avoid inconsistencies
        self._data.columns = self._data.columns.str.lower()

        log.info(f"{len(self._data.columns)} columns and {len(self._data)} lines in the data file.")

    def compute_mapped_values(self) -> None:
        self._mapped_values = {}

        for index, row in self._metadata.iterrows():
            if is_not_nan(row[MetadataColumns.JSON_VALUES.value]):
                current_dicts = json.loads(row[MetadataColumns.JSON_VALUES.value])
                parsed_dicts = []
                for current_dict in current_dicts:
                    # if we can convert the JSON value to a float or an int, we do it, otherwise we let it as a string
                    current_dict["value"] = convert_value(value=current_dict["value"])
                    # if we can also convert the snomed_ct / loinc code, we do it
                    # TODO Nelly: loop on all ontologies known in OntologyNames
                    if Ontologies.SNOMEDCT.value["name"] in current_dict:
                        current_dict[Ontologies.SNOMEDCT.value["name"]] = convert_value(value=current_dict[Ontologies.SNOMEDCT.value])
                    if Ontologies.LOINC.value["name"] in current_dict:
                        current_dict[Ontologies.LOINC.value["name"]] = convert_value(value=current_dict[Ontologies.LOINC.value])
                    parsed_dicts.append(current_dict)
                self._mapped_values[row["name"]] = parsed_dicts
        log.debug(f"{self._mapped_values}")

    def compute_mapped_types(self) -> None:
        self._mapped_types = {}

        for index, row in self._metadata.iterrows():
            if is_not_nan(row[MetadataColumns.VAR_TYPE.value]):
                # we associate the column name to its expected type
                self._mapped_types[row[MetadataColumns.COLUMN_NAME.value]] = row[MetadataColumns.VAR_TYPE.value]
        log.debug(f"{self._mapped_types}")

    def run_value_analysis(self) -> None:
        log.debug(f"{self._mapped_values}")
        # for each column in the sample data (and not in the metadata because some (empty) data columns are not
        # present in the metadata file), we compare the set of values it takes against the accepted set of values
        # (available in the mapped_values variable)
        for column in self._data.columns:
            values = pd.Series(self._data[column].values)
            values = values.apply(lambda value: value.casefold().strip() if isinstance(value, str) else value)
            # log.debug("Values are: %s", values)
            # log.debug(self.metadata["name"])
            # log.info("Working on column '%s'", column)
            # trying to get the expected type of the current column
            if column in self._mapped_types:
                expected_type = self._mapped_types[column]
            else:
                expected_type = ""
            # trying to get expected values for the current column
            if column in self._mapped_values:
                # self.mapped_values[column] contains the mappings (JSON dicts) for the given column
                # we need to get only the set of values described in the mappings of the given column
                accepted_values = get_values_from_json_values(json_values=self._mapped_values[column])
            else:
                accepted_values = []
            value_analysis = ValueAnalysis(column_name=column, values=values, expected_type=expected_type, accepted_values=accepted_values)
            value_analysis.run_analysis()
            if value_analysis.nb_unrecognized_data_types > 0 or (0 < value_analysis.ratio_non_empty_values_matching_accepted < 1):
                log.info(f"{column}: {value_analysis}")

    def run_variable_analysis(self) -> None:
        variable_analysis = VariableAnalysis(samples=self._data, metadata=self.metadata)
        variable_analysis.run_analysis()
        log.info(f"{variable_analysis}")

    @property
    def data(self) -> DataFrame:
        return self._data

    @property
    def metadata(self) -> DataFrame:
        return self._metadata

    @property
    def mapped_values(self) -> dict:
        return self._mapped_values
