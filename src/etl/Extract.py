import json
import os
import re

import pandas as pd

from src.analysis.ValueAnalysis import ValueAnalysis
from src.analysis.VariableAnalysis import VariableAnalysis
from src.config.BetterConfig import BetterConfig
from src.database.Database import Database
from src.utils.HospitalNames import HospitalNames
from src.utils.Ontologies import Ontologies
from src.utils.utils import is_not_nan, convert_value, get_values_from_json_values
from src.utils.constants import METADATA_VARIABLES
from src.utils.setup_logger import log


class Extract:

    def __init__(self, database: Database, run_analysis: bool, config: BetterConfig):
        self.metadata = None
        self.data = None
        self.mapped_values = {}  # accepted values for some categorical columns (column "JSON_values" in metadata)
        self.mapped_types = {}  # expected data type for columns (column "vartype" in metadata)

        self.config = config
        self.database = database

        # flags
        self.run_analysis = run_analysis

    def run(self):
        self.load_metadata_file()
        self.load_data_file()
        self.compute_mapped_values()
        self.compute_mapped_types()

        if self.run_analysis:
            self.run_value_analysis()
            self.run_variable_analysis()

    def load_metadata_file(self):
        assert os.path.exists(self.config.get_metadata_filepath()), "The provided metadata file could not be found. Please check the filepath you specify when running this script."

        log.info("Metadata filepath is %s.", self.config.get_metadata_filepath())

        # index_col is False to not add a column with line numbers
        self.metadata = pd.read_csv(self.config.get_metadata_filepath(), index_col=False)

        # For UC2 and UC3 metadata files, we need to keep only the variables for the current hospital
        # and remove the columns for other hospitals
        if self.config.get_hospital_name() not in [HospitalNames.IT_BUZZI_UC1.value, HospitalNames.RS_IMGGE.value, HospitalNames.ES_HSJD.value]:
            # for those metadata files, we need to split them to obtain one per
            self.preprocess_metadata_file()
        else:
            pass

        # For any metadata file, we need to keep only the variables that concern the current dataset
        filename = os.path.basename(self.config.get_data_filepath())
        log.debug(self.metadata["dataset"].unique())
        if filename not in self.metadata["dataset"].unique():
            log.error("The current dataset is not described in the provided metadata file.")
            exit()
        else:
            self.metadata = self.metadata[self.metadata["dataset"] == filename]

        # lower case all column names to avoid inconsistencies
        self.metadata['name'] = self.metadata['name'].apply(lambda x: x.lower())

        # remove spaces in ontology names and codes
        self.metadata["ontology"] = self.metadata["ontology"].apply(lambda value: str(value).replace(" ", ""))
        self.metadata["ontology_code"] = self.metadata["ontology_code"].apply(lambda value: str(value).replace(" ", ""))
        self.metadata["secondary_ontology"] = self.metadata["secondary_ontology"].apply(lambda value: str(value).replace(" ", ""))
        self.metadata["secondary_ontology_code"] = self.metadata["secondary_ontology_code"].apply(lambda value: str(value).replace(" ", ""))

        # the non-NaN JSON_values values are of the form: "{...}, {...}, ..."
        # thus, we need to
        # 1. create an empty list
        # 2. add each JSON dict of JSON_values in that list
        # Note: we cannot simply add brackets around the dicts because it would add a string with the dicts in the list
        # we cannot either use json.loads on row["JSON_values"] because it is not parsable (it lacks the brackets)
        for index, row in self.metadata.iterrows():
            if is_not_nan(row["JSON_values"]):
                values_dicts = []
                json_dicts = re.split('}, {', row["JSON_values"])
                for json_dict in json_dicts:
                    if not json_dict.startswith("{"):
                        json_dict = "{" + json_dict
                    if not json_dict.endswith("}"):
                        json_dict = json_dict + "}"
                    values_dicts.append(json.loads(json_dict))
                self.metadata.loc[index, "JSON_values"] = json.dumps(values_dicts)  # set the new JSON values as a string

        log.info("%s columns and %s lines in the metadata file.", len(self.metadata.columns), len(self.metadata))

    def preprocess_metadata_file(self):
        # 1. capitalize and replace spaces in column names
        self.metadata.rename(columns=lambda x: x.upper().replace(" ", "_"), inplace=True)

        # 2. for each hospital, get its associated metadata
        log.debug("working on hospital %s", self.config.get_hospital_name())
        # a. we remove columns that are talking about other hospitals, and keep metadata variables + the column for the current hospital
        columns_to_keep = []
        columns_to_keep.extend([meta_variable.upper().replace(" ", "_") for meta_variable in METADATA_VARIABLES])
        columns_to_keep.append(self.config.get_hospital_name())
        log.debug(self.metadata.columns)
        log.debug(columns_to_keep)
        self.metadata = self.metadata[columns_to_keep]
        # b. we filter metadata that is not part of the current hospital (to avoid having the whole metadata for each hospital)
        self.metadata = self.metadata[self.metadata[self.config.get_hospital_name()] == 1]
        # c. we remove the column for the hospital, now that we have filtered the rows using it
        log.debug("will drop %s in %s", self.config.get_hospital_name(), self.metadata.columns)
        self.metadata = self.metadata.drop(self.config.get_hospital_name(), axis=1)
        log.debug(self.metadata)

    def load_data_file(self):
        assert os.path.exists(self.config.get_data_filepath()), "The provided samples file could not be found. Please check the filepath you specify when running this script."

        log.info("Data filepath is %s.", self.config.get_data_filepath())

        # index_col is False to not add a column with line numbers
        self.data = pd.read_csv(self.config.get_data_filepath(), index_col=False)

        # lower case all column names to avoid inconsistencies
        self.data.columns = self.data.columns.str.lower()

        log.info("%s columns and %s lines in the data file.", len(self.data.columns), len(self.data))

    def compute_mapped_values(self):
        self.mapped_values = {}

        for index, row in self.metadata.iterrows():
            if is_not_nan(row["JSON_values"]):
                current_dicts = json.loads(row["JSON_values"])
                parsed_dicts = []
                for current_dict in current_dicts:
                    # if we can convert the JSON value to a float or an int, we do it, otherwise we let it as a string
                    current_dict["value"] = convert_value(current_dict["value"])
                    # if we can also convert the snomed_ct / loinc code, we do it
                    # TODO Nelly: loop on all ontologies known in OntologyNames
                    if Ontologies.SNOMEDCT.value["name"] in current_dict:
                        current_dict[Ontologies.SNOMEDCT.value["name"]] = convert_value(current_dict[Ontologies.SNOMEDCT.value])
                    if Ontologies.LOINC.value["name"] in current_dict:
                        current_dict[Ontologies.LOINC.value["name"]] = convert_value(current_dict[Ontologies.LOINC.value])
                    parsed_dicts.append(current_dict)
                self.mapped_values[row["name"]] = parsed_dicts
        log.debug(self.mapped_values)

    def compute_mapped_types(self):
        self.mapped_types = {}

        for index, row in self.metadata.iterrows():
            if is_not_nan(row["vartype"]):
                self.mapped_types[row["name"]] = row["vartype"]  # we associate the column name to its expected type
        log.debug(self.mapped_types)

    def run_value_analysis(self):
        log.debug(self.mapped_values)
        # for each column in the sample data (and not in the metadata because some (empty) data columns are not
        # present in the metadata file), we compare the set of values it takes against the accepted set of values
        # (available in the mapped_values variable)
        for column in self.data.columns:
            values = pd.Series(self.data[column].values)
            values = values.apply(lambda value: value.casefold().strip() if isinstance(value, str) else value)
            # log.debug("Values are: %s", values)
            # log.debug(self.metadata["name"])
            # log.info("Working on column '%s'", column)
            # trying to get the expected type of the current column
            if column in self.mapped_types:
                expected_type = self.mapped_types[column]
            else:
                expected_type = ""
            # trying to get expected values for the current column
            if column in self.mapped_values:
                # self.mapped_values[column] contains the mappings (JSON dicts) for the given column
                # we need to get only the set of values described in the mappings of the given column
                accepted_values = get_values_from_json_values(json_values=self.mapped_values[column])
            else:
                accepted_values = []
            value_analysis = ValueAnalysis(column_name=column, values=values, expected_type=expected_type, accepted_values=accepted_values)
            value_analysis.run_analysis()
            if value_analysis.nb_unrecognized_data_types > 0 or (0 < value_analysis.ratio_non_empty_values_matching_accepted < 1):
                log.info("%s: %s", column, value_analysis)

    def run_variable_analysis(self):
        variable_analysis = VariableAnalysis(samples=self.data, metadata=self.metadata)
        variable_analysis.run_analysis()
        log.info(variable_analysis)
