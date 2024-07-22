import json
import os
import re

import pandas as pd
from pandas import DataFrame

from analysis.ValueAnalysis import ValueAnalysis
from analysis.VariableAnalysis import VariableAnalysis
from database.Database import Database
from database.Execution import Execution
from enums.HospitalNames import HospitalNames
from enums.MetadataColumns import MetadataColumns
from enums.Ontologies import Ontologies
from utils.setup_logger import log
from utils.utils import is_not_nan, get_values_from_json_values, normalize_column_name, \
    normalize_ontology_system, normalize_ontology_code, normalize_column_value, normalize_hospital_name, \
    normalize_var_type, read_csv_file_as_string


class Extract:

    def __init__(self, database: Database, execution: Execution):
        self.metadata = None
        self.data = None
        self.mapped_values = {}  # accepted values for some categorical columns (column "JSON_values" in metadata)
        self.mapped_types = {}  # expected data type for columns (column "vartype" in metadata)

        self.execution = execution
        self.database = database

    def run(self) -> None:
        self.load_metadata_file()
        self.load_data_file()
        self.remove_unused_columns()
        self.compute_mapped_values()
        self.compute_mapped_types()

        if self.execution.is_analyze:
            self.run_value_analysis()
            self.run_variable_analysis()

    def load_metadata_file(self) -> None:
        log.info(f"Metadata filepath is {self.execution.clinical_metadata_filepath}")

        # index_col is False to not add a column with line numbers
        self.metadata = read_csv_file_as_string(self.execution.clinical_metadata_filepath)  # keep all metadata as str
        log.debug(self.metadata.dtypes)
        # log.debug(self.metadata.to_string())

        log.info("Will preprocess metadata")

        # 1. normalize the header, e.g., "Significato it" becomes "significato_it"
        self.metadata.rename(columns=lambda x: normalize_column_name(column_name=x), inplace=True)
        # log.debug(self.metadata.to_string())
        # we will also specifically normalize the hospital names if they are in the header (UC 2 and UC 3) when counting how many they are (see below)

        # 2. Get the metadata associated to the current hospital (but any dataset within that hospital)
        log.debug(f"working on hospital {self.execution.hospital_name}")
        # a. we remove columns indicating whether the column is present in the hospital if this is not the current one
        # for this we check whether there are more than 1 hospital name (> 1) in column names
        nb_hospitals_in_columns = 0
        for column_name in self.metadata.columns:
            normalized_hospital_name = normalize_hospital_name(hospital_name=column_name)
            if normalized_hospital_name in HospitalNames:
                # this column is labelled with a hospital name
                # and indicates whether the associated variables are present in the UC
                nb_hospitals_in_columns = nb_hospitals_in_columns + 1
                # take advantage of this, we also rename the hospital name in the metadata header
                log.info(f"rename {column_name} into {normalized_hospital_name}")
                self.metadata.rename(columns={column_name: normalized_hospital_name}, inplace=True)
        log.debug(f"nb_hospitals_in_columns: {nb_hospitals_in_columns}")
        # log.debug(self.metadata.to_string())
        if nb_hospitals_in_columns > 1:
            # there are more than one hospital described in this metadata
            # a. we filter the unnecessary hospital columns (for UC2 and UC3 there are several hospitals in the same metadata file)
            columns_to_keep = []
            columns_to_keep.extend([normalize_column_name(column_name=meta_variable.value) for meta_variable in MetadataColumns])
            columns_to_keep.append(normalize_hospital_name(self.execution.hospital_name))
            log.debug(f"{self.metadata.columns}")
            log.debug(f"{columns_to_keep}")
            self.metadata = self.metadata[columns_to_keep]
            # b. we remove the column for the hospital, now that we have filtered the rows using it
            log.debug(f"will drop {normalize_hospital_name(self.execution.hospital_name)} in {self.metadata.columns}")
            self.metadata = self.metadata.drop(normalize_hospital_name(self.execution.hospital_name), axis=1)
        else:
            # we have 0 or 1 column specifying the hospital name,
            # so the metadata is only for the current hospital
            # thus, nothing more to do
            pass
        # log.debug(self.metadata.to_string())

        # 3. We keep the metadata of the current dataset
        # TODO Nelly: store the clinical metadata into self_clinical_metadata (a subset of self.metadata); similarly for images and genomic data
        filename = os.path.basename(self.execution.clinical_filepaths[0]).lower()
        log.debug(f"{filename}")
        log.debug(f"{self.metadata.to_string()}")
        log.debug(f"{self.metadata[MetadataColumns.DATASET_NAME.value].unique()}")
        if filename not in self.metadata[MetadataColumns.DATASET_NAME.value.lower()].unique():
            raise ValueError(f"The current dataset ({filename}) is not described in the provided metadata file.")
        else:
            self.metadata = self.metadata[self.metadata[MetadataColumns.DATASET_NAME.value] == filename]
        # log.debug(self.metadata.to_string())

        # normalize ontology system names and codes
        self.metadata[MetadataColumns.FIRST_ONTOLOGY_SYSTEM.value] = self.metadata[MetadataColumns.FIRST_ONTOLOGY_SYSTEM.value].apply(lambda value: normalize_ontology_system(ontology_system=value))
        self.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE.value] = self.metadata[MetadataColumns.FIRST_ONTOLOGY_CODE.value].apply(lambda value: normalize_ontology_code(ontology_code=value))
        self.metadata[MetadataColumns.SEC_ONTOLOGY_SYSTEM.value] = self.metadata[MetadataColumns.SEC_ONTOLOGY_SYSTEM.value].apply(lambda value: normalize_ontology_system(ontology_system=value))
        self.metadata[MetadataColumns.SEC_ONTOLOGY_CODE.value] = self.metadata[MetadataColumns.SEC_ONTOLOGY_CODE.value].apply(lambda value: normalize_ontology_code(ontology_code=value))
        # log.debug(self.metadata.to_string())

        # we also normalize column names described in the metadata, inc. "sex", "dateOfBirth", "Ethnicity", etc
        self.metadata[MetadataColumns.COLUMN_NAME.value] = self.metadata[MetadataColumns.COLUMN_NAME.value].apply(lambda x: normalize_column_name(column_name=x))
        # log.debug(self.metadata.to_string())

        # normalize the var_type
        self.metadata[MetadataColumns.VAR_TYPE.value] = self.metadata[MetadataColumns.VAR_TYPE.value].apply(lambda x: normalize_var_type(var_type=x))
        # log.debug(self.metadata.to_string())

        # normalize the dict of accepted JSON values
        # the non-NaN JSON_values values are of the form: "{...}, {...}, ..."
        # thus, we need to
        # 1. create an empty list
        # 2. add each JSON dict of JSON_values in that list
        # Note: we cannot simply add brackets around the dicts because it would add a string with the dicts in the list
        # we cannot either use json.loads on row["JSON_values"] because it is not parsable (it lacks the brackets)
        for index, row in self.metadata.iterrows():
            if is_not_nan(row[MetadataColumns.JSON_VALUES.value]):
                values_dicts = []
                json_dicts = re.split('}, {', row[MetadataColumns.JSON_VALUES.value])
                for json_dict in json_dicts:
                    if not json_dict.startswith("{"):
                        json_dict = "{" + json_dict
                    if not json_dict.endswith("}"):
                        json_dict = json_dict + "}"
                    the_json_dict = json.loads(json_dict)
                    # normalize the keys and values before happening the json dict to the list
                    normalized_json_dict = {}
                    # we suppose we have three (k, v): one for "value", one for "explanation", and one for ontology_system: ontology_code
                    json_dict_keys = {k.strip().lower(): k for k in the_json_dict.keys()}  # we need to keep the mapping between the original keys and the normalized to be able to query the original dict
                    for a_key in json_dict_keys.keys():
                        if a_key == "value":
                            # for the categorical value, e.g., F and M for sex, we normalize it
                            # so that we can lift inconsistencies between data and metadata
                            normalized_json_dict["value"] = normalize_column_value(the_json_dict[json_dict_keys["value"]])
                        elif a_key == "explanation":
                            # for the (human) textual explanation of the JSON value, we do not normalize it (as this is supposed to be human text)
                            normalized_json_dict["explanation"] = the_json_dict[json_dict_keys["explanation"]]
                        else:
                            # this should be an ontology code, associated to an ontology code
                            # there may be several, e.g., F (female) may have both LOINC and SNOMED_CT codes
                            ontology_system = normalize_ontology_system(ontology_system=a_key)
                            ontology_code = normalize_ontology_code(ontology_code=the_json_dict[json_dict_keys[a_key]])
                            normalized_json_dict[ontology_system] = ontology_code
                    values_dicts.append(normalized_json_dict)
                self.metadata.loc[index, MetadataColumns.JSON_VALUES.value] = json.dumps(values_dicts)  # set the new JSON values as a string (required by pandas)
        # log.debug(self.metadata.to_string())

        # reindex the remaining metadata rows, starting from 0
        # because when dropping rows, rows keep their original indexes
        # to ease tests, we reindex starting from 0
        self.metadata.reset_index(drop=True, inplace=True)
        # log.debug(self.metadata.to_string())

        log.info(f"{len(self.metadata.columns)} columns and {len(self.metadata)} lines in the metadata file.")

    def load_data_file(self) -> None:
        log.info(f"{self.execution.current_filepath}")
        assert os.path.exists(self.execution.current_filepath), "The provided samples file could not be found. Please check the filepath you specify when running this script."

        log.info(f"Data filepath is {self.execution.current_filepath}")

        # index_col is False to not add a column with line numbers
        # we also keep all data as str (we will cast later)
        self.data = read_csv_file_as_string(filepath=self.execution.current_filepath)

        # normalize column names ("sex", "dateOfBirth", "Ethnicity", etc) to match column names described in the metadata
        self.data.rename(columns=lambda x: normalize_column_name(column_name=x), inplace=True)
        # we also normalize the data values
        # they will be cast to the right type (int, float, datetime) in the Transform step
        for column in self.data:
            self.data[column] = self.data[column].apply(lambda x: normalize_column_value(column_value=x))

        log.info(f"{len(self.data.columns)} columns and {len(self.data)} lines in the data file.")

    def remove_unused_columns(self) -> None:
        # removes the data columns that are NOT described in the metadata
        # if a column is described in the metadata but is not present in the data or this column is empty we keep it
        # because people took the time to describe it.
        # for this, we get the union of both sets and remove the columns that are not described in the metadata
        log.debug(self.metadata.to_string())
        data_columns = list(self.data.columns)
        columns_described_in_metadata = list(self.metadata[MetadataColumns.COLUMN_NAME.value])
        variables_to_keep = []
        variables_to_keep.extend(data_columns)
        variables_to_keep.extend(columns_described_in_metadata)
        variables_to_keep = list(set(variables_to_keep))  # we have appended two lists, mostly containing the same columns, thus we have duplicates
        for one_column in variables_to_keep:
            if one_column not in columns_described_in_metadata:
                variables_to_keep.remove(one_column)
        data_columns.sort()
        columns_described_in_metadata.sort()
        variables_to_keep.sort()
        log.debug(f"Columns present in the data: {data_columns}")
        log.debug(f"Columns described in the metadata: {columns_described_in_metadata}")
        log.debug(f"Variables to keep: {variables_to_keep}")

        for column in self.data.columns:
            if column not in variables_to_keep:
                log.info(f"Drop data column corresponding to the variable {column}.")
                self.data = self.data.drop(column, axis=1)  # axis=1 -> columns

        log.debug(self.data.to_string())
        log.debug(self.metadata.to_string())

    def compute_mapped_values(self) -> None:
        self.mapped_values = {}

        for index, row in self.metadata.iterrows():
            if is_not_nan(row[MetadataColumns.JSON_VALUES.value]):
                current_dicts = json.loads(row[MetadataColumns.JSON_VALUES.value])
                parsed_dicts = []
                for current_dict in current_dicts:
                    # if we can convert the JSON value to a float or an int, we do it, otherwise we let it as a string
                    current_dict["value"] = normalize_column_value(column_value=current_dict["value"])
                    # if we can also convert the snomed_ct / loinc code, we do it
                    # TODO Nelly: loop on all ontologies known in OntologyNames
                    if Ontologies.SNOMEDCT.value["name"] in current_dict:
                        current_dict[Ontologies.SNOMEDCT.value["name"]] = normalize_ontology_code(ontology_code=current_dict[Ontologies.SNOMEDCT.value["name"]])
                    if Ontologies.LOINC.value["name"] in current_dict:
                        current_dict[Ontologies.LOINC.value["name"]] = normalize_ontology_code(ontology_code=current_dict[Ontologies.LOINC.value["name"]])
                    parsed_dicts.append(current_dict)
                self.mapped_values[row["name"]] = parsed_dicts
        log.debug(f"{self.mapped_values}")

    def compute_mapped_types(self) -> None:
        self.mapped_types = {}

        for index, row in self.metadata.iterrows():
            if is_not_nan(row[MetadataColumns.VAR_TYPE.value]):
                # we associate the column name to its expected type
                self.mapped_types[row[MetadataColumns.COLUMN_NAME.value]] = row[MetadataColumns.VAR_TYPE.value]
        log.debug(f"{self.mapped_types}")

    def run_value_analysis(self) -> None:
        log.debug(f"{self.mapped_values}")
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
                log.info(f"{column}: {value_analysis}")

    def run_variable_analysis(self) -> None:
        variable_analysis = VariableAnalysis(samples=self.data, metadata=self.metadata)
        variable_analysis.run_analysis()
        log.info(f"{variable_analysis}")
