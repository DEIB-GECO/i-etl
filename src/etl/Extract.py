import json
import os
import re

import numpy as np
import pandas as pd

from analysis.ValueAnalysis import ValueAnalysis
from analysis.VariableAnalysis import VariableAnalysis
from database.Database import Database
from database.Execution import Execution
from datatypes.CodeableConcept import CodeableConcept
from datatypes.Coding import Coding
from enums.DiagnosisClassificationColumns import DiagnosisClassificationColumns
from enums.DiagnosisRegexColumns import DiagnosisRegexColumns
from datatypes.OntologyCode import OntologyCode
from enums.DiagnosisClassificationColumns import DiagnosisClassificationColumns
from enums.DiagnosisRegexColumns import DiagnosisRegexColumns
from enums.FileTypes import FileTypes
from enums.HospitalNames import HospitalNames
from enums.MetadataColumns import MetadataColumns
from enums.Ontologies import Ontologies
from enums.TableNames import TableNames
from etl.Task import Task
from utils.constants import ID_COLUMNS, PATTERN_VALUE_DIMENSION
from utils.setup_logger import log
from utils.utils import is_not_nan, get_values_from_json_values, normalize_column_name, \
    normalize_ontology_name, normalize_column_value, normalize_hospital_name, \
    normalize_type, read_tabular_file_as_string


class Extract(Task):

    def __init__(self, database: Database, execution: Execution):
        super().__init__(database, execution)
        self.metadata = None
        self.data = None
        self.patient_ids_mapping = None
        self.mapping_categorical_value_to_cc = {}  # accepted values for categorical columns (column "JSON_values" in metadata) and their CodeableConcept
        self.mapping_column_to_categorical_value = {}  # each column name with its normalized accepted values (str, not cc)
        self.mapping_column_to_vartype = {}  # each column is associated to its var type (column "vartype" in metadata)
        self.mapping_column_to_dimension = {}  # each column is associated to its dimension, the union set of the dimension given in the metadata and the units found in the string values themselves
        self.mapping_disease_to_classification = {}  # each (data) disease name is associated to a standard disease name and a classification healthy/disease
        self.mapping_disease_to_cc = {}  # each standard disease name is associated to an OrphaNet code (and possibly omim)

    def run(self) -> None:
        # load and pre-process metadata
        self.load_metadata_file()
        self.compute_mapping_categorical_value_to_cc()
        log.debug(self.metadata.columns)

        # load and pre-process data (all kinds)
        if self.execution.current_file_type in (FileTypes.LABORATORY, FileTypes.DIAGNOSIS, FileTypes.MEDICINE):
            # laboratory, diagnosis, medicine data
            self.load_csv_data_file()
            self.remove_unused_csv_columns()
            log.debug(self.metadata.columns)
        elif self.execution.current_file_type == FileTypes.IMAGING:
            self.load_imaging_data_file()
        elif self.execution.current_file_type == FileTypes.GENOMIC:
            self.load_genomic_data_file()
        else:
            raise TypeError(f"The type of the current file is unknown. It should be laboratory, diagnosis, medicine, imaging or genomic. It is of type: {self.execution.current_file_type}")

        # The remaining task of 1. (loading metadata) has to be done after the data is loaded because
        # the dimensions are either extracted from the metadata (if described) or from the data
        # we do this only for lab data, otherwise we end up with units extracted from any string containing both digits and chars
        if self.execution.current_file_type == FileTypes.LABORATORY:
            self.compute_column_to_dimension()

        # if provided as input, load the mapping between patient IDs and anonymized IDs
        self.load_patient_id_mapping()

        # if provided as input, load disease classification and codes
        self.compute_mapping_disease_to_classification()
        self.compute_mapping_disease_to_cc()

        # if asked, run some data analysis on the loaded data
        if self.execution.is_analyze:
            self.run_value_analysis()
            self.run_variable_analysis()

    def load_metadata_file(self) -> None:
        log.info(f"Metadata filepath is {self.execution.metadata_filepath}")

        # index_col is False to not add a column with line numbers
        self.metadata = read_tabular_file_as_string(self.execution.metadata_filepath)  # keep all metadata as str
        log.debug(self.metadata.dtypes)

        log.info("Will preprocess metadata")

        # 1. normalize the header, e.g., "Significato it" becomes "significato_it"
        # this also normalizes hospital names if they are in the header (UC 2 and UC 3)
        self.metadata.rename(columns=lambda x: normalize_column_name(column_name=x), inplace=True)

        # 2. Get the metadata associated to the current hospital (but any dataset within that hospital)
        log.debug(f"working on hospital {self.execution.hospital_name}")
        # a. we remove columns indicating whether the column is present in the hospital if this is not the current one
        # for this we check whether there are more than 1 hospital name (> 1) in column names
        nb_hospitals_in_columns = 0
        for column_name in self.metadata.columns:
            normalized_hospital_name = normalize_hospital_name(hospital_name=column_name)
            if normalized_hospital_name in HospitalNames.values():
                # this column is labelled with a hospital name
                # and indicates whether the associated variables are present in the UC
                nb_hospitals_in_columns = nb_hospitals_in_columns + 1
                # take advantage of this, we also rename the hospital name in the metadata header
                log.info(f"rename {column_name} into {normalized_hospital_name}")
                self.metadata.rename(columns={column_name: normalized_hospital_name}, inplace=True)
        log.debug(f"nb_hospitals_in_columns: {nb_hospitals_in_columns}")
        if nb_hospitals_in_columns > 1:
            # there are more than one hospital described in this metadata
            # a. we filter the unnecessary hospital columns (for UC2 and UC3 there are several hospitals in the same metadata file)
            columns_to_keep = []
            columns_to_keep.extend([normalize_column_name(column_name=meta_variable) for meta_variable in MetadataColumns.values()])
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
            # thus, nothing to do
            pass

        # 3. We keep the metadata of the current dataset
        filename = os.path.basename(self.execution.current_filepath)
        log.debug(f"current file path is: {self.execution.current_filepath}; thus filename is: {filename}")
        log.debug(f"List of datasets described in the metadata is: {self.metadata[MetadataColumns.DATASET_NAME].unique()}")
        if filename not in self.metadata[MetadataColumns.DATASET_NAME].unique():
            raise ValueError(f"The current dataset ({filename}) is not described in the provided metadata file.")
        else:
            self.metadata = self.metadata[self.metadata[MetadataColumns.DATASET_NAME] == filename]

        # normalize ontology names (but not codes because they will be normalized within OntologyCode)
        self.metadata[MetadataColumns.FIRST_ONTOLOGY_NAME] = self.metadata[MetadataColumns.FIRST_ONTOLOGY_NAME].apply(lambda value: normalize_ontology_name(ontology_system=value))
        self.metadata[MetadataColumns.SEC_ONTOLOGY_NAME] = self.metadata[MetadataColumns.SEC_ONTOLOGY_NAME].apply(lambda value: normalize_ontology_name(ontology_system=value))

        # we also normalize column names described in the metadata, inc. "sex", "dateOfBirth", "Ethnicity", etc
        self.metadata[MetadataColumns.COLUMN_NAME] = self.metadata[MetadataColumns.COLUMN_NAME].apply(lambda x: normalize_column_name(column_name=x))

        # normalize the var_type and the ETL type
        self.metadata[MetadataColumns.VAR_TYPE] = self.metadata[MetadataColumns.VAR_TYPE].apply(lambda x: normalize_type(data_type=x))
        self.metadata[MetadataColumns.ETL_TYPE] = self.metadata[MetadataColumns.ETL_TYPE].apply(lambda x: normalize_type(data_type=x))

        # reindex the remaining metadata rows, starting from 0
        # because when dropping rows, rows keep their original indexes
        # to ease tests, we reindex starting from 0
        self.metadata.reset_index(drop=True, inplace=True)

        log.info(f"{len(self.metadata.columns)} columns and {len(self.metadata)} lines in the metadata file.")

    def load_csv_data_file(self) -> None:
        log.info(f"{self.execution.current_filepath}")
        assert os.path.exists(self.execution.current_filepath), "The provided samples file could not be found. Please check the filepath you specify when running this script."

        log.info(f"Data filepath is {self.execution.current_filepath}")

        # index_col is False to not add a column with line numbers
        # we also keep all data as str (we will cast later)
        self.data = read_tabular_file_as_string(filepath=self.execution.current_filepath)

        # normalize column names ("sex", "dateOfBirth", "Ethnicity", etc) to match column names described in the metadata
        self.data.rename(columns=lambda x: normalize_column_name(column_name=x), inplace=True)
        # we also normalize the data values
        # they will be cast to the right type (int, float, datetime) in the Transform step
        # issue 113: we do not normalize identifiers assigned by hospitals to avoid discrepancies
        columns_no_normalization = []
        columns_no_normalization.append(ID_COLUMNS[self.execution.hospital_name][TableNames.PATIENT])
        if TableNames.SAMPLE in ID_COLUMNS[self.execution.hospital_name]:
            columns_no_normalization.append(ID_COLUMNS[self.execution.hospital_name][TableNames.SAMPLE])

        for column in self.data:
            if column not in columns_no_normalization:
                self.data[column] = self.data[column].apply(lambda x: normalize_column_value(column_value=x))

        log.info(f"{len(self.data.columns)} columns and {len(self.data)} lines in the data file.")

    def load_imaging_data_file(self):
        # TODO Nelly: implement this
        pass

    def load_genomic_data_file(self):
        # TODO Nelly: implement this
        pass

    def load_patient_id_mapping(self) -> None:
        log.info(f"Patient ID mapping filepath is {self.execution.anonymized_patient_ids_filepath}")

        # index_col is False to not add a column with line numbers
        self.patient_ids_mapping = {}
        log.debug(self.execution.anonymized_patient_ids_filepath)
        if self.execution.anonymized_patient_ids_filepath is not None:
            with open(self.execution.anonymized_patient_ids_filepath, "r") as f:
                self.patient_ids_mapping = json.load(f)
        log.info(f"{len(self.patient_ids_mapping)} patient IDs in the mapping file.")

    def remove_unused_csv_columns(self) -> None:
        # removes the data columns that are NOT described in the metadata or that are explicitly marked as not be loaded
        # if a column is described in the metadata but is not present in the data or this column is empty we keep it
        # because people took the time to describe it.
        # for this, we get the union of both sets and remove the columns that are not described in the metadata
        data_columns = list(self.data.columns)
        columns_described_in_metadata = list(self.metadata[MetadataColumns.COLUMN_NAME])
        columns_described_in_metadata = [col_name if is_not_nan(col_name) else "" for col_name in columns_described_in_metadata]  # it may happen that some columns have no name, thus they appear as nan in the list; thus, we label them '' (empty string)
        variables_to_keep = []
        variables_to_keep.extend(data_columns)
        variables_to_keep.extend(columns_described_in_metadata)
        variables_to_keep = list(set(variables_to_keep))  # we have appended two lists, mostly containing the same columns, thus we have duplicates
        for one_column in variables_to_keep:
            if one_column not in columns_described_in_metadata or one_column in self.execution.columns_to_remove:
                variables_to_keep.remove(one_column)
        log.debug(f"Columns present in the data: {data_columns}")
        log.debug(f"Columns described in the metadata: {columns_described_in_metadata}")
        log.debug(f"Variables to keep: {variables_to_keep}")
        data_columns.sort()
        columns_described_in_metadata.sort()
        variables_to_keep.sort()
        log.debug(f"Columns present in the data: {data_columns}")
        log.debug(f"Columns described in the metadata: {columns_described_in_metadata}")
        log.debug(f"Columns to be explicitly removed: {self.execution.columns_to_remove}")
        log.debug(f"Variables to keep: {variables_to_keep}")

        for column in self.data.columns:
            if column not in variables_to_keep:
                log.info(f"Drop data column corresponding to the variable {column}.")
                self.data = self.data.drop(column, axis=1)  # axis=1 -> columns

    def compute_mapping_categorical_value_to_cc(self) -> None:
        self.mapping_categorical_value_to_cc = {}
        self.mapping_column_to_categorical_value = {}
        # 1. first, we retrieve the existing categorical values already transformed as CodeableConcept
        # this will avoid to send again API queries to re-build already-built CodeableConcept
        # we get all the categorical values across all tables to not miss any of them
        existing_categorical_codeable_concepts = {}
        for table_name in TableNames.values():
            # the set of categorical values are defined in Features only, thus we can restrict the find to only those:
            if table_name.endswith("Feature"):
                # categorical_values_for_table_name = {'_id': ObjectId('66b9b890583ee775ef4edcb9'), 'categorical_values': [{...}, {...}, ...]}
                categorical_values_for_table_name = self.database.find_operation(table_name=table_name, filter_dict={"categorical_values": {"$exists": 1}}, projection={"categorical_values": 1})
                for one_tuple in categorical_values_for_table_name:
                    # existing_categorical_value_for_table_name = [{...}, {...}, ...]}
                    existing_categorical_values_for_table_name = one_tuple["categorical_values"]
                    for encoded_categorical_value in existing_categorical_values_for_table_name:
                        existing_cc = CodeableConcept.from_json(encoded_categorical_value)
                        existing_categorical_codeable_concepts[existing_cc.text] = existing_cc
        log.debug(existing_categorical_codeable_concepts)

        # 2. then, we associate each column to its set of categorical values
        # if we already compute the cc of that value (e.g., several column have categorical values Yes/No/NA),
        # we do not recompute it and take it from the mapping
        for index, row in self.metadata.iterrows():
            if is_not_nan(row[MetadataColumns.JSON_VALUES]):
                column_name = normalize_column_name(row[MetadataColumns.COLUMN_NAME])
                # we get the possible categorical values for the column, e.g., F, or M, or NA for sex
                json_categorical_values = json.loads(row[MetadataColumns.JSON_VALUES])
                # log.info(f"For column {column_name}, JSON values are: {json_categorical_values}")
                self.mapping_column_to_categorical_value[column_name] = []
                for json_categorical_value in json_categorical_values:
                    normalized_categorical_value = normalize_column_value(json_categorical_value["value"])
                    # log.info(f"For column {column_name}, processing value: {normalized_categorical_value}")
                    if normalized_categorical_value not in self.mapping_categorical_value_to_cc:
                        # the categorical value does not exist yet in the mapping, thus:
                        # - it may be retrieved from the db and be added to the mapping
                        # - or, it may be computed for the first time
                        if normalized_categorical_value not in existing_categorical_codeable_concepts.keys():
                            # this is a categorical value that we have never seen (not even in previous executions),
                            # we need to create a CodeableConcept for it from scratch
                            # json_categorical_value is of the form: {'value': 'X', 'snomed_ct': '123', 'loinc': '456' }
                            # and we add each ontology code to that CodeableConcept
                            cc = CodeableConcept(original_name=normalized_categorical_value)
                            # TODO Nelly: do not compute cc for boolean (categorical) columns
                            for key, val in json_categorical_value.items():
                                # for any key value pair that is not about the value or the explanation
                                # (i.e., loinc and snomed_ct columns), we create a Coding, which we add to the CodeableConcept
                                # we need to do a loop because there may be several ontology terms for a single mapping
                                if key != "value" and key != "explanation":
                                    ontology = Ontologies.get_enum_from_name(ontology_name=normalize_ontology_name(key))
                                    cc.add_coding(one_coding=Coding(ontology=ontology, code=OntologyCode(full_code=val), display=None))
                            # {
                            #   'm': {"coding": [{"system": "snomed", "code": "248153007", "display": "m (Male)"}], "text": ""},
                            #   'f': {"coding": [{"system": "snomed", "code": "248152002", "display": "f (Female)"}], "text": ""},
                            #   'inadeguata': {"coding": [{"system": "snomed", "code": "71978007", "display": "inadeguata (inadequate)"}], "text": ""},
                            #   ...
                            # }
                            self.mapping_categorical_value_to_cc[normalized_categorical_value] = cc
                            self.mapping_column_to_categorical_value[column_name].append(normalized_categorical_value)
                        else:
                            # a CodeableConcept already exists for this value (it has been retrieved from the db),
                            # we simply add it to the mapping
                            log.debug(f"The categorical value {normalized_categorical_value} already exists in the database as a CC. Taking it from here.")
                            self.mapping_categorical_value_to_cc[normalized_categorical_value] = existing_categorical_codeable_concepts[normalized_categorical_value]
                    else:
                        # this categorical value is already present in the mapping self.mapping_categorical_value_to_cc
                        # (it has either been retrieved from the db or computed for the first time),
                        # so no need to add it again
                        # log.debug(f"{normalized_categorical_value} is already in the mapping: {self.mapping_categorical_value_to_cc}")

                        # however, we still need to add it as a categorical value for the current column
                        if normalized_categorical_value not in self.mapping_column_to_categorical_value[column_name]:
                            self.mapping_column_to_categorical_value[column_name].append(normalized_categorical_value)
            else:
                # no JSON values for this column, pass
                pass
        # log.debug(f"{self.mapping_categorical_value_to_cc}")
        # log.debug(f"{self.mapping_column_to_categorical_value}")

    def compute_column_to_dimension(self) -> None:
        self.mapping_column_to_dimension = {}

        for index, row in self.metadata.iterrows():
            column_name = row[MetadataColumns.COLUMN_NAME]
            add_dimension_from_metadata = False
            if column_name in self.data.columns:  # some columns are in the metadata and not in the data (but are kept), thus we need to check
                # we compute the set of units used in the data for that column and keep the most frequent
                # if there is no unit for those value, we use the provided dimension if it exists, otherwise None
                values_in_columns = self.data[column_name]
                units = {}
                for value in values_in_columns:
                    if is_not_nan(value):
                        m = re.search(PATTERN_VALUE_DIMENSION, value)
                        if m is not None:
                            # m.group(0) is the text itself, m.group(1) is the int/float value, m.group(2) is the dimension
                            unit = m.group(2)
                            if unit not in units:
                                units[unit] = 0
                            units[unit] += 1
                        else:
                            # this value does not contain a dimension or is not of the form "value dimension"
                            pass

                # we only keep the most frequent unit; values that do not have this unit will not be cast to numeric
                if len(units.keys()) > 0:
                    # if several units reach the highest frequency, we sorted them in alphabetical order and take the first
                    max_frequency = max(units.values())
                    units_with_max_frequency = [unit for unit, frequency in units.items() if frequency == max_frequency]
                    units_with_max_frequency.sort()
                    most_frequent_unit = units_with_max_frequency[0]
                    self.mapping_column_to_dimension[column_name] = most_frequent_unit
                else:
                    # no unit found in those values
                    add_dimension_from_metadata = True
            else:
                # this column is described in the metadata but not in the data
                add_dimension_from_metadata = True

            if add_dimension_from_metadata:
                # 1. if there is a dimension provided by the description, we use it
                # 2. otherwise, we set it to None
                column_expected_dimension = row[MetadataColumns.VAR_DIMENSION]
                if is_not_nan(column_expected_dimension):
                    # if there is a dimension provided in the metadata (this may be overridden if there are dimensions in the cells values)
                    self.mapping_column_to_dimension[column_name] = column_expected_dimension
                else:
                    self.mapping_column_to_dimension[column_name] = None
        # log.debug(self.mapping_column_to_dimension)

    def compute_mapping_disease_to_classification(self) -> None:
        self.mapping_disease_to_classification = {}

        if self.execution.diagnosis_classification_filepath is not None:
            diagnosis_csv = read_tabular_file_as_string(self.execution.diagnosis_classification_filepath)
            # normalize the headers, e.g., "Classification" -> "classification"
            diagnosis_csv.rename(columns=lambda x: normalize_column_name(column_name=x), inplace=True)
            for index, line in diagnosis_csv.iterrows():
                diagnosis_free_text = normalize_column_value(line[DiagnosisClassificationColumns.DIAGNOSIS_FREE_TEXT])
                self.mapping_disease_to_classification[diagnosis_free_text] = {}
                standard_name = line[DiagnosisClassificationColumns.DIAGNOSIS_NAME]
                if is_not_nan(standard_name):
                    # a standard name exists in the classification
                    self.mapping_disease_to_classification[diagnosis_free_text]["standard_name"] = standard_name
                else:
                    # no standard name is associated to the free text
                    # we need to explicitly specify None, otherwise we would have "nan" (which is not recognised in JSON documents)
                    pass
                classification = normalize_column_value(line[DiagnosisClassificationColumns.DIAGNOSIS_CLASSIFICATION])
                if is_not_nan(classification) and (classification == normalize_column_value("healthy") or classification == normalize_column_value("disease")):
                    # this disease is classified as Healthy/Disease
                    self.mapping_disease_to_classification[diagnosis_free_text]["classification"] = classification
                else:
                    # no classification (Healthy/Disease) is provided for this disease.
                    pass
        # log.debug(self.mapping_disease_to_classification)

    def compute_mapping_disease_to_cc(self) -> None:
        self.mapping_disease_to_cc = {}

        if self.execution.diagnosis_regexes_filepath is not None:
            diagnosis_csv = read_tabular_file_as_string(self.execution.diagnosis_regexes_filepath)
            # normalize the headers, e.g., "Classification" -> "classification"
            diagnosis_csv.rename(columns=lambda x: normalize_column_name(column_name=x), inplace=True)
            for index, line in diagnosis_csv.iterrows():
                diagnosis_standard_name = line[DiagnosisRegexColumns.DIAGNOSIS_NAME]
                cc = CodeableConcept(original_name=diagnosis_standard_name)
                cc.add_coding(one_coding=Coding(ontology=Ontologies.ORPHANET, code=OntologyCode(full_code=line[DiagnosisRegexColumns.ORPHANET_CODE]), display=None))
                self.mapping_disease_to_cc[diagnosis_standard_name] = cc.to_json()  # do not forget to add the JSONified CC, not the CC itlelf
        # log.debug(self.mapping_disease_to_cc)

    def run_value_analysis(self) -> None:
        log.debug(f"{self.mapping_categorical_value_to_cc}")
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
            # TODO NELLY: complete this part
            expected_type = ""
            # trying to get expected values for the current column
            if column in self.mapping_categorical_value_to_cc:
                # self.mapped_values[column] contains the mappings (JSON dicts) for the given column
                # we need to get only the set of values described in the mappings of the given column
                accepted_values = get_values_from_json_values(json_values=self.mapping_categorical_value_to_cc[column])
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
