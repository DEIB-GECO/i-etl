import json
import os
import re

import pandas as pd

from src.analysis.ValueAnalysis import ValueAnalysis
from src.analysis.VariableAnalysis import VariableAnalysis
from src.database.Database import Database
from src.fhirDatatypes.CodeableConcept import CodeableConcept
from src.utils.Ontologies import Ontologies
from src.utils.TableNames import TableNames
from src.utils.setup_logger import log
from src.utils.utils import is_not_nan, convert_value, get_values_from_json_values


class Extract:

    def __init__(self, metadata_filepath: str, samples_filepath: str, database: Database, run_analysis: bool):
        self.metadata_filepath = metadata_filepath
        self.metadata = None
        self.samples_filepath = samples_filepath
        self.samples = None
        self.mapped_values = {}  # accepted values for some categorical columns (column "JSON_values" in metadata)
        self.mapped_types = {}  # expected data type for columns (column "vartype" in metadata)

        self.database = database

        # data that has already been ingested
        # we load it in-memory to avoid many calls to MongoDB for efficiency
        self.existing_local_patient_ids = set()
        self.existing_disease_ccs = set()
        self.existing_examination_ccs = set()
        self.existing_medication_ccs = set()

        # flags
        self.run_analysis = run_analysis

    def run(self):
        self.load_existing_data_in_memory()
        self.load_metadata_file()
        self.load_samples_file()
        self.compute_mapped_values()
        self.compute_mapped_types()

        if self.run_analysis:
            self.run_value_analysis()
            self.run_variable_analysis()

    def load_metadata_file(self):
        assert os.path.exists(self.metadata_filepath), "The provided metadata file could not be found. Please check the filepath you specify when running this script."

        # index_col is False to not add a column with line numbers
        self.metadata = pd.read_csv(self.metadata_filepath, index_col=False)

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
                self.metadata.loc[index, "JSON_values"] = json.dumps(values_dicts) # set the new JSON values as a string

    def load_samples_file(self):
        assert os.path.exists(self.samples_filepath), "The provided samples file could not be found. Please check the filepath you specify when running this script."

        log.debug("self.samples_filepath is %s", self.samples_filepath)

        # index_col is False to not add a column with line numbers
        self.samples = pd.read_csv(self.samples_filepath, index_col=False)
        log.debug(self.samples)

        # lower case all column names to avoid inconsistencies
        self.samples.columns = self.samples.columns.str.lower()
        log.debug(self.samples.columns)

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

    def load_existing_data_in_memory(self):
        # get existing patient IDs
        self.existing_local_patient_ids = set()
        cursor = self.database.find_operation(TableNames.PATIENT.value, {}, {"identifier.value": 1})
        for result in cursor:
            log.debug(result)
            self.existing_local_patient_ids.add(result["identifier"]["value"])
        log.info("%s existing patients", len(self.existing_local_patient_ids))
        log.debug(self.existing_local_patient_ids)

        # get existing CCs for examinations
        self.existing_examination_ccs = set()
        cursor = self.database.find_operation(TableNames.EXAMINATION.value, {}, {"code.coding.system": 1, "code.coding.code": 1})
        for result in cursor:
            log.info(result)
            cc = CodeableConcept()
            for one_coding in result["code"]["coding"]:
                cc.add_coding((one_coding["system"], one_coding["code"], ""))  # TODO Nelly: why is there no display in inserted Codings ? one_coding["display"]
            self.existing_examination_ccs.add(cc)
        log.info("%s existing examinations", len(self.existing_examination_ccs))
        log.debug(self.existing_examination_ccs)

        # TODO Nelly: bring thi back when I wil implement Diseases
        # self.existing_disease_ccs = set()
        # cursor = self.database.find_operation(TableNames.DISEASE.value, {}, {"code.coding.system": 1, "code.coding.code": 1})
        # for result in cursor:
        #     self.existing_disease_ccs.add((result["system"], result["code"]))
        # log.info("%s existing diseases", len(self.existing_disease_ccs))
        # log.debug(self.existing_disease_ccs)

    def run_value_analysis(self):
        log.debug(self.mapped_values)
        # for each column in the sample data (and not in the metadata because some (empty) data columns are not
        # present in the metadata file), we compare the set of values it takes against the accepted set of values
        # (available in the mapped_values variable)
        for column in self.samples.columns:
            values = pd.Series(self.samples[column].values)
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
        variable_analysis = VariableAnalysis(samples=self.samples, metadata=self.metadata)
        variable_analysis.run_analysis()
        log.info(variable_analysis)
