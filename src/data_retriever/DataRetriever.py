import dataclasses
import json

import pandas as pd
from pandas import DataFrame
from pymongo import MongoClient

from database.Operators import Operators
from entities.OntologyResource import OntologyResource
from enums.Ontologies import Ontologies
from enums.QueryTypes import QueryTypes
from enums.TableNames import TableNames
from utils.setup_logger import log


@dataclasses.dataclass()
class DataRetriever(object):
    """Legacy DataRetriever for DATA and METADATA query types"""
    # db connection
    mongodb_url: str  # "mongodb://localhost:27018/"
    db_name: str
    query_type: QueryTypes  # METADATA or DATA

    # user input to build the query to retrieve feature data (METADATA mode)
    feature_list: list = dataclasses.field(default_factory=list)  # user input of the form ["snomedct:422549004", "loinc:1234", ...]
    # user input to build the query to retrieve record data (DATA mode)
    # FEATURE_CODES = {"hypotonia": "8116006:278201002=(\"HPO\",288467006)", "vcf_path": "12953007"}
    feature_selected: dict = dataclasses.field(default_factory=dict)  # user input of the form {"user variable name": "ontology code"}
    # FEATURES_VALUE_PROCESS = {"hypotonia": {"$addFields": { "hypotonia": { "$in": ["hp:0001252", "$hypotonia"] }}}, "vcf_path": None}
    feature_value_process: dict = dataclasses.field(default_factory=dict)  # {"field_name": {mongodb operation} or None}
    feature_filters: dict = dataclasses.field(default_factory=dict)  # {"field_name": {"code": "ontology code", "filter": {mongodb operator}}}
    features_info: dict = dataclasses.field(init=False)  # generated from feature inputs
    the_dataframe: DataFrame = dataclasses.field(init=False)

    def __post_init__(self):
        self.client = MongoClient(host=self.mongodb_url, serverSelectionTimeoutMS=30, w=0, directConnection=True)
        log.info(self.client)
        self.db = self.client[self.db_name]
        log.info(self.db)
        self.the_query = ""

        # check that the query type match the given parameters
        if self.query_type == QueryTypes.METADATA:
            if self.feature_list is None or len(self.feature_list) == 0:
                raise Exception("You specified that you want to retrieve metadata from the database but did not specify which features to retrieve.")
            else:
                new_feature_list = []
                for code in self.feature_list:
                    code_split = code.split(":", 1)  # split on the first :
                    system, code = Ontologies.get_enum_from_name(Ontologies.normalize_name(code_split[0])), Ontologies.normalize_code(code_split[1])
                    new_feature_list.append(OntologyResource(system=system, code=code, label=" ", quality_stats=None))
                self.feature_list = new_feature_list
        elif self.query_type == QueryTypes.DATA:
            if self.feature_selected is None or len(self.feature_selected) == 0:
                raise Exception("You specified that you want to retrieve data from the database but did not specify which features to retrieve.")
            else:
                # normalize ontology resource codes
                # we have to set the system to no ontology, otherwise no ontology resource will be created
                # we have to set the label to the non-empty string, otherwise a label wil be computed
                # OntologyResource(system=Ontologies.NONE, code=code, label=" ", quality_stats=None).code
                self.features_info = {}
                i = 1
                for key, code in self.feature_selected.items():
                    code_split = code.split(":", 1)  # split on the first :
                    system, code = Ontologies.get_enum_from_name(Ontologies.normalize_name(code_split[0])), Ontologies.normalize_code(code_split[1])
                    self.features_info[key] = {
                        "the_or": OntologyResource(system=system, code=code, label=" ", quality_stats=None),
                        "filter": None,
                        "process": None,
                        "show": True,
                        "i": i
                    }
                    i = i + 1
                log.info(self.features_info)
                # for each feature specified in the filter, add it to the infos
                for feature_name, feature in self.feature_filters.items():
                    if feature_name not in self.features_info:
                        self.features_info[feature_name] = {}
                        code_split = self.feature_filters[feature_name]["code"].split(":", 1)  # split on the first :
                        system, code = Ontologies.get_enum_from_name(Ontologies.normalize_name(code_split[0])), Ontologies.normalize_code(code_split[1])
                        self.features_info[feature_name]["the_or"] = OntologyResource(system=system, code=code, label=" ", quality_stats=None)
                        self.features_info[feature_name]["i"] = i
                        i = i + 1
                        self.features_info[feature_name]["show"] = False
                    if feature_name not in self.feature_filters:
                        pass
                    else:
                        if self.feature_filters[feature_name] is not None and "filter" in self.feature_filters[feature_name]:
                            self.features_info[feature_name]["filter"] = self.feature_filters[feature_name]["filter"]
                        else:
                            self.features_info[feature_name]["filter"] = None
                    self.features_info[feature_name]["process"] = None
                # for each feature specified in the post-process, add it to the info
                for feature_name, process in self.feature_value_process.items():
                    if feature_name not in self.features_info:
                        # we do not have a code for this feature
                        pass
                    else:
                        self.features_info[feature_name]["process"] = process
                log.info(self.features_info)

    def run(self):
        if self.query_type == QueryTypes.DATA:
            self.retrieve_records()
        elif self.query_type == QueryTypes.METADATA:
            self.retrieve_features()

    def retrieve_features(self):
        log.info("***************")
        log.info("Creating indexes")
        self.db[TableNames.FEATURE].create_index("ontology_resource.system")
        self.db[TableNames.FEATURE].create_index("ontology_resource.code")

        log.info("Generating the query")
        self.generate_metadata_query()
        log.info(self.the_query)
        log.info("Creating the dataframe")
        self.the_dataframe = pd.DataFrame(self.db[TableNames.FEATURE].aggregate(json.loads(self.the_query))).set_index("identifier")
        log.info("Done.")

    def generate_metadata_query(self):
        # {"$or": [{"$and": [{"system": "a"}, {"code": 1}]}, {"$and": [{"system": "b", "code": 2}]}]}
        match_feature_codes = Operators.or_operator(list_of_conditions=[
            Operators.and_operator(list_of_conditions=[{"ontology_resource.system": ontology_resource.system, "ontology_resource.code": ontology_resource.code}]) for ontology_resource in self.feature_list
        ])

        self.the_query += "["
        self.the_query += json.dumps(Operators.match(field=None, value=match_feature_codes, is_regex=False))
        self.the_query += ","
        self.the_query += json.dumps(Operators.set_variables([{"name": "onto_system", "operation": "$ontology_resource.system"}, {"name": "onto_code", "operation": "$ontology_resource.code"}]))
        self.the_query += ","
        self.the_query += json.dumps(Operators.unset_variables(["_id"]))
        self.the_query += "]"

    def retrieve_records(self):
        feature_codes_inv = {v["the_or"].to_string(): k for k, v in self.features_info.items()}
        i = 1
        # get features for features asked in the SELECT
        match_feature_codes = Operators.or_operator(list_of_conditions=[
            Operators.and_operator(list_of_conditions=[{"ontology_resource.system": feature["the_or"].system,
                                                        "ontology_resource.code": feature["the_or"].code}]) for
            feature_name, feature in self.features_info.items() #if feature["show"] is True
        ])
        log.info(match_feature_codes)

        for res in self.db[TableNames.FEATURE].find(match_feature_codes):
            log.info(res)
            log.info(res['ontology_resource']['system'])
            log.info(res['ontology_resource']['code'])
            onto_system = res["ontology_resource"]["system"]
            onto_code = res["ontology_resource"]["code"]
            total = str(onto_system) + ":" + str(onto_code)
            feature_user_name = feature_codes_inv[total]
            self.features_info[feature_user_name]["identifier"] = res["identifier"]
            i = i + 1
        log.info(self.features_info)

        log.info("***************")
        log.info("Creating indexes")
        self.db[TableNames.RECORD].create_index("instantiates")
        self.db[TableNames.FEATURE].create_index("ontology_resource.code")

        log.info("Generating the query")
        self.generate_data_query()
        log.info(self.the_query)
        log.info("Creating the dataframe")
        self.the_dataframe = pd.DataFrame(self.db[TableNames.RECORD].aggregate(json.loads(self.the_query))).set_index(
            "has_subject")
        log.info("Done.")

    def generate_data_query(self):
        # 0. compute the lookups
        self.rec_internal(position=1)
        # we now need to add the latest stages to set final variables from the lookups
        # this cannot be done during the recursion, so we do it now
        self.the_query = self.the_query[0:len(self.the_query) - 1]  # remove the last ] before adding final stages
        # log.info(self.the_query)
        self.the_query += ","

        # 1. set variables of the unwind to place them on the first level of the computed record
        set_variables = []
        for feature_name, feature in self.features_info.items():
            lookup_names = "$"
            for j in range(1, feature["i"]):
                lookup_names += f"lookup_{j}."
            lookup_names += "value"
            set_variables.append({"name": feature_name, "operation": lookup_names})
        self.the_query += json.dumps(Operators.set_variables(variables=set_variables))
        self.the_query += ","

        # 2. filter the obtained records
        for feature in self.features_info.values():
            if "filter" in feature and feature["filter"] is not None:
                for select_key, select_operator in feature["filter"].items():
                    self.the_query += json.dumps(Operators.match(field=select_key, value=select_operator, is_regex=False)) + ","

        # 3. set the value to keep + those to flatten
        for feature_name, feature in self.features_info.items():
            if "process" in feature and feature["process"] is not None:
                if feature["process"] == "get_label":
                    # we are in a category, we get its label
                    self.the_query += json.dumps(Operators.set_variables(variables=[{"name": feature_name, "operation": f"${feature_name}.label"}]))
                    self.the_query += ","
                else:
                    # the user gave a personalized post-process, we apply it as is
                    self.the_query += json.dumps(feature["process"])
                    self.the_query += ","
            log.info(self.the_query)

        # 4. project only variables specified in the SELECT (not those only in the WHERE)
        projected_values = {feature_name: 1 for feature_name, feature in self.features_info.items() if feature["show"] is True}
        projected_values["_id"] = 0
        projected_values["has_subject"] = 1
        self.the_query += json.dumps(Operators.project(field=None, projected_value=projected_values))
        self.the_query += "]"
        log.info(self.the_query)

    def rec_internal(self, position):
        log.info(f"rec {position} with {self.features_info}")
        feature_id = [feature["identifier"] for feature_name, feature in self.features_info.items() if feature["i"] == position][0]
        if position == len(self.features_info):
            self.the_query += "["
            # log.info(self.the_query)
            self.the_query += json.dumps(Operators.match(field="instantiates", value=feature_id, is_regex=False))
            # log.info(self.the_query)
            if position > 1:
                self.the_query += ","
                # log.info(self.the_query)
                # if position > 1 and position < max_position:
                self.the_query += json.dumps(
                    Operators.match(field=None, value={"$expr": {"$eq": [f"$$id_{position - 1}", "$has_subject"]}},
                                    is_regex=False))
                # log.info(self.the_query)
            self.the_query += "]"
            # log.info(self.the_query)
        else:
            self.the_query += "["
            # log.info(self.the_query)
            self.the_query += json.dumps(Operators.match(field="instantiates", value=feature_id, is_regex=False))
            # log.info(self.the_query)
            self.the_query += ","
            # log.info(self.the_query)
            if position > 1:
                self.the_query += json.dumps(
                    Operators.match(field=None, value={"$expr": {"$eq": [f"$$id_{position - 1}", "$has_subject"]}},
                                    is_regex=False))
                # log.info(self.the_query)
                self.the_query += ","
                # log.info(self.the_query)

            # the lookup needs to be only partially written in order to compute its internal pipeline
            # this is why we need to write manually the string in order to compute the internal pipeline in-between
            self.the_query += "{\"$lookup\": {\"from\": \"" + TableNames.RECORD + "\", \"let\": {\"id_" + str(
                position) + "\": \"$has_subject\"}, \"as\": \"lookup_" + str(position) + "\", \"pipeline\": "
            # log.info(self.the_query)
            self.rec_internal(position + 1)
            # log.info(self.the_query)
            self.the_query += "}}"
            # log.info(self.the_query)
            self.the_query += ","
            # log.info(self.the_query)
            self.the_query += json.dumps(Operators.unwind(field=f"lookup_{position}"))
            # log.info(self.the_query)
            self.the_query += "]"
            # log.info(self.the_query)


@dataclasses.dataclass()
class DataRetrieverNew(object):
    """WIP DataRetriever for METADATA_NEW and DATA_NEW query types"""
    # db connection
    mongodb_url: str  # "mongodb://localhost:27018/"
    db_name: str
    query_type: QueryTypes  # METADATA_NEW or DATA_NEW

    # user input to build the query to retrieve all metadata fields
    feature_list: list = dataclasses.field(default_factory=list)  # not used in DataRetrieverNew, kept for compatibility
    # user input to build the query to retrieve record data (DATA_NEW mode)
    ontology_list: list = dataclasses.field(default_factory=list)  # user input of the form ["snomedct:77477000", "loinc:1234"]
    feature_value_process: dict = dataclasses.field(default_factory=dict)  # not used in DataRetrieverNew, kept for compatibility
    feature_filters: dict = dataclasses.field(default_factory=dict)  # not used in DataRetrieverNew, kept for compatibility
    feature_selected: dict = dataclasses.field(default_factory=dict)  # not used in DataRetrieverNew, kept for compatibility
    features_info: dict = dataclasses.field(init=False)  # generated from feature inputs
    the_dataframe: DataFrame = dataclasses.field(init=False)

    def __post_init__(self):
        self.client = MongoClient(host=self.mongodb_url, serverSelectionTimeoutMS=30, w=0, directConnection=True)
        log.info(self.client)
        self.db = self.client[self.db_name]
        log.info(self.db)
        self.the_query = ""

        # check that the query type match the given parameters
        if self.query_type == QueryTypes.METADATA_NEW:
            # no validation needed, retrieves all fields
            pass
        elif self.query_type == QueryTypes.DATA_NEW:
            if self.ontology_list is None or len(self.ontology_list) == 0:
                raise Exception("You specified that you want to retrieve data from the database but did not specify which ontology codes to retrieve.")
            else:
                # normalize ontology resource codes
                self.features_info = {}
                for code in self.ontology_list:
                    code_split = code.split(":", 1)  # split on the first :
                    system, code_normalized = Ontologies.get_enum_from_name(Ontologies.normalize_name(code_split[0])), Ontologies.normalize_code(code_split[1])
                    self.features_info[code] = {
                        "the_or": OntologyResource(system=system, code=code_normalized, label=" ", quality_stats=None),
                        "matched_features": []  # will be populated during retrieve_records_new
                    }
                log.info(self.features_info)

    def run(self):
        if self.query_type == QueryTypes.METADATA_NEW:
            self.retrieve_all_fields()
        elif self.query_type == QueryTypes.DATA_NEW:
            self.retrieve_records_new()

    def retrieve_all_fields(self):
        log.info("***************")
        log.info("Creating indexes")
        self.db[TableNames.FEATURE].create_index("ontology_resource.system")
        self.db[TableNames.FEATURE].create_index("ontology_resource.code")

        log.info("Generating the query to retrieve all fields")
        self.generate_all_fields_query()
        log.info(self.the_query)
        log.info("Creating the dataframe")
        self.the_dataframe = pd.DataFrame(self.db[TableNames.FEATURE].aggregate(json.loads(self.the_query))).set_index("identifier")
        
        # Add a combined "code" column in the format "ontology_name:code"
        log.info("Creating combined ontology code column")
        self.the_dataframe["code"] = self.the_dataframe.apply(
            lambda row: self._create_combined_code(row["onto_system"], row["onto_code"]), 
            axis=1
        )
        log.info("Done.")

    def generate_all_fields_query(self):
        # retrieves all fields without filtering by specific codes
        self.the_query += "["
        self.the_query += json.dumps(Operators.set_variables([{"name": "onto_system", "operation": "$ontology_resource.system"}, {"name": "onto_code", "operation": "$ontology_resource.code"}]))
        self.the_query += ","
        self.the_query += json.dumps(Operators.unset_variables(["_id"]))
        self.the_query += "]"

    def retrieve_records_new(self):
        """Retrieve records for DATA_NEW mode: finds all features matching ontology codes and uses their database names"""
        log.info("***************")
        log.info("Creating indexes")
        self.db[TableNames.RECORD].create_index("instantiates")
        self.db[TableNames.FEATURE].create_index("ontology_resource.system")
        self.db[TableNames.FEATURE].create_index("ontology_resource.code")

        # Find all features matching the specified ontology codes
        log.info("Finding all features matching the ontology codes")
        for ontology_code, info in self.features_info.items():
            match_condition = {
                "ontology_resource.system": info["the_or"].system,
                "ontology_resource.code": info["the_or"].code
            }
            matching_features = self.db[TableNames.FEATURE].find(match_condition)
            
            for feature in matching_features:
                feature_name = feature["name"]
                feature_id = feature["identifier"]
                log.info(f"Found feature: {feature_name} (id: {feature_id}) for ontology code {ontology_code}")
                info["matched_features"].append({
                    "name": feature_name,
                    "identifier": feature_id
                })
        
        log.info(f"Matched features: {self.features_info}")
        
        # Build features_info in the format expected by generate_data_query
        feature_index = 1
        temp_features_info = {}
        
        for ontology_code, info in self.features_info.items():
            for matched_feature in info["matched_features"]:
                feature_name = matched_feature["name"]
                feature_id = matched_feature["identifier"]
                temp_features_info[feature_name] = {
                    "identifier": feature_id,
                    "the_or": info["the_or"],
                    "filter": None,
                    "process": None,
                    "show": True,
                    "i": feature_index
                }
                feature_index += 1
        
        # Replace features_info with the expanded mapping
        self.features_info = temp_features_info
        log.info(f"Expanded features_info: {self.features_info}")

        log.info("Generating the query")
        self.generate_data_query()
        log.info(self.the_query)
        log.info("Creating the dataframe")
        self.the_dataframe = pd.DataFrame(self.db[TableNames.RECORD].aggregate(json.loads(self.the_query))).set_index(
            "has_subject")
        log.info("Done.")

    def generate_data_query(self):
        # 0. compute the lookups
        self.rec_internal(position=1)
        # we now need to add the latest stages to set final variables from the lookups
        self.the_query = self.the_query[0:len(self.the_query) - 1]  # remove the last ] before adding final stages
        self.the_query += ","

        # 1. set variables of the unwind to place them on the first level of the computed record
        set_variables = []
        for feature_name, feature in self.features_info.items():
            lookup_names = "$"
            for j in range(1, feature["i"]):
                lookup_names += f"lookup_{j}."
            lookup_names += "value"
            set_variables.append({"name": feature_name, "operation": lookup_names})
        self.the_query += json.dumps(Operators.set_variables(variables=set_variables))
        self.the_query += ","

        # 2. filter the obtained records
        for feature in self.features_info.values():
            if "filter" in feature and feature["filter"] is not None:
                for select_key, select_operator in feature["filter"].items():
                    self.the_query += json.dumps(Operators.match(field=select_key, value=select_operator, is_regex=False)) + ","

        # 3. set the value to keep + those to flatten
        for feature_name, feature in self.features_info.items():
            if "process" in feature and feature["process"] is not None:
                if feature["process"] == "get_label":
                    # we are in a category, we get its label
                    self.the_query += json.dumps(Operators.set_variables(variables=[{"name": feature_name, "operation": f"${feature_name}.label"}]))
                    self.the_query += ","
                else:
                    # the user gave a personalized post-process, we apply it as is
                    self.the_query += json.dumps(feature["process"])
                    self.the_query += ","
            log.info(self.the_query)

        # 4. project only variables specified in the SELECT (not those only in the WHERE)
        projected_values = {feature_name: 1 for feature_name, feature in self.features_info.items() if feature["show"] is True}
        projected_values["_id"] = 0
        projected_values["has_subject"] = 1
        self.the_query += json.dumps(Operators.project(field=None, projected_value=projected_values))
        self.the_query += "]"
        log.info(self.the_query)

    def _create_combined_code(self, onto_system: str, onto_code: str) -> str:
        """Converts an ontology system URL and code to the combined format 'ontology_name:code'"""
        # Handle NaN values
        if pd.isna(onto_system) or pd.isna(onto_code):
            return ""
        
        # Get the ontology name from the system URL
        ontology_enum = Ontologies.get_enum_from_url(onto_system)
        if ontology_enum and "name" in ontology_enum:
            ontology_name = ontology_enum["name"]
            return f"{ontology_name}:{onto_code}"
        else:
            # Fallback: return just the code if we can't determine the ontology
            return str(onto_code)

    def rec_internal(self, position):
        log.info(f"rec {position} with {self.features_info}")
        feature_id = [feature["identifier"] for feature_name, feature in self.features_info.items() if feature["i"] == position][0]
        if position == len(self.features_info):
            self.the_query += "["
            self.the_query += json.dumps(Operators.match(field="instantiates", value=feature_id, is_regex=False))
            if position > 1:
                self.the_query += ","
                self.the_query += json.dumps(
                    Operators.match(field=None, value={"$expr": {"$eq": [f"$$id_{position - 1}", "$has_subject"]}},
                                    is_regex=False))
            self.the_query += "]"
        else:
            self.the_query += "["
            self.the_query += json.dumps(Operators.match(field="instantiates", value=feature_id, is_regex=False))
            self.the_query += ","
            if position > 1:
                self.the_query += json.dumps(
                    Operators.match(field=None, value={"$expr": {"$eq": [f"$$id_{position - 1}", "$has_subject"]}},
                                    is_regex=False))
                self.the_query += ","

            # the lookup needs to be only partially written in order to compute its internal pipeline
            self.the_query += "{\"$lookup\": {\"from\": \"" + TableNames.RECORD + "\", \"let\": {\"id_" + str(
                position) + "\": \"$has_subject\"}, \"as\": \"lookup_" + str(position) + "\", \"pipeline\": "
            self.rec_internal(position + 1)
            self.the_query += "}}"
            self.the_query += ","
            self.the_query += json.dumps(Operators.unwind(field=f"lookup_{position}"))
            self.the_query += "]"
