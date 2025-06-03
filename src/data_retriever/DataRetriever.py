import dataclasses
import json

import pandas as pd
from pandas import DataFrame
from pymongo import MongoClient

from database.Operators import Operators
from entities.OntologyResource import OntologyResource
from enums.Ontologies import Ontologies
# from entities.OntologyResource import OntologyResource
# from enums.Ontologies import Ontologies
from enums.TableNames import TableNames
from utils.setup_logger import log


@dataclasses.dataclass(kw_only=True)
class DataRetriever:
    # db connection
    mongodb_url: str  # "mongodb://localhost:27018/"
    db_name: str

    # user input to build the query
    # FEATURE_CODES = {"hypotonia": "8116006:278201002=(\"HPO\",288467006)", "vcf_path": "12953007"}
    feature_codes: dict  # user input of the form {"user variable name": "ontology code"
    # FEATURES_VALUE_PROCESS = {"hypotonia": {"$addFields": { "hypotonia": { "$in": ["hp:0001252", "$hypotonia"] }}}, "vcf_path": None}
    feature_value_process: dict  # user input of the form {"user variable name": { the mongo db operation or None } }
    the_dataframe: DataFrame = dataclasses.field(init=False)

    def __post_init__(self):
        self.client = MongoClient(host=self.mongodb_url, serverSelectionTimeoutMS=30, w=0, directConnection=True)
        log.info(self.client)
        self.db = self.client[self.db_name]
        log.info(self.db)
        self.identifiers = {}
        self.the_query = ""

        # normalize ontology resource codes
        # we have to set the system to no ontology, otherwise no ontology resource will be created
        # we have to set the label to the non-empty string, otherwise a label wil be computed
        # OntologyResource(system=Ontologies.NONE, code=code, label=" ", quality_stats=None).code
        self.feature_codes = {key: OntologyResource(system=Ontologies.NONE, code=code, label=" ", quality_stats=None).code for key, code in self.feature_codes.items()}

    def run(self):
        feature_codes_inv = {v: k for k, v in self.feature_codes.items()}
        log.info(f"FEATURE_CODES: {self.feature_codes}")
        log.info(f"FEATURE_CODES_INV: {feature_codes_inv}")
        i = 1
        for res in self.db[TableNames.FEATURE].find({"ontology_resource.code": {"$in": list(self.feature_codes.values())}}):
            log.info(res)
            feature_user_name = feature_codes_inv[res["ontology_resource"]["code"]]
            self.identifiers[i] = (res["ontology_resource"]["code"], res["identifier"], feature_user_name, self.feature_value_process[feature_user_name])
            i = i + 1
        log.info(f"identifiers: {self.identifiers}")

        if len(self.identifiers) != len(self.feature_codes):
            log.info("Not all given features have been found in the database. Abort.")
            exit()

        log.info("***************")
        log.info("Creating indexes")
        self.db[TableNames.RECORD].create_index("instantiates")
        self.db[TableNames.FEATURE].create_index("ontology_resource.code")

        log.info("Generating the query")
        self.generate_query()
        log.info(self.the_query)
        log.info("Creating the dataframe")
        self.the_dataframe = pd.DataFrame(self.db[TableNames.RECORD].aggregate(json.loads(self.the_query))).set_index("has_subject")
        log.info("Done.")

    def generate_query(self):
        self.rec_internal(position=1)
        # we now need to add the latest stages to set final variables from the lookups
        # this cannot be done during the recursion, so we do it now
        self.the_query = self.the_query[0:len(self.the_query) - 1]  # remove the last ] before adding final stages
        # print(self.the_query)
        self.the_query += ","
        set_variables = []
        for i in self.identifiers.keys():
            lookup_names = "$"
            for j in range(1, i):
                lookup_names += f"lookup_{j}."
            lookup_names += "value"
            set_variables.append({"name": self.identifiers[i][2], "operation": lookup_names})
        self.the_query += json.dumps(Operators.set_variables(variables=set_variables))
        projected_values = {f_name: 1 for f_name in self.feature_codes}
        projected_values["_id"] = 0
        projected_values["has_subject"] = 1
        self.the_query += ","
        for i in self.identifiers.keys():
            if self.identifiers[i][3] is not None:
                self.the_query += json.dumps(self.identifiers[i][3])
                self.the_query += ","
        self.the_query += json.dumps(Operators.project(field=None, projected_value=projected_values))
        self.the_query += "]"
        print(self.the_query)

    def rec_internal(self, position):
        # print(f"rec {position}/{max_position} with {identifiers}")

        if position == len(self.identifiers):
            self.the_query += "["
            # print(self.the_query)
            self.the_query += json.dumps(
                Operators.match(field="instantiates", value=self.identifiers[position][1], is_regex=False))
            # print(self.the_query)
            self.the_query += ","
            # print(self.the_query)
            # if position > 1 and position < max_position:
            self.the_query += json.dumps(
                Operators.match(field=None, value={"$expr": {"$eq": [f"$$id_{position - 1}", "$has_subject"]}},
                                is_regex=False))
            # print(self.the_query)
            self.the_query += "]"
            # print(self.the_query)
        else:
            self.the_query += "["
            # print(self.the_query)
            self.the_query += json.dumps(Operators.match(field="instantiates", value=self.identifiers[position][1], is_regex=False))
            # print(self.the_query)
            self.the_query += ","
            # print(self.the_query)
            if position > 1:
                self.the_query += json.dumps(
                    Operators.match(field=None, value={"$expr": {"$eq": [f"$$id_{position - 1}", "$has_subject"]}},
                                    is_regex=False))
                # print(self.the_query)
                self.the_query += ","
                # print(self.the_query)

            # the lookup needs to be only partially written in order to compute its internal pipeline
            # this is why we need to write manually the string in order to compute the internal pipeline in-between
            self.the_query += "{\"$lookup\": {\"from\": \"" + TableNames.RECORD + "\", \"let\": {\"id_" + str(
                position) + "\": \"$has_subject\"}, \"as\": \"lookup_" + str(position) + "\", \"pipeline\": "
            # print(self.the_query)
            self.rec_internal(position + 1)
            # print(self.the_query)
            self.the_query += "}}"
            # print(self.the_query)
            self.the_query += ","
            # print(self.the_query)
            self.the_query += json.dumps(Operators.unwind(field=f"lookup_{position}"))
            # print(self.the_query)
            self.the_query += "]"
            # print(self.the_query)
