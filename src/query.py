import json
import os
import pprint

import pandas as pd
from pymongo import MongoClient

from database.DataRetriever import DataRetriever
from database.Operators import Operators
from enums.TableNames import TableNames

THE_QUERY = ""

def generate_query(identifiers):
    rec_internal(position=1, max_position=len(identifiers), identifiers=identifiers)
    global THE_QUERY  # this contains all the MATCH and LOOKUP operations
    # we now need to add the latest stages to set final variables from the lookups
    # this cannot be done during the recursion, so we do it now
    THE_QUERY = THE_QUERY[0:len(THE_QUERY)-1]  # remove the last ] before adding final stages
    # print(THE_QUERY)
    THE_QUERY += ","
    set_variables = []
    for i in identifiers.keys():
        lookup_names = "$"
        for j in range(1,i):
            lookup_names += f"lookup_{j}."
        lookup_names += "value"
        set_variables.append({"name": identifiers[i][2], "operation": lookup_names})
    THE_QUERY += json.dumps(Operators.set_variables(variables=set_variables))
    projected_values = {f_name: 1 for f_name in FEATURE_CODES}
    projected_values["_id"] = 0
    projected_values["has_subject"] = 1
    THE_QUERY += ","
    for i in identifiers.keys():
        if identifiers[i][4] is not None:
            THE_QUERY += json.dumps(identifiers[i][4])
            THE_QUERY += ","
    THE_QUERY += json.dumps(Operators.project(field=None, projected_value=projected_values))
    THE_QUERY += "]"
    print(THE_QUERY)


def rec_internal(position, max_position, identifiers):
    global THE_QUERY
    # print(f"rec {position}/{max_position} with {identifiers}")

    if position == max_position:
        THE_QUERY += "["
        # print(THE_QUERY)
        THE_QUERY += json.dumps(Operators.match(field="instantiates", value=identifiers[position][1], is_regex=False))
        # print(THE_QUERY)
        THE_QUERY += ","
        # print(THE_QUERY)
        # if position > 1 and position < max_position:
        THE_QUERY += json.dumps(Operators.match(field=None, value={"$expr": {"$eq": [f"$$id_{position-1}", "$has_subject"]}}, is_regex=False))
        # print(THE_QUERY)
        THE_QUERY += "]"
        # print(THE_QUERY)
    else:
        THE_QUERY += "["
        # print(THE_QUERY)
        THE_QUERY += json.dumps(Operators.match(field="instantiates", value=identifiers[position][1], is_regex=False))
        # print(THE_QUERY)
        THE_QUERY += ","
        # print(THE_QUERY)
        if position > 1:
            THE_QUERY += json.dumps(Operators.match(field=None, value={"$expr": {"$eq": [f"$$id_{position-1}", "$has_subject"]}}, is_regex=False))
            # print(THE_QUERY)
            THE_QUERY += ","
            # print(THE_QUERY)

        # the lookup needs to be only partially written in order to compute its internal pipeline
        # this is why we need to write manually the string in order to compute the internal pipeline in-between
        THE_QUERY += "{\"$lookup\": {\"from\": \""+TableNames.RECORD+"\", \"let\": {\"id_"+str(position)+"\": \"$has_subject\"}, \"as\": \"lookup_"+str(position)+"\", \"pipeline\": "
        # print(THE_QUERY)
        rec_internal(position + 1, max_position, identifiers=identifiers)
        # print(THE_QUERY)
        THE_QUERY += "}}"
        # print(THE_QUERY)
        THE_QUERY += ","
        # print(THE_QUERY)
        THE_QUERY += json.dumps(Operators.unwind(field=f"lookup_{position}"))
        # print(THE_QUERY)
        THE_QUERY += "]"
        # print(THE_QUERY)


if __name__ == '__main__':
    # ssh -L 27018:131.175.120.88:27018 barret@131.175.120.88
    # command to open SSH tunnel to access the databases on GECO from my laptop
    # to be run before running this script

    # SJD
    FEATURE_CODES = {"hypotonia": "398152000", "vcf_path": "12953007"}  # , "gene": "82256003:734841007"}
    FEATURES_VALUE_PROCESS = {"hypotonia": None, "vcf_path": None}
    dataRetriever = DataRetriever(mongodb_url="mongodb://localhost:27018/", db_name="better_hsjd",
                                  feature_codes=FEATURE_CODES, feature_value_process=FEATURES_VALUE_PROCESS)
    dataRetriever.run()
    print(dataRetriever.the_dataframe)

    # IMGGE
    FEATURE_CODES = {"hypotonia": "8116006:278201002=(\"HPO\",288467006)", "vcf_path": "12953007"}
    FEATURES_VALUE_PROCESS = {"hypotonia": {"$addFields": {
                                "hypotonia": {
                                  "$in": [
                                    "hp:0001252",
                                    "$hypotonia"
                                  ]
                                }
                            }},
                            "vcf_path": None}
    dataRetriever = DataRetriever(mongodb_url="mongodb://localhost:27018/", db_name="better_imgge", feature_codes=FEATURE_CODES, feature_value_process=FEATURES_VALUE_PROCESS)
    dataRetriever.run()
    print(dataRetriever.the_dataframe)
