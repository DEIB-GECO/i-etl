import copy
import json
import os
import re

import bson
import inflection
import jsonpickle
import pandas as pd
from bson.json_util import loads
from dateutil.parser import parse
from pandas import DataFrame
from pymongo.mongo_client import MongoClient

from datatypes.OntologyResource import OntologyResource
from datatypes.Reference import Reference
from enums.HospitalNames import HospitalNames
from enums.MetadataColumns import MetadataColumns
from enums.Ontologies import Ontologies
from profiles.Hospital import Hospital
from profiles.LaboratoryRecord import LaboratoryRecord
from database.Counter import Counter
from query.Operators import Operators
from statistics.DatabaseStatistics import DatabaseStatistics
from statistics.QualityStatistics import QualityStatistics
from statistics.TimeStatistics import TimeStatistics
from utils.cast_utils import cast_str_to_float, cast_str_to_datetime
from utils.file_utils import read_tabular_file_as_string
from utils.setup_logger import log
from utils.str_utils import remove_operators_in_strings


def main_load_json_from_file_as_bson():
    # d1 = datetime.datetime.strptime("2018-10-13T11:56:52.000Z", "%Y-%m-%dT%H:%M:%S.000Z")
    # d2 = datetime.datetime.strptime("2017-10-13T10:53:53.000Z", "%Y-%m-%dT%H:%M:%S.000Z")
    # d3 = datetime.datetime.strptime("2022-09-13T10:53:53.000Z", "%Y-%m-%dT%H:%M:%S.000Z")
    with MongoClient() as mongo:
        db = mongo.get_database("mydb")
        # my_tuple1 = { "mydate": { "$date": d1.isoformat() } }
        # my_tuple2 = { "mydate": { "$date": d2.isoformat() } }
        # my_tuple3 = { "mydate": { "$date": d3.isoformat() } }
        # { $dateFromString: {
        #     dateString: "2017-02-08T12:10:40.787"
        # } }
        # my_tuples = [my_tuple1, my_tuple2, my_tuple3]
        # with open("../datasets/test_dates/data.json", "w") as data_file:
        #   json.dump(my_tuples, data_file)
        with open("../datasets/test_dates/data.json", "r") as data_file:
            # my_tuples = json.load(data_file)
            my_tuples = bson.json_util.loads(data_file.read())
            # for my_tuple in my_tuples:
            #     db["mycollectionIII"].insert_one(my_tuple)
            db["mycollectionIX"].insert_many(my_tuples, ordered=False)


def main_ontology_api():
    # does not work due to ssl certificates
    # print(Coding.compute_display_from_api(ontology=Ontologies.SNOMEDCT, ontology_code="248152002"))
    # print()
    # print(Coding.compute_display_from_api(ontology=Ontologies.LOINC, ontology_code="4544-3"))
    # print()
    # print(Coding.compute_display_from_api(ontology=Ontologies.PUBCHEM, ontology_code="126894"))
    # print()
    # print(Coding.compute_display_from_api(ontology=Ontologies.GSSO, ontology_code="GSSO_000818"))
    # print()
    # print(Coding.compute_display_from_api(ontology=Ontologies.ORPHANET, ontology_code="ORPHA:159"))
    o1 = OntologyResource(ontology=Ontologies.LOINC, full_code="LA6675-8", quality_stats=None)
    print(f"label: {o1.get_resource_label_from_api(single_ontology_code=o1.full_code)}")
    o2 = OntologyResource(ontology=Ontologies.LOINC, full_code="60478-5", quality_stats=None)
    print(f"label: {o2.get_resource_label_from_api(single_ontology_code=o2.full_code)}")


def main_ontology_code_class():
    # o1 = OntologyResource(ontology=Ontologies.SNOMEDCT, full_code="1306850007")
    # o1.compute_concat_codes()
    # o1.compute_concat_names()
    # o2 = OntologyResource(ontology=Ontologies.SNOMEDCT, full_code="264275001 | Fluorescence polarization immunoassay technique |  :  250895007| Intensity  change |   ")
    # o2.compute_concat_codes()
    # o2.compute_concat_names()
    # o3 = OntologyResource(ontology=Ontologies.SNOMEDCT, full_code="  365471004 |   finding of  details of   relatives  |    :247591002|  affected |=   (410515003|known present( qualifier value) |= 782964007|  genetic disease |)")
    # o3.compute_concat_codes()
    # o3.compute_concat_names()
    o = OntologyResource(ontology=Ontologies.SNOMEDCT, full_code="1306850007")
    o.compute_concat_names()
    log.info(o.concat_names)

    return 0
    resources = json.load(open("datasets/local/all_codes_gen.json"))
    for metadata_file in os.listdir("/Users/nelly/Documents/google-drive-data/metadata/"):
        print(metadata_file)
        df = read_tabular_file_as_string("/Users/nelly/Documents/google-drive-data/metadata/" + metadata_file)
        it = 0
        early_stop = False

        # FIRST ONTOLOGY CODES (first and second)
        for i in range(len(df)):
            for onto, code in zip(["ontology", "secondary_ontology"], ["ontology_code", "secondary_ontology_code"]):
                if not pd.isna(df.iloc[i][onto]) and df.iloc[i][code] not in resources:
                    o = OntologyResource(ontology=Ontologies.get_enum_from_name(ontology_name=df.iloc[i][onto]), full_code=df.iloc[i][code], quality_stats=None)
                    log.info(f"----- iteration {it} -----")
                    log.info(o.full_code)
                    o.compute_concat_codes()
                    o.compute_concat_names()
                    res = int(input())
                    if res == 1:
                        resources[df.iloc[i][code]] = o.to_json()
                    elif res == 0:
                        # broken onto resource, do not add it
                        pass
                    elif res == -1:
                        # early stop
                        early_stop = True
                        break
                    it = it + 1
            if early_stop:
                break
        # JSON VALUES
        if not early_stop:
            for i in range(len(df)):
                json_vals = df.iloc[i]["JSON_values"]
                if not pd.isna(json_vals):
                    log.info(json_vals)
                    json_vals = json.loads(json_vals)
                    for json_resource in json_vals:
                        for key, value in json_resource.items():
                            if key != "value" and key != "explanation" and value != "" and value not in resources:
                                log.info(f"----- iteration {it} -----")
                                o = OntologyResource(ontology=Ontologies.get_enum_from_name(ontology_name=key), full_code=value)
                                log.info(o.full_code)
                                o.compute_concat_codes()
                                o.compute_concat_names()
                                res = int(input())
                                if res == 1:
                                    resources[str(value)] = o.to_json()
                                elif res == 0:
                                    # broken onto resource, do not add it
                                    pass
                                elif res == -1:
                                    # early stop
                                    early_stop = True
                                    break
                                it = it + 1
                        if early_stop:
                            break
                if early_stop:
                    break
        if early_stop:
            break
    # log.info(len(resources))
    # log.info(resources)

    with open("datasets/local/all_codes_gen.json", "w") as file:
        file.write(json.dumps(resources, indent=4))


if __name__ == '__main__':
    # main_load_json_from_file_as_bson()
    
    # openssl_dir, openssl_cafile = os.path.split(ssl.get_default_verify_paths().openssl_cafile)
    # print(openssl_dir)
    # print(openssl_cafile)
    # main_ontology_api()

    # main_ontology_code_class()

    print("Done.")
