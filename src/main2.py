import json
import os
from urllib.request import Request, urlopen

import bson
import pandas as pd
from bson.json_util import loads
from pymongo.mongo_client import MongoClient

from datatypes.OntologyResource import OntologyResource
from enums.Ontologies import Ontologies
from etl.Extract import Extract
from utils.file_utils import read_tabular_file_as_string
from utils.setup_logger import log


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
    # o1 = OntologyResource(ontology=Ontologies.LOINC, full_code="LA6675-8", label=None, quality_stats=None)
    # print(f"label: {o1.label}")
    # o2 = OntologyResource(ontology=Ontologies.LOINC, full_code="60478-5", label=None, quality_stats=None)
    # print(f"label: {o2.label}")
    o3 = OntologyResource(ontology=Ontologies.SNOMEDCT, full_code="258211005", label=None, quality_stats=None)
    print(f"label: {o3.label}")


def main_ontology_code_class():
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
                    o = OntologyResource(ontology=Ontologies.get_enum_from_name(ontology_name=df.iloc[i][onto]), full_code=df.iloc[i][code], label=None, quality_stats=None)
                    log.info(f"----- iteration {it} -----")
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
                                o = OntologyResource(ontology=Ontologies.get_enum_from_name(ontology_name=key), full_code=value, label=None, quality_stats=None)
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


def test_get_inheritance_orphanet():
    orphanet_codes = ["ORPHA:5", "ORPHA:6", "ORPHA:13", "ORPHA:20", "ORPHA:23",
                        "ORPHA:25", "ORPHA:27", "ORPHA:28", "ORPHA:33", "ORPHA:35",
                        "ORPHA:42", "ORPHA:90", "ORPHA:134", "ORPHA:147", "ORPHA:156",
                        "ORPHA:157", "ORPHA:158", "ORPHA:159", "ORPHA:226", "ORPHA:352",
                        "ORPHA:394", "ORPHA:395", "ORPHA:511", "ORPHA:586", "ORPHA:716",
                        "ORPHA:746", "ORPHA:882", "ORPHA:943", "ORPHA:26791", "ORPHA:26792",
                        "ORPHA:26793", "ORPHA:28378", "ORPHA:51188", "ORPHA:69723",
                        "ORPHA:71212", "ORPHA:79157", "ORPHA:79159", "ORPHA:79238",
                        "ORPHA:79239", "ORPHA:79241", "ORPHA:79242", "ORPHA:79282",
                        "ORPHA:79283", "ORPHA:79284", "ORPHA:79651", "ORPHA:168598",
                        "ORPHA:181412", "ORPHA:238583", "ORPHA:247525", "ORPHA:247598"]

    for orphanet_code in orphanet_codes:
        orphanet_code = orphanet_code.replace("ORPHA:", "")
        inheritance = Extract.get_inheritance_from_diagnosis(diagnosis_code=orphanet_code)
        chromosome = Extract.get_chromosome_from_diagnosis(diagnosis_code=orphanet_code)
        log.info(f"{orphanet_code}: {inheritance} ; {chromosome}")


if __name__ == '__main__':
    # main_load_json_from_file_as_bson()
    
    # openssl_dir, openssl_cafile = os.path.split(ssl.get_default_verify_paths().openssl_cafile)
    # print(openssl_dir)
    # print(openssl_cafile)
    main_ontology_api()
    # main_ontology_code_class()

    # test_get_inheritance_orphanet()

    print("Done.")
