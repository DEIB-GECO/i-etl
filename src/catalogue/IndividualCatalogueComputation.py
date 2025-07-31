import json
import os

import requests
from utils.cast_utils import cast_str_to_boolean

from enums.MetadataColumns import MetadataColumns

from utils.file_utils import read_tabular_file_as_string, read_tabular_file_as_string_2

from database.Database import Database
from database.Execution import Execution
from database.Operators import Operators
from enums.AggregationTypes import AggregationTypes
from enums.CatalogueEntries import CatalogueEntries
from enums.CatalogueProfileEntries import CatalogueProfileEntries
from enums.TableNames import TableNames
from utils.setup_logger import log


class IndividualCatalogueComputation:
    def __init__(self, database: Database, execution: Execution):
        self.database = database
        self.execution = execution
        self.catalogue_data = []
        self.whitelist = {}  # <feature name: [list of allowed statistics]>
        self.catalogue_filepath = os.path.join(self.execution.working_dir_current, f"catalogue_{self.database.execution.db_name}.json")
        self.token = self.database.execution.token
        self.usecase = self.database.execution.usecase

    def run(self) -> None:
        self.obtain_whitelist()
        self.retrieve_data_for_catalogue()
        self.send_to_webapp()

    def obtain_whitelist(self):
        log.info("obtain whitelist")
        whitelist_metadata = read_tabular_file_as_string_2(filepath=self.execution.metadata_filepath, sheet_name="whitelist")
        for row in whitelist_metadata.itertuples(index=False):
            feature_name = MetadataColumns.normalize_name(column_name=row[whitelist_metadata.columns.get_loc(MetadataColumns.COLUMN_NAME)])
            self.whitelist[feature_name] = []
            for column in whitelist_metadata.columns:
                try:
                    boolean_value = cast_str_to_boolean(row[whitelist_metadata.columns.get_loc(column)])
                    if boolean_value:
                        self.whitelist[feature_name].append(column)
                except:
                    # this an X or NaN value, we are not allowed to show that statistic for the feature
                    # therefore, we do not add the statistic to the list
                    pass
        log.info(self.whitelist)

    def send_to_webapp(self):
        # endpoint = f"https://web-api.better-health-project.eu/{self.usecase}/data-ingestion"  # Noosware PRODUCTION environment
        endpoint = f"https://web-api-demo.better-health-project.eu/{self.usecase}/data-ingestion"  # Noosware TEST environment
        log.info(endpoint)

        with open(self.catalogue_filepath, "r") as f:
            the_catalogue_data = json.load(f)
            # log.info(the_catalogue_data)
            headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {self.token}'}
            log.info(f"Sending data to the catalogue web interface...")
            log.info(headers)
            response = requests.post(endpoint, headers=headers, json=the_catalogue_data)
            log.info(f"The response is: {response}")
            log.info(f"The response is: {response.reason}")
            log.info(f"The response is: {response.text}")

    def retrieve_data_for_catalogue(self) -> None:
        # 2. for each dataset, get its info, profile and features
        datasets = self.get_datasets()
        # log.info(datasets)
        for dataset_global_identifier in datasets:
            log.info(dataset_global_identifier)
            dataset_entry = {"identifier": dataset_global_identifier}
            dataset_entry = self.set_dataset_info_and_profile(dataset_entry=dataset_entry, dataset_global_identifier=dataset_global_identifier, dataset_name=datasets[dataset_global_identifier])
            dataset_entry = self.set_dataset_features(dataset_entry=dataset_entry, dataset_gid=dataset_global_identifier)
            self.catalogue_data.append(dataset_entry)
        # log.info(json.dumps(self.catalogue_data, default=str))  # default=str for converting datetime objects to strings
        with open(self.catalogue_filepath, "w") as f:
            f.write(json.dumps(self.catalogue_data, default=str))

    def get_datasets(self) -> dict:
        cursor = self.database.find_operation(table_name=TableNames.DATASET, filter_dict={}, projection={"global_identifier": 1, "docker_path": 1})
        return {dataset_instance["global_identifier"]: IndividualCatalogueComputation.get_filename_from_filepath(dataset_instance["docker_path"]) for dataset_instance in cursor}

    @classmethod
    def get_filename_from_filepath(cls, filepath: str) -> str:
        return os.path.basename(filepath)

    def set_dataset_info_and_profile(self, dataset_entry: dict, dataset_global_identifier: str, dataset_name: str) -> dict:
        dataset_entry[CatalogueEntries.DATASET_ID] = dataset_name  # each dataset has two fields: the identifier (global ID) and the dataset_id (which is shown as the dataset name in the catalogue)
        dataset_entry["dataset_info"] = {}
        dataset_entry["dataset_profile"] = {}
        result = self.database.find_operation(table_name=TableNames.DATASET, filter_dict={"global_identifier": dataset_global_identifier}, projection={})
        for dataset in result:
            dataset_entry["dataset_info"][CatalogueEntries.DATASET_VERSION] = dataset[CatalogueEntries.DATASET_VERSION] if CatalogueEntries.DATASET_VERSION in dataset else None
            dataset_entry["dataset_info"][CatalogueEntries.DATASET_RELEASE_DATE] = dataset[CatalogueEntries.DATASET_RELEASE_DATE]["$date"].replace("T", " ").replace("Z", "") if CatalogueEntries.DATASET_RELEASE_DATE in dataset else None  # Noosware require to have Python dates, not MongoDB ones
            dataset_entry["dataset_info"][CatalogueEntries.DATASET_LAST_UPDATE_DATE] = dataset[CatalogueEntries.DATASET_LAST_UPDATE_DATE] if CatalogueEntries.DATASET_LAST_UPDATE_DATE in dataset else None
            dataset_entry["dataset_info"][CatalogueEntries.DATASET_VERSION_NOTES] = dataset[CatalogueEntries.DATASET_VERSION_NOTES] if CatalogueEntries.DATASET_VERSION_NOTES in dataset else None
            dataset_entry["dataset_info"][CatalogueEntries.DATASET_LICENSE] = dataset[CatalogueEntries.DATASET_LICENSE] if CatalogueEntries.DATASET_LICENSE in dataset else None
            dataset_entry["dataset_profile"][CatalogueEntries.DS_PROFILE_DESCRIPTION] = dataset[CatalogueEntries.DS_PROFILE_DESCRIPTION] if CatalogueEntries.DS_PROFILE_DESCRIPTION in dataset else None
            dataset_entry["dataset_profile"][CatalogueEntries.DS_PROFILE_THEME] = dataset[CatalogueEntries.DS_PROFILE_THEME] if CatalogueEntries.DS_PROFILE_THEME in dataset else None
            dataset_entry["dataset_profile"][CatalogueEntries.DS_PROFILE_FILE_TYPE] = dataset[CatalogueEntries.DS_PROFILE_FILE_TYPE] if CatalogueEntries.DS_PROFILE_FILE_TYPE in dataset else None
            dataset_entry["dataset_profile"][CatalogueEntries.DS_PROFILE_SIZE] = dataset[CatalogueEntries.DS_PROFILE_SIZE] if CatalogueEntries.DS_PROFILE_SIZE in dataset else None
            dataset_entry["dataset_profile"][CatalogueEntries.DS_PROFILE_NB_TUPLES] = dataset[CatalogueEntries.DS_PROFILE_NB_TUPLES] if CatalogueEntries.DS_PROFILE_NB_TUPLES in dataset else None
            dataset_entry["dataset_profile"][CatalogueEntries.DS_PROFILE_TUPLE_COMPLETENESS] = dataset[CatalogueEntries.DS_PROFILE_TUPLE_COMPLETENESS] if CatalogueEntries.DS_PROFILE_TUPLE_COMPLETENESS in dataset else None
            dataset_entry["dataset_profile"][CatalogueEntries.DS_PROFILE_TUPLE_UNIQUENESS] = dataset[CatalogueEntries.DS_PROFILE_TUPLE_UNIQUENESS] if CatalogueEntries.DS_PROFILE_TUPLE_UNIQUENESS in dataset else None
            return dataset_entry
        # else: no dataset has been found: nothing to do

    def set_dataset_features(self, dataset_entry: dict, dataset_gid: str) -> dict:
        dataset_entry["features"] = []
        features_infos = {}
        features_data_types = {}

        # for each feature, set its information
        cursor = self.database.find_operation(table_name=TableNames.FEATURE, filter_dict={"dataset": dataset_gid}, projection={})
        for feature in cursor:
            feature_id = feature["identifier"]
            if feature_id not in features_infos:
                features_infos[feature_id] = {}

            # compute the feature information
            features_infos[feature_id][CatalogueEntries.FEATURE_ID] = feature_id
            datatype = feature["data_type"] if "data_type" in feature else None
            features_infos[feature_id][CatalogueEntries.FEATURE_DATA_TYPE] = datatype
            features_data_types[feature_id] = datatype
            # log.info(features_data_types)
            keys = [
                CatalogueEntries.FEATURE_NAME,
                CatalogueEntries.FEATURE_DESCRIPTION,
                CatalogueEntries.FEATURE_ONTOLOGY_CODE,
                # CatalogueEntries.FEATURE_ONTOLOGY_LABEL,
                CatalogueEntries.FEATURE_VISIBILITY,
                CatalogueEntries.FEATURE_ENTITY_TYPE
            ]
            for key in keys:
                if key == CatalogueEntries.FEATURE_ONTOLOGY_CODE:
                    if "ontology_resource" in feature and "code" in feature["ontology_resource"]:
                        features_infos[feature_id][key] = feature["ontology_resource"]["code"]
                # elif key == CatalogueEntries.FEATURE_ONTOLOGY_LABEL:
                #     if "ontology_resource" in feature and "label" in feature["ontology_resource"]:
                #         features_infos[feature_id][key] = feature["ontology_resource"]["label"]
                else:
                    if key in feature:
                        features_infos[feature_id][key] = feature[key]
                    else:
                        features_infos[feature_id][key] = None
            # log.info(features_infos)
            # compute the feature domain
            feature_domain = {}
            agg_type = AggregationTypes.get_agg_type(data_type=datatype)
            if agg_type in [AggregationTypes.CONTINUOUS, AggregationTypes.DATE]:
                keys = [CatalogueEntries.DOMAIN_MIN, CatalogueEntries.DOMAIN_MAX]
            elif agg_type == AggregationTypes.CATEGORICAL:
                keys = [CatalogueEntries.DOMAIN_CAT_ACCEPTED]
            else:
                log.error(f"The aggregation type {agg_type} is unknown.")
            for key in keys:
                feature_domain[key] = feature["domain"][key] if "domain" in feature and key in feature["domain"] else None
            features_infos[feature_id]["domain"] = feature_domain

        # associate each feature name to its feature id
        # this is necessary because the whitelist is based on feature names while the profiles are based on feature ids
        cursor = self.database.find_operation(table_name=TableNames.FEATURE, filter_dict={}, projection={"name": 1, "identifier": 1})
        map_feature_id_name = {feature["identifier"]: feature["name"] for feature in cursor}

        # for each feature, set its profile using the table FEATURE_PROFILE
        cursor = self.database.find_operation(table_name=TableNames.FEATURE_PROFILE, filter_dict={"dataset": dataset_gid}, projection={})
        for feature in cursor:
            feature_id = feature["instantiates"]
            if feature_id not in features_infos:
                features_infos[feature_id] = {}

            # compute the feature aggregation type, based on the feature datatype
            # datatype = features_data_types[feature_id] if feature_id in features_data_types else None
            # agg_type = AggregationTypes.get_agg_type(data_type=datatype)
            # log.info(f"Feature {feature_id} has datatype '{datatype}'" )

            # compute the feature profile ONLY FOR WHAT CAN BE SHOWN IN THE CATALOGUE (c.f. "whitelist" sheet in the metadata)
            feature_profile = {}

            # we automatically create the set of keys to get from the whitelist
            # the names of the statistics used in the whitelist are the same as the ones in CatalogueProfileEntries
            log.info(f"{map_feature_id_name[feature_id]}: {self.whitelist[map_feature_id_name[feature_id]]}")
            keys = self.whitelist[map_feature_id_name[feature_id]]
            # if agg_type == AggregationTypes.CONTINUOUS:
            #     keys.extend([
            #         CatalogueProfileEntries.F_NUM_PROFILE_MIN, CatalogueProfileEntries.F_NUM_PROFILE_MAX,
            #         CatalogueProfileEntries.F_NUM_PROFILE_MEAN, CatalogueProfileEntries.F_NUM_PROFILE_MEDIAN,
            #         CatalogueProfileEntries.F_NUM_PROFILE_STD_DEV, CatalogueProfileEntries.F_NUM_PROFILE_SKEWNESS,
            #         CatalogueProfileEntries.F_NUM_PROFILE_KURTOSIS, CatalogueProfileEntries.F_NUM_PROFILE_MED_ABS_DEV,
            #         CatalogueProfileEntries.F_NUM_PROFILE_INTER_QU_RANGE, CatalogueProfileEntries.F_NUM_PROFILE_CORRELATION
            #     ])
            # elif agg_type == AggregationTypes.DATE:
            #     keys.extend([
            #         CatalogueProfileEntries.F_NUM_PROFILE_MIN, CatalogueProfileEntries.F_NUM_PROFILE_MAX,
            #         CatalogueProfileEntries.F_NUM_PROFILE_MEDIAN, CatalogueProfileEntries.F_NUM_PROFILE_INTER_QU_RANGE
            #     ])
            # elif agg_type == AggregationTypes.CATEGORICAL:
            #     keys.extend([
            #         CatalogueProfileEntries.F_CAT_PROFILE_IMBALANCE, CatalogueProfileEntries.F_CAT_PROFILE_CONSTANCY,
            #         CatalogueProfileEntries.F_CAT_PROFILE_MODE
            #     ])
            # else:
            #     log.error(f"Unrecognized data type '{datatype}' for feature {feature_id}")
            #     keys.extend([])
            for key in keys:
                if key in feature:
                    feature_profile[key] = feature[key]
                else:
                    # the key used in the FeatureProfile table (does not match exactly the catalogue keys)
                    new_keys = {
                        CatalogueProfileEntries.F_NUM_PROFILE_MIN: "min_value",
                        CatalogueProfileEntries.F_NUM_PROFILE_MAX: "max_value",
                        CatalogueProfileEntries.F_NUM_PROFILE_MEAN: "mean_value",
                        CatalogueProfileEntries.F_NUM_PROFILE_MEDIAN: "median_value",
                        CatalogueProfileEntries.F_NUM_PROFILE_STD_DEV: "std_value",
                        CatalogueProfileEntries.F_NUM_PROFILE_MED_ABS_DEV: "ema",
                        CatalogueProfileEntries.F_NUM_PROFILE_INTER_QU_RANGE: "iqr",
                        CatalogueProfileEntries.F_NUM_PROFILE_CORRELATION: "pearson",
                    }
                    if key in new_keys and new_keys[key] in feature:
                        feature_profile[key] = feature[new_keys[key]]
                    else:
                        feature_profile[key] = None
            # store all the information in the JSON dict of the current feature
            # log.info(f"for feature {feature_id} the profile is {json.dumps(feature_profile)}")
            features_infos[feature_id]["profile"] = feature_profile
        # log.info(features_infos)
        # log.info(features_infos.values())
        dataset_entry["features"].extend(features_infos.values())
        # log.info(dataset_entry)
        return dataset_entry

    def compute_nb_patients_for_dataset(self, dataset_gid: str) -> int:
        # computes the number of patients used by this dataset
        where_dataset_gid = Operators.match(field="dataset", value=dataset_gid, is_regex=False)
        get_subject = Operators.project(field="has_subject", projected_value=None)
        operations = [where_dataset_gid, get_subject]
        operations.append(Operators.group_by(group_key="$has_subject", groups=[]))  # to simulate distinct because we cannot combine distinct and an aggregation pipeline
        operations.append(Operators.group_by(group_key="$has_subject", groups=[{"name": "p_count", "operator": "$sum", "field": 1}]))  # to simulate count
        # log.info(operations)
        cursor = self.database.db[TableNames.RECORD].aggregate(operations)
        for result in cursor:
            return result["p_count"]

    def compute_stats(self, dataset_gid: str, feature_id: str, feature_name: str) -> dict:
        rec_count = self.database.count_documents(table_name=TableNames.RECORD, filter_dict={"dataset": dataset_gid, "instantiates": feature_id})
        cursor = self.database.find_operation(table_name=TableNames.STATS_QUALITY, filter_dict={}, projection={f"empty_cells_per_column.{feature_name}": 1})
        count_empty_cells = 0
        for res in cursor:
            if "empty_cells_per_column" in res and feature_name in res["empty_cells_per_column"]:
                count_empty_cells = res["empty_cells_per_column"][feature_name]
            else:
                count_empty_cells = 0
            break
        percentage_empty_cells = round((float(count_empty_cells) / (count_empty_cells + rec_count)) * 100, 3)
        return {"record_count": rec_count, "perc_empty_cells": percentage_empty_cells}

    # to test the catalogue easily on a local existing database
    if __name__ == "__main__":
        # send data to polimi test on the catalogue
        bearer_token_test_polimi = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Im95UFVnbXdJMnNNb2U5NjdfQmMyUyJ9.eyJodHRwczovL2JldHRlci1iYWNrLWRlbW8uY29tL2hvc3BpdGFsSWQiOiI4IiwiaXNzIjoiaHR0cHM6Ly9kZW1vLW5sLmV1LmF1dGgwLmNvbS8iLCJzdWIiOiJNOXVNWEhYbTZIQUNoWHF1NTdMUFczbkZQM0lnODloQ0BjbGllbnRzIiwiYXVkIjoiaHR0cHM6Ly9iZXR0ZXItYmFjay1kZW1vLmNvbSIsImlhdCI6MTc0NjYxNzQ3NSwiZXhwIjoxNzQ5MjA5NDc1LCJzY29wZSI6InBvc3Q6ZGF0YS1zZXQiLCJndHkiOiJjbGllbnQtY3JlZGVudGlhbHMiLCJhenAiOiJNOXVNWEhYbTZIQUNoWHF1NTdMUFczbkZQM0lnODloQyIsInBlcm1pc3Npb25zIjpbInBvc3Q6ZGF0YS1zZXQiXX0.IdeLJ0knKOlEJqYsvwT-6vWC6nOjCx8Uv0VcuGz59AbnjKGKnKvYCgJiu8BXD8J44KINCSczshZqqdyf7pIo_YjGe8mjA2a_gvdz3y4_WRMG-3_AYMbkMX8MrJKcaiUuOvAsS4mFEtTec_h8oxJnEdn5b1q8hwnQKSvC0GtBztN3flLRnV4yP4mmoP0hNEp65bGrPC3kuFnZ2g4SKywua5b4NMu7TsVTQO5xAXoGZy7-uK_hz5hgVrcmI0ZtIRMkB7l-MO3Hvv3O8omaGJQgBVtAKa93dWA0C6pFTt-SMiQ6ouzw0BFa1wExHtkDc2Vo-oCs7wYB5s9jH2KB8Dp-LA"
        bearer_token_showcase = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Im95UFVnbXdJMnNNb2U5NjdfQmMyUyJ9.eyJodHRwczovL2JldHRlci1iYWNrLWRlbW8uY29tL2hvc3BpdGFsSWQiOiI5IiwiaXNzIjoiaHR0cHM6Ly9kZW1vLW5sLmV1LmF1dGgwLmNvbS8iLCJzdWIiOiJNOXVNWEhYbTZIQUNoWHF1NTdMUFczbkZQM0lnODloQ0BjbGllbnRzIiwiYXVkIjoiaHR0cHM6Ly9iZXR0ZXItYmFjay1kZW1vLmNvbSIsImlhdCI6MTc1MDE3MjIzOSwiZXhwIjoxNzUyNzY0MjM5LCJzY29wZSI6InBvc3Q6ZGF0YS1zZXQiLCJndHkiOiJjbGllbnQtY3JlZGVudGlhbHMiLCJhenAiOiJNOXVNWEhYbTZIQUNoWHF1NTdMUFczbkZQM0lnODloQyIsInBlcm1pc3Npb25zIjpbInBvc3Q6ZGF0YS1zZXQiXX0.GJ_h7ixuufKbO0ov-RX5xlI5-xc4v1OknZYJr3lKrIzXDjz--djfYPnnNazg3agIiSbe7K0L7Igl4wKlc5p2Zj5Hlwl-clFj9GPSWxr3aB0n6xfd5qk_TyOm4Uyog_qttkq7yKZhCgefrWl_vU36iEonUCRRSNZc2WMMvt7seUNyUesP3guqBvmPA2lQUEVUwmkZJGKmmSsGeVFwaYvVVi7ryWs-zoSYC_qDky_hEL69MtJpWBRgWMuX0vfaoPbrbV_W_DtMoJlY1IpDtxBWVOCuYC2fe5HLiOXwLAd3sRABW5m81qBKmntb9MWKXw3Wl9F9d8NaHeTZKjE88ZIRuQ"
        use_case = "paediatric"
        endpoint = "https://backend-374895817917.europe-west1.run.app/paediatric/data-ingestion"
        # endpoint = "https://web-api.better-health-project.eu/paediatric/data-ingestion"

        # # send H1 data to test polimi hospital
        # catalogue_file = "catalogue_better_kidney_h1.json"
        # with open(catalogue_file, "r") as f:
        #     the_catalogue_data = json.load(f)
        #     # print(the_catalogue_data)
        #     headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {bearer_token_test_polimi}'}
        #     response = requests.post(endpoint, headers=headers, json=the_catalogue_data)
        #     print(response)

        # send H2 data to showcase hospital
        catalogue_file = "catalogue_better_kidney_h2.json"
        with open(catalogue_file, "r") as f:
            the_catalogue_data = json.load(f)
            print(the_catalogue_data)
            headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {bearer_token_showcase}'}
            response = requests.post(endpoint, headers=headers, json=the_catalogue_data)
            print(response)
