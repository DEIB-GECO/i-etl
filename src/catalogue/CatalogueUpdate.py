import json

from database.Database import Database
from enums.AggregationTypes import AggregationTypes
from enums.CatalogueEntries import CatalogueEntries
from enums.DataTypes import DataTypes
from enums.TableNames import TableNames
from database.Operators import Operators
from utils.setup_logger import log


class CatalogueUpdate:
    def __init__(self, database: Database):
        self.database = database
        self.catalogue_data = []

    def run(self) -> None:
        self.create_views()
        self.retrieve_data_for_catalogue()

    def create_views(self) -> None:
        # create a materialized view for storing pairs <feature id, dataset gid>
        get_instantiate = Operators.project(field=["dataset", "instantiates"], projected_value=None)
        # group by is used to simulate a distinct because we cannot combine distinct and an aggregation pipeline
        operations = [get_instantiate,
                      Operators.group_by(group_key={"instantiates": "$instantiates", "dataset": "$dataset"}, groups=[])]
        log.info(operations)
        self.database.create_on_demand_view(table_name=TableNames.RECORD, view_name=TableNames.VIEW_FEATURES_DATASET,
                                            pipeline=operations)

    def retrieve_data_for_catalogue(self) -> None:
        # 2. for each dataset, get its info, profile and features
        datasets = self.get_datasets()
        log.info(datasets)
        for dataset_global_identifier in datasets:
            dataset_entry = {"identifier": dataset_global_identifier}
            dataset_entry = self.set_dataset_info_and_profile(dataset_entry=dataset_entry, dataset_global_identifier=dataset_global_identifier)
            dataset_entry = self.set_dataset_features(dataset_entry=dataset_entry, dataset_gid=dataset_global_identifier)
            self.catalogue_data.append(dataset_entry)
        log.info(json.dumps(self.catalogue_data, default=str))  # default=str for converting datetime objects to strings

    def get_datasets(self) -> list:
        cursor = self.database.find_operation(table_name=TableNames.DATASET, filter_dict={}, projection={})
        dataset_identifiers = [dataset_instance["global_identifier"] for dataset_instance in cursor]
        log.info(dataset_identifiers)
        return dataset_identifiers

    def set_dataset_info_and_profile(self, dataset_entry: dict, dataset_global_identifier: str) -> dict:
        dataset_entry[CatalogueEntries.DATASET_ID] = dataset_global_identifier
        dataset_entry["dataset_info"] = {}
        dataset_entry["dataset_profile"] = {}
        result = self.database.find_operation(table_name=TableNames.DATASET, filter_dict={"global_identifier": dataset_global_identifier}, projection={})
        for res in result:
            dataset_entry["dataset_info"][CatalogueEntries.DATASET_VERSION] = res[CatalogueEntries.DATASET_VERSION] if CatalogueEntries.DATASET_VERSION in res else None
            dataset_entry["dataset_info"][CatalogueEntries.DATASET_RELEASE_DATE] = res[CatalogueEntries.DATASET_RELEASE_DATE] if CatalogueEntries.DATASET_RELEASE_DATE in res else None
            dataset_entry["dataset_info"][CatalogueEntries.DATASET_LAST_UPDATE_DATE] = res[CatalogueEntries.DATASET_LAST_UPDATE_DATE] if CatalogueEntries.DATASET_LAST_UPDATE_DATE in res else None
            dataset_entry["dataset_info"][CatalogueEntries.DATASET_VERSION_NOTES] = res[CatalogueEntries.DATASET_VERSION_NOTES] if CatalogueEntries.DATASET_VERSION_NOTES in res else None
            dataset_entry["dataset_info"][CatalogueEntries.DATASET_LICENSE] = res[CatalogueEntries.DATASET_LICENSE] if CatalogueEntries.DATASET_LICENSE in res else None
            dataset_entry["dataset_profile"][CatalogueEntries.DS_PROFILE_DESCRIPTION] = res[CatalogueEntries.DS_PROFILE_DESCRIPTION] if CatalogueEntries.DS_PROFILE_DESCRIPTION in res else None
            dataset_entry["dataset_profile"][CatalogueEntries.DS_PROFILE_THEME] = res[CatalogueEntries.DS_PROFILE_THEME] if CatalogueEntries.DS_PROFILE_THEME in res else None
            dataset_entry["dataset_profile"][CatalogueEntries.DS_PROFILE_FILE_TYPE] = res[CatalogueEntries.DS_PROFILE_FILE_TYPE] if CatalogueEntries.DS_PROFILE_FILE_TYPE in res else None
            dataset_entry["dataset_profile"][CatalogueEntries.DS_PROFILE_SIZE] = res[CatalogueEntries.DS_PROFILE_SIZE] if CatalogueEntries.DS_PROFILE_SIZE in res else None
            dataset_entry["dataset_profile"][CatalogueEntries.DS_PROFILE_NB_TUPLES] = res[CatalogueEntries.DS_PROFILE_NB_TUPLES] if CatalogueEntries.DS_PROFILE_NB_TUPLES in res else None
            dataset_entry["dataset_profile"][CatalogueEntries.DS_PROFILE_TUPLE_COMPLETENESS] = res[CatalogueEntries.DS_PROFILE_TUPLE_COMPLETENESS] if CatalogueEntries.DS_PROFILE_TUPLE_COMPLETENESS in res else None
            dataset_entry["dataset_profile"][CatalogueEntries.DS_PROFILE_TUPLE_UNIQUENESS] = res[CatalogueEntries.DS_PROFILE_TUPLE_UNIQUENESS] if CatalogueEntries.DS_PROFILE_TUPLE_UNIQUENESS in res else None
            return dataset_entry
        # else: no dataset has been found: nothing to do

    def set_dataset_features(self, dataset_entry: dict, dataset_gid: str) -> dict:
        log.info(dataset_gid)
        dataset_entry["features"] = []

        # 1. get the IDs of the features in the dataset
        # there is a _id due to the nesting of the instantiate and dataset field due to the group by
        cursor = self.database.find_operation(table_name=TableNames.VIEW_FEATURES_DATASET, filter_dict={"_id.dataset": dataset_gid}, projection={})
        features_ids = [res["_id"]["instantiates"] for res in cursor]
        log.info(features_ids)

        # for each feature, compute its information (profile, counts)
        cursor = self.database.find_operation(table_name=TableNames.FEATURE, filter_dict={"identifier": {"$in": features_ids}}, projection={})
        for feature in cursor:
            log.info(feature)
            feature_id = feature["identifier"]

            # compute the feature ontology code url and its label
            if "ontology_resource" in feature and "system" in feature["ontology_resource"] and "code" in feature["ontology_resource"] and "label" in feature["ontology_resource"]:
                feature_code = f"{feature["ontology_resource"]["system"]}/{feature["ontology_resource"]["code"]}"
                feature_label = feature["ontology_resource"]["label"]
            elif "ontology_resource" in feature and "system" in feature["ontology_resource"] and "code" in feature["ontology_resource"] :
                feature_code = f"{feature["ontology_resource"]["system"]}/{feature["ontology_resource"]["code"]}"
                feature_label = None
            else:
                feature_code = None
                feature_label = None

            # compute the feature aggregation type, based on the feature datatype
            datatype = feature["datatype"] if "datatype" in feature else None
            if datatype in [DataTypes.CATEGORY, DataTypes.BOOLEAN, DataTypes.REGEX, DataTypes.STRING]:
                agg_type = AggregationTypes.CATEGORICAL
            elif datatype in [DataTypes.DATE, DataTypes.DATETIME]:
                agg_type = AggregationTypes.DATE
            else:
                agg_type = AggregationTypes.CONTINUOUS

            # compute the feature domain
            feature_domain = {}
            if agg_type == AggregationTypes.CONTINUOUS:
                feature_domain[CatalogueEntries.DOMAIN_NUM_MIN] = feature[CatalogueEntries.DOMAIN_NUM_MIN] if CatalogueEntries.DOMAIN_NUM_MIN in feature else None
                feature_domain[CatalogueEntries.DOMAIN_NUM_MAX] = feature[CatalogueEntries.DOMAIN_NUM_MAX] if CatalogueEntries.DOMAIN_NUM_MAX in feature else None
            elif agg_type == AggregationTypes.CATEGORICAL:
                feature_domain[CatalogueEntries.DOMAIN_CAT_ACCEPTED] = feature[CatalogueEntries.DOMAIN_CAT_ACCEPTED] if CatalogueEntries.DOMAIN_CAT_ACCEPTED in feature else None
            elif agg_type == AggregationTypes.DATE:
                feature_domain[CatalogueEntries.DOMAIN_DATE_MIN] = feature[CatalogueEntries.DOMAIN_DATE_MIN] if CatalogueEntries.DOMAIN_DATE_MIN in feature else None
                feature_domain[CatalogueEntries.DOMAIN_DATE_MAX] = feature[CatalogueEntries.DOMAIN_DATE_MAX] if CatalogueEntries.DOMAIN_DATE_MAX in feature else None
            else:
                log.error(f"The aggregation type {agg_type} is unknown.")

            # compute the feature profile
            feature_profile = {}
            feature_profile[CatalogueEntries.F_PROFILE_ENTROPY] = feature[CatalogueEntries.F_PROFILE_ENTROPY] if CatalogueEntries.F_PROFILE_ENTROPY in feature else None
            feature_profile[CatalogueEntries.F_PROFILE_DENSITY] = feature[CatalogueEntries.F_PROFILE_DENSITY] if CatalogueEntries.F_PROFILE_DENSITY in feature else None
            feature_profile[CatalogueEntries.F_PROFILE_MAP_VALUE_COUNTS] = feature[CatalogueEntries.F_PROFILE_MAP_VALUE_COUNTS] if CatalogueEntries.F_PROFILE_MAP_VALUE_COUNTS in feature else None
            feature_profile[CatalogueEntries.F_PROFILE_MISSING_PERC] = feature[CatalogueEntries.F_PROFILE_MISSING_PERC] if CatalogueEntries.F_PROFILE_MISSING_PERC in feature else None
            feature_profile[CatalogueEntries.F_PROFILE_DT_VALIDITY] = feature[CatalogueEntries.F_PROFILE_DT_VALIDITY] if CatalogueEntries.F_PROFILE_DT_VALIDITY in feature else None
            feature_profile[CatalogueEntries.F_PROFILE_UNIQUENESS] = feature[CatalogueEntries.F_PROFILE_UNIQUENESS] if CatalogueEntries.F_PROFILE_UNIQUENESS in feature else None
            feature_profile[CatalogueEntries.F_PROFILE_ACCURACY_SCORE] = feature[CatalogueEntries.F_PROFILE_ACCURACY_SCORE] if CatalogueEntries.F_PROFILE_ACCURACY_SCORE in feature else None

            if agg_type == AggregationTypes.CONTINUOUS:
                feature_profile[CatalogueEntries.F_NUM_PROFILE_MIN] = feature[CatalogueEntries.F_NUM_PROFILE_MIN] if CatalogueEntries.F_NUM_PROFILE_MIN in feature else None
                feature_profile[CatalogueEntries.F_NUM_PROFILE_MAX] = feature[CatalogueEntries.F_NUM_PROFILE_MAX] if CatalogueEntries.F_NUM_PROFILE_MAX in feature else None
                feature_profile[CatalogueEntries.F_NUM_PROFILE_MEAN] = feature[CatalogueEntries.F_NUM_PROFILE_MEAN] if CatalogueEntries.F_NUM_PROFILE_MEAN in feature else None
                feature_profile[CatalogueEntries.F_NUM_PROFILE_MEDIAN] = feature[CatalogueEntries.F_NUM_PROFILE_MEDIAN] if CatalogueEntries.F_NUM_PROFILE_MEDIAN in feature else None
                feature_profile[CatalogueEntries.F_NUM_PROFILE_STD_DEV] = feature[CatalogueEntries.F_NUM_PROFILE_STD_DEV] if CatalogueEntries.F_NUM_PROFILE_STD_DEV in feature else None
                feature_profile[CatalogueEntries.F_NUM_PROFILE_SKEWNESS] = feature[CatalogueEntries.F_NUM_PROFILE_SKEWNESS] if CatalogueEntries.F_NUM_PROFILE_SKEWNESS in feature else None
                feature_profile[CatalogueEntries.F_NUM_PROFILE_KURTOSIS] = feature[CatalogueEntries.F_NUM_PROFILE_KURTOSIS] if CatalogueEntries.F_NUM_PROFILE_KURTOSIS in feature else None
                feature_profile[CatalogueEntries.F_NUM_PROFILE_MED_ABS_DEV] = feature[CatalogueEntries.F_NUM_PROFILE_MED_ABS_DEV] if CatalogueEntries.F_NUM_PROFILE_MED_ABS_DEV in feature else None
                feature_profile[CatalogueEntries.F_NUM_PROFILE_INTER_QU_RANGE] = feature[CatalogueEntries.F_NUM_PROFILE_INTER_QU_RANGE] if CatalogueEntries.F_NUM_PROFILE_INTER_QU_RANGE in feature else None
                feature_profile[CatalogueEntries.F_NUM_PROFILE_CORRELATION] = feature[CatalogueEntries.F_NUM_PROFILE_CORRELATION] if CatalogueEntries.F_NUM_PROFILE_CORRELATION in feature else None
            elif agg_type == AggregationTypes.DATE:
                feature_profile[CatalogueEntries.F_NUM_PROFILE_MIN] = feature[CatalogueEntries.F_NUM_PROFILE_MIN] if CatalogueEntries.F_NUM_PROFILE_MIN in feature else None
                feature_profile[CatalogueEntries.F_NUM_PROFILE_MAX] = feature[CatalogueEntries.F_NUM_PROFILE_MAX] if CatalogueEntries.F_NUM_PROFILE_MAX in feature else None
                feature_profile[CatalogueEntries.F_NUM_PROFILE_MEDIAN] = feature[CatalogueEntries.F_NUM_PROFILE_MEDIAN] if CatalogueEntries.F_NUM_PROFILE_MEDIAN in feature else None
                feature_profile[CatalogueEntries.F_NUM_PROFILE_INTER_QU_RANGE] = feature[CatalogueEntries.F_NUM_PROFILE_INTER_QU_RANGE] if CatalogueEntries.F_NUM_PROFILE_INTER_QU_RANGE in feature else None
            elif agg_type == AggregationTypes.CATEGORICAL:
                feature_profile[CatalogueEntries.F_CAT_PROFILE_IMBALANCE] = feature[CatalogueEntries.F_CAT_PROFILE_IMBALANCE] if CatalogueEntries.F_CAT_PROFILE_IMBALANCE in feature else None
                feature_profile[CatalogueEntries.F_CAT_PROFILE_CONSTANCY] = feature[CatalogueEntries.F_CAT_PROFILE_CONSTANCY] if CatalogueEntries.F_CAT_PROFILE_CONSTANCY in feature else None
                feature_profile[CatalogueEntries.F_CAT_PROFILE_MODE] = feature[CatalogueEntries.F_CAT_PROFILE_MODE] if CatalogueEntries.F_CAT_PROFILE_MODE in feature else None

            # store all the information in the JSON dict of the current feature
            feature_info = {
                CatalogueEntries.FEATURE_ID: feature_id,
                CatalogueEntries.FEATURE_NAME: feature[CatalogueEntries.FEATURE_NAME] if CatalogueEntries.FEATURE_NAME in feature else None,
                CatalogueEntries.FEATURE_DESCRIPTION: feature[CatalogueEntries.FEATURE_DESCRIPTION] if CatalogueEntries.FEATURE_DESCRIPTION in feature else None,
                CatalogueEntries.FEATURE_ONTOLOGY_CODE: feature_code,
                CatalogueEntries.FEATURE_ONTOLOGY_LABEL: feature_label,
                CatalogueEntries.FEATURE_DATA_TYPE: datatype,
                CatalogueEntries.FEATURE_VISIBILITY: feature[CatalogueEntries.FEATURE_VISIBILITY] if CatalogueEntries.FEATURE_VISIBILITY in feature else None,
                CatalogueEntries.FEATURE_ENTITY_TYPE: feature[CatalogueEntries.FEATURE_ENTITY_TYPE] if CatalogueEntries.FEATURE_ENTITY_TYPE in feature else None,
                CatalogueEntries.FEATURE_AGG_TYPE: agg_type,
                "domain": feature_domain,
                "profile": feature_profile
            }


            # compute feature statistics
            log.info("compute stats")
            statistics = self.compute_stats(dataset_gid=dataset_gid, feature_id=feature_id, feature_name=original_name)
            feature_info["profile"][CatalogueEntries.F_PROFILE_MISSING_PERC] = None

            # compute feature aggregated data
            log.info("compute aggregated data")
            feature_info["profile"] = {}
            feature_info["profile"][CatalogueEntries.F_PROFILE_MAP_VALUE_COUNTS] = self.compute_values_and_their_counts(dataset_gid=dataset_gid, feature_id=feature_id)
            if agg_type == AggregationTypes.CATEGORICAL:
                # TODO: imbalance, constancy, mode
                pass
            elif agg_type == AggregationTypes.CONTINUOUS:
                # get min, max, avg
                # TODO: median, std dev, skewness, kurtosis, median abs. dev, interquartile range, correlation matrix
                aggregated_min_max_avg = self.compute_min_max_avg(dataset_gid=dataset_gid, feature_id=feature_id)
                feature_info["profile"][CatalogueEntries.F_NUM_PROFILE_MIN] = aggregated_min_max_avg["min"]
                feature_info["profile"][CatalogueEntries.F_NUM_PROFILE_MAX] = aggregated_min_max_avg["max"]
                feature_info["profile"][CatalogueEntries.F_NUM_PROFILE_MEAN] = aggregated_min_max_avg["avg"]
            elif agg_type == AggregationTypes.DATE:
                # get min and max
                # TODO: interquartile range
                aggregated_min_max_avg = self.compute_min_max_avg(dataset_gid=dataset_gid, feature_id=feature_id)
                feature_info["profile"][CatalogueEntries.F_DATE_PROFILE_MIN] = aggregated_min_max_avg["min"]
                feature_info["profile"][CatalogueEntries.F_DATE_PROFILE_MAX] = aggregated_min_max_avg["max"]
            else:
                log.error(f"Unrecognised aggregation type {agg_type} for feature {feature_id}.")
            dataset_entry["features"].append(feature_info)

        log.info(dataset_entry)
        return dataset_entry

    def compute_nb_patients_for_dataset(self, dataset_gid: str) -> int:
        # computes the number of patients used by this dataset
        where_dataset_gid = Operators.match(field="dataset", value=dataset_gid, is_regex=False)
        get_subject = Operators.project(field="has_subject", projected_value=None)
        operations = [where_dataset_gid, get_subject]
        operations.append(Operators.group_by(group_key="$has_subject", groups=[]))  # to simulate distinct because we cannot combine distinct and an aggregation pipeline
        operations.append(Operators.group_by(group_key="$has_subject", groups=[{"name": "p_count", "operator": "$sum", "field": 1}]))  # to simulate count
        log.info(operations)
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
        cursor = self.database.find_operation(table_name=TableNames.STATS_QUALITY, filter_dict={}, projection={"unknown_categorical_values": 1})
        unknown_categorical_values = {}
        for res in cursor:
            if "unknown_categorical_values" in res and feature_name in res["unknown_categorical_values"]:
                # there are some unknown categorical values for this feature
                # that have been reported while computing quality stats
                unknown_categorical_values = res["unknown_categorical_values"][feature_name]
            else:
                unknown_categorical_values = {}
        return {"record_count": rec_count, "perc_empty_cells": percentage_empty_cells,  "unknown_categorical_values": unknown_categorical_values}

    def compute_values_and_their_counts(self, dataset_gid: str, feature_id: str) -> dict:
        # returns two values: the values and their counts
        operations = []
        # for this, we can have "simple" values, i.e., str, int, bool, or "complex" values such as objects.
        # Objects correspond to OntologyResource, thus we need to unnest it to get their label
        # it seems that we cannot do it in a single query
        # therefore, I get the first simple values, then complex values that I un-nest, and then union these two
        match_on_instantiate = Operators.match(field="instantiates", value=feature_id, is_regex=False)
        match_on_ds = Operators.match(field="dataset", value=dataset_gid, is_regex=False)
        group_by_value = Operators.group_by(group_key="$value", groups=[
            {"name": "counts", "operator": "$sum", "field": 1},  # {"$group" : {_id:"$province", count:{$sum:1}}}
        ])
        operations.append(match_on_instantiate)
        operations.append(match_on_ds)
        operations.append(Operators.match(field="value", value={"$not": {"$type": "object"}}, is_regex=False))  # {value:{$not:{$type:'object'}}}
        operations.append(Operators.project(field="value", projected_value=None))
        operations.append(group_by_value)
        log.info(Operators.equality(field="$value.label", value=""))
        log.info(Operators.concat(["$value.system", "/", "$value.code"]))
        log.info(Operators.if_condition(cond=Operators.equality(field="$value.label", value=""), if_part=Operators.concat(["$value.system", "/", "$value.code"]), else_part="$value.label"))
        log.info(Operators.project(Operators.if_condition(cond=Operators.equality(field="$value.label", value=""), if_part=Operators.concat(["$value.system", "/", "$value.code"]), else_part="$value.label"), projected_value="value"))
        # {'value': {'$cond': {'if': {'$eq': ['$value.label', '']}, 'then': {'$concat': ['$value.system', '/', '$value.code']}, 'else': '$value.label'}}, '_id': 0}
        operations.append(Operators.union(second_table_name=TableNames.RECORD, second_pipeline=[
            match_on_instantiate,
            match_on_ds,
            Operators.match(field="value", value={"$type": "object"}, is_regex=False),
            # get the text of the codeable concept (the base is _id and not value because of the join)
            # if the label is empty, which may happen when the API call failed, we get the url to the concept (at least)
            # "field1": {"$ifNull": ["$field1", "$field2"]}
            # I use the above dict instead of field="value.label" to cover the case when no label has been retrieved for the categorical value
            # {
            #     "$cond": {
            #         "if": { $eq: [ { "$ifNull": [ "$field.value", "" ] }, "" ] },
            #         "then": "No Data",
            #         "else": "$field.value"
            #     }
            # }
            Operators.project(field=Operators.if_condition(cond=Operators.equality(field="$value.label", value=""), if_part=Operators.concat(["$value.system", "/", "$value.code"]), else_part="$value.label"), projected_value="value"),
            group_by_value
        ]))
        log.info(operations)
        cursor = self.database.db[TableNames.RECORD].aggregate(operations)
        values_and_counts = {res["_id"]: res["counts"] for res in cursor}
        return values_and_counts

    def compute_min_max_avg(self, dataset_gid: str, feature_id: str) -> dict:
        # db.MyCollection.aggregate([
        #    { "$group": {
        #       "_id": null,
        #       "max_val": { "$max": "$value" },
        #       "min_val": { "$min": "$value" }
        #       "avg_val": { "$avg": "$value" }
        #    }}
        # ]);
        or_on_types = Operators.or_operator([
            {"value": {"$type": "long"}},
            {"value": {"$type": "double"}},
            {"value": {"$type": "decimal"}},
            {"value": {"$type": "int"}},
            {"value": {"$type": "bool"}},
            {"value": {"$type": "timestamp"}},
            {"value": {"$type": "date"}}
        ])
        operations = []
        operations.append(Operators.match(field="instantiates", value=feature_id, is_regex=False))
        operations.append(Operators.match(field="dataset", value=dataset_gid, is_regex=False))
        # keep only int/float values (it may happen if there are some numeric values and one is "no information")
        operations.append(Operators.match(field=None, value=or_on_types, is_regex=False))
        operations.append(Operators.group_by(group_key=None, groups=[
            {"name": "max_val", "operator": "$max", "field": "$value"},
            {"name": "min_val", "operator": "$min", "field": "$value"},
            {"name": "avg_val", "operator": "$avg", "field": "$value"}
        ]))
        log.info(operations)
        cursor = self.database.db[TableNames.RECORD].aggregate(operations)
        for res in cursor:
            log.info(res)
            # there is only one result because we get the info for a single feature
            # however, if some feature has no data, then this result is empty, so we need to use a for loop
            # instead of a single cursor.next (which fails if no data is returned)
            return {"min": res["min_val"], "max": res["max_val"], "avg": res["avg_val"]}
        # no info for this feature
        return {"min": None, "max": None, "avg": None}
