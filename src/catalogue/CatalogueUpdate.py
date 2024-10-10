import datetime
import json

from database.Database import Database
from datatypes.ResourceIdentifier import ResourceIdentifier
from enums.AggregationTypes import AggregationTypes
from enums.DataTypes import DataTypes
from enums.TableNames import TableNames
from query.Operators import Operators
from utils.setup_logger import log
from enums.CatalogueEntries import CatalogueEntries


class CatalogueUpdate:
    def __init__(self, db: Database):
        self.db = db
        self.catalogue_data = {}

    def compute_data_for_catalogue(self) -> None:
        # TODO NELLY: compute agg_type
        datasets = self.get_datasets()
        log.info(datasets)
        for dataset_profile in datasets:
            log.info(dataset_profile)
            log.info(type(dataset_profile))
            self.catalogue_data[dataset_profile] = {}
            self.catalogue_data[dataset_profile]["description"] = "This is a dataset"
            self.catalogue_data[dataset_profile]["nb_patients"] = self.compute_nb_patients_for_dataset(dataset_name=dataset_profile)
            self.catalogue_data[dataset_profile]["timestamp"] = datetime.datetime.now()
            self.catalogue_data[dataset_profile]["features"] = []
            for feature_id_type_tuple in self.get_feature_ids_in_dataset(dataset_name=dataset_profile):
                log.info(feature_id_type_tuple)
                feature_json_data = self.compute_json_data_for_one_feature(dataset_name=dataset_profile, feature_id=feature_id_type_tuple[0], feature_entity=feature_id_type_tuple[1], nb_patients=self.catalogue_data[dataset_profile]["nb_patients"])
                self.catalogue_data[dataset_profile]["features"].append(feature_json_data)
        log.info(json.dumps(self.catalogue_data, default=str))  # default=str for converting datetime objects to strings

    def get_datasets(self) -> list[dict]:
        distinct_dataset_names = []
        for record_table_name in TableNames.records(db=self.db):
            cursor = self.db.db[record_table_name].distinct("dataset_name")
            for dataset_name in cursor:
                if dataset_name not in distinct_dataset_names:
                    distinct_dataset_names.append(dataset_name)
        return distinct_dataset_names

    def get_feature_ids_in_dataset(self, dataset_name: str) -> list:
        where_dataset_name = Operators.match(field="dataset_name", value=dataset_name, is_regex=False)
        get_instantiate = Operators.project(field="instantiate", projected_value=None)
        operations = []
        operations.append(where_dataset_name)
        operations.append(get_instantiate)
        for record_table_name in TableNames.records(db=self.db):
            if record_table_name != TableNames.LABORATORY_RECORD:  # this is the base of the union
                operations.append(Operators.union(second_table_name=record_table_name, second_pipeline=[where_dataset_name, get_instantiate]))
        operations.append(Operators.group_by(group_key="$instantiate", groups=[]))  # to simulate distinct because we cannot combine distinct and an aggregation pipeline
        log.info(operations)
        cursor = self.db.db[TableNames.LABORATORY_RECORD].aggregate(operations)
        # _id contains the Feature id (and is not the MongoDB id in this particular case,
        # and this is because group_by used the field _id to know on which field to do the group by)
        return [(result["_id"], ResourceIdentifier.get_type(identifier=result["_id"])) for result in cursor]

    def compute_nb_patients_for_dataset(self, dataset_name: str) -> int:
        # computes the number of patients used by this dataset
        where_dataset_name = Operators.match(field="dataset_name", value=dataset_name, is_regex=False)
        get_subject = Operators.project(field="subject", projected_value=None)
        operations = []
        operations.append(where_dataset_name)
        operations.append(get_subject)
        for record_table_name in TableNames.records(db=self.db):
            if record_table_name != TableNames.LABORATORY_RECORD:  # this is the base of the union
                operations.append(Operators.union(second_table_name=record_table_name, second_pipeline=[where_dataset_name, get_subject]))
        operations.append(Operators.group_by(group_key="$subject", groups=[]))  # to simulate distinct because we cannot combine distinct and an aggregation pipeline
        operations.append(Operators.group_by(group_key="$subject", groups=[{"name": "p_count", "operator": "$sum", "field": 1}]))  # to simulate count
        log.info(operations)
        cursor = self.db.db[TableNames.LABORATORY_RECORD].aggregate(operations)
        for result in cursor:
            return result["p_count"]

    def compute_json_data_for_one_feature(self, dataset_name: str, feature_id: str, feature_entity: str, nb_patients: int) -> dict:
        feature_info = {
            "feature": {},
            "statistics": {},
            "aggregated_data": {}
        }

        # compute feature information
        operations = []
        operations.append(Operators.match(field="identifier", value=feature_id, is_regex=False))
        operations.append(Operators.project(field=["ontology_resource.system", "ontology_resource.code", "ontology_resource.label", "permitted_datatype"], projected_value=None))
        log.info(operations)
        log.info(feature_entity)
        cursor = self.db.db[feature_entity].aggregate(operations)
        agg_type = AggregationTypes.CONTINUOUS
        for feature in cursor:
            # there is only one result because we get the info for a single feature
            # but if there is no data for this feature, cursor.next() will throw an error, thus using a for loop
            log.info(feature["ontology_resource"]["label"])
            # this allows us to compute the aggregation type, based on the datatype
            datatype = feature["permitted_datatype"]
            feature_label = feature["ontology_resource"]["label"]
            if datatype in [DataTypes.CATEGORY, DataTypes.BOOLEAN, DataTypes.REGEX, DataTypes.STRING]:
                agg_type = AggregationTypes.CATEGORICAL
            elif datatype in [DataTypes.DATE, DataTypes.DATETIME]:
                agg_type = AggregationTypes.DATE
            feature_info["feature"] = {
                CatalogueEntries.FEATURE_ID: feature_id,
                CatalogueEntries.FEATURE_NAME: feature_label,
                CatalogueEntries.FEATURE_CODES: f"{feature["ontology_resource"]["system"]}/{feature["ontology_resource"]["code"]}",
                CatalogueEntries.FEATURE_TYPE: datatype,
                CatalogueEntries.FEATURE_AGG_TYPE: agg_type,
                CatalogueEntries.FEATURE_ENTITY: feature_entity
            }

        # compute feature statistics
        log.info("compute stats")
        statistics = self.compute_stats(dataset_name=dataset_name, feature_id=feature_id, feature_entity=feature_entity, feature_label=feature_label, nb_patients=nb_patients)
        log.info(statistics)
        feature_info["statistics"] = {}
        feature_info["statistics"][CatalogueEntries.STATS_REC_COUNT] = statistics["record_count"]
        feature_info["statistics"][CatalogueEntries.STATS_PERC_EMPTY_CELLS] = statistics["perc_empty_cells"]
        feature_info["statistics"][CatalogueEntries.UNCATEGORIZED_CATEGORIES] = statistics["unknown_categorical_values"]
        log.info(feature_info["statistics"])

        # compute feature aggregated data
        log.info("compute aggregated data")
        feature_info["aggregated_data"] = {}
        if agg_type == AggregationTypes.CATEGORICAL:
            # get values and their counts
            aggregated_values_and_counts = self.compute_values_and_their_counts(dataset_name=dataset_name, feature_entity=feature_entity, feature_id=feature_id)
            feature_info["aggregated_data"][CatalogueEntries.AGG_VALUES] = aggregated_values_and_counts["values"]
            feature_info["aggregated_data"][CatalogueEntries.AGG_COUNTS] = aggregated_values_and_counts["counts"]
        elif agg_type == AggregationTypes.CONTINUOUS:
            # get values and their counts
            aggregated_values_and_counts = self.compute_values_and_their_counts(dataset_name=dataset_name, feature_entity=feature_entity, feature_id=feature_id)
            feature_info["aggregated_data"][CatalogueEntries.AGG_VALUES] = aggregated_values_and_counts["values"]
            feature_info["aggregated_data"][CatalogueEntries.AGG_COUNTS] = aggregated_values_and_counts["counts"]
            # get min, max, avg
            aggregated_min_max_avg = self.compute_min_max_avg(dataset_name=dataset_name, feature_entity=feature_entity, feature_id=feature_id)
            feature_info["aggregated_data"][CatalogueEntries.AGG_MIN] = aggregated_min_max_avg["min"]
            feature_info["aggregated_data"][CatalogueEntries.AGG_MAX] = aggregated_min_max_avg["max"]
            feature_info["aggregated_data"][CatalogueEntries.AGG_AVG] = aggregated_min_max_avg["avg"]
        elif agg_type == AggregationTypes.DATE:
            # get values and their counts
            aggregated_values_and_counts = self.compute_values_and_their_counts(dataset_name=dataset_name, feature_entity=feature_entity, feature_id=feature_id)
            feature_info["aggregated_data"][CatalogueEntries.AGG_VALUES] = aggregated_values_and_counts["values"]
            feature_info["aggregated_data"][CatalogueEntries.AGG_COUNTS] = aggregated_values_and_counts["counts"]
            # get min and max
            aggregated_min_max_avg = self.compute_min_max_avg(dataset_name=dataset_name, feature_entity=feature_entity, feature_id=feature_id)
            feature_info["aggregated_data"][CatalogueEntries.AGG_MIN] = aggregated_min_max_avg["min"]
            feature_info["aggregated_data"][CatalogueEntries.AGG_MAX] = aggregated_min_max_avg["max"]
        else:
            log.error(f"Unrecognised aggregation type {feature_entity} for feature {feature_id}.")

        log.info(feature_info)
        return feature_info

    def compute_stats(self, dataset_name: str, feature_entity: str, feature_id: str, feature_label: str, nb_patients: int) -> dict:
        record_table = TableNames.get_record_table_from_feature_table(feature_table_name=feature_entity)
        rec_count = self.db.count_documents(table_name=record_table, filter_dict={"dataset_name": dataset_name, "instantiate": feature_id})
        # cursor = self.db.find_operation(table_name=TableNames.STATS_QUALITY, filter_dict={}, projection={"columns_unmatched_typeof_etl_types": 1})
        # perc_empty_cells = 0.0
        # for res in cursor:
        #     # there is only one result
        #     # columns_unmatched_typeof_etl_types: { affected: { typeof_type: 'str', etl_type: 'bool' }, ... }
        #     perc_empty_cells = res[""]
        # perc_empty_cells = perc_empty_cells / rec_count
        cursor = self.db.find_operation(table_name=TableNames.STATS_QUALITY, filter_dict={}, projection={"unknown_categorical_values": 1})
        unknown_categorical_values = {}
        log.info(feature_label)
        for res in cursor:
            log.info(res)
            if "unknown_categorical_values" in res and feature_label in res["unknown_categorical_values"]:
                # there are some unknown categorical values for this feature
                # that have been reported while computing quality stats
                unknown_categorical_values = res["unknown_categorical_values"][feature_label]
            else:
                unknown_categorical_values = {}
        return {"record_count": rec_count, "perc_empty_cells": round((1-(rec_count/float(nb_patients)))*100, 3), "unknown_categorical_values": unknown_categorical_values}

    def compute_values_and_their_counts(self, dataset_name: str, feature_entity: str, feature_id: str) -> dict:
        # returns two values: the values and their counts
        record_table = TableNames.get_record_table_from_feature_table(feature_table_name=feature_entity)
        operations = []
        # for this, we can have "simple" values, i.e., str, int, bool, or "complex" values such as objects.
        # Objects correspond to OntologyResource, thus we need to unnest it to get their label
        # it seems that we cannot do it in a single query
        # therefore, I get the first simple values, then complex values that I un-nest, and then union these two
        match_on_instantiate = Operators.match(field="instantiate", value=feature_id, is_regex=False)
        match_on_ds = Operators.match(field="dataset_name", value=dataset_name, is_regex=False)
        group_by_value = Operators.group_by(group_key="$value", groups=[
            {"name": "counts", "operator": "$sum", "field": 1},  # {"$group" : {_id:"$province", count:{$sum:1}}}
        ])
        operations.append(match_on_instantiate)
        operations.append(match_on_ds)
        operations.append(Operators.match(field="value", value={"$not": {"$type": "object"}}, is_regex=False))  # {value:{$not:{$type:'object'}}}
        operations.append(Operators.project(field="value", projected_value=None))
        operations.append(group_by_value)
        operations.append(Operators.union(second_table_name=record_table, second_pipeline=[
            match_on_instantiate,
            match_on_ds,
            Operators.match(field="value", value={"$type": "object"}, is_regex=False),
            Operators.project(field="value.label", projected_value="value"),  # get the text of the codeable concept (the base is _id and not value because of the join)
            group_by_value
        ]))
        log.info(operations)
        cursor = self.db.db[record_table].aggregate(operations)
        values = []
        counts = []
        for res in cursor:
            # each res is a pair of a value and its count
            values.append(res["_id"])  # the value
            counts.append(res["counts"])  # and its count
        return {"values": values, "counts": counts}

    def compute_min_max_avg(self, dataset_name: str, feature_entity: str, feature_id: str) -> dict:
        # db.MyCollection.aggregate([
        #    { "$group": {
        #       "_id": null,
        #       "max_val": { "$max": "$value" },
        #       "min_val": { "$min": "$value" }
        #       "avg_val": { "$avg": "$value" }
        #    }}
        # ]);
        record_table = TableNames.get_record_table_from_feature_table(feature_table_name=feature_entity)
        log.info(record_table)
        operations = []
        operations.append(Operators.match(field="instantiate", value=feature_id, is_regex=False))
        operations.append(Operators.match(field="dataset_name", value=dataset_name, is_regex=False))
        operations.append(Operators.group_by(group_key=None, groups=[
            {"name": "max_val", "operator": "$max", "field": "$value"},
            {"name": "min_val", "operator": "$min", "field": "$value"},
            {"name": "avg_val", "operator": "$avg", "field": "$value"}
        ]))
        log.info(operations)
        cursor = self.db.db[record_table].aggregate(operations)
        for res in cursor:
            log.info(res)
            # there is only one result because we get the info for a single feature
            # however, if some feature has no data, then this result is empty, so we need to use a for loop
            # instead of a single cursor.next (which fails if no data is returned)
            return {"min": res["min_val"], "max": res["max_val"], "avg": res["avg_val"]}
        # no info for this feature
        return {"min": None, "max": None, "avg": None}