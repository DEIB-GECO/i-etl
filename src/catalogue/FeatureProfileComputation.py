import copy
import dataclasses
import json

import numpy as np

from constants.defaults import DEFAULT_NAN_VALUE, PRINT_QUERIES
from constants.methods import factory
from database.Database import Database
from database.Operators import Operators
from entities.Feature import Feature
from entities.Record import Record
from entities.Resource import Resource
from enums.DataTypes import DataTypes
from enums.Profile import Profile
from enums.TableNames import TableNames
from utils.setup_logger import log


@dataclasses.dataclass(kw_only=True)
class FeatureProfileComputation:
    database: Database

    def __post_init__(self):
        # store, for each dataset, the total number of patients to compute the missing percentage
        operations = [
            Operators.match(field=None,
                            value={Resource.ENTITY_TYPE_: {"$ne": f"{Profile.CLINICAL}{TableNames.RECORD}"}},
                            is_regex=False),
            Operators.group_by(group_key={Record.DATASET_: Record.DATASET__, Record.SUBJECT_: Record.SUBJECT__},
                               groups=[{"name": "nb_patients", "operator": "$sum", "field": 1}]),
            Operators.group_by(group_key={Record.DATASET_: Record.DATASET___},
                               groups=[{"name": "distinct_nb_patients", "operator": "$sum", "field": 1}]),
            Operators.write_to_table(table_name=TableNames.COUNTS_PATIENTS)
        ]
        if PRINT_QUERIES:
            log.info(operations)
        _ = self.database.db[TableNames.RECORD].aggregate(operations)

        # store, for each clinical dataset, the total number of samples to compute the missing percentage for clinical data
        operations = [
            Operators.match(field=None,
                            value={Resource.ENTITY_TYPE_: {"$eq": f"{Profile.CLINICAL}{TableNames.RECORD}"}},
                            is_regex=False),
            Operators.group_by(group_key={Record.DATASET_: Record.DATASET__, Record.BASE_ID_: Record.BASE_ID__},
                               groups=[{"name": "nb_samples", "operator": "$sum", "field": 1}]),
            Operators.group_by(group_key={Record.DATASET_: Record.DATASET___},
                               groups=[{"name": "distinct_nb_samples", "operator": "$sum", "field": 1}]),
            Operators.write_to_table(table_name=TableNames.COUNTS_SAMPLES)
        ]
        if PRINT_QUERIES:
            log.info(operations)
        _ = self.database.db[TableNames.RECORD].aggregate(operations)

        # compute the list of Feature identifiers for each Profile data type (numeric, category, date)
        # the filter {"datasets": self.dataset_gid} means that the array "datasets" contains the element self.dataset_gid
        cursor = self.database.find_operation(table_name=TableNames.FEATURE, filter_dict={},
                                              projection={Resource.IDENTIFIER_: 1, Feature.DT_: 1, "_id": 0})
        map_feature_datatype = {}
        for element in cursor:
            if element[Feature.DT_] not in map_feature_datatype:
                map_feature_datatype[element[Feature.DT_]] = []
            map_feature_datatype[element[Feature.DT_]].append(element[Resource.IDENTIFIER_])
        # log.info(map_feature_datatype)

        self.numeric_features = []
        self.numeric_features.extend(
            map_feature_datatype[DataTypes.INTEGER] if DataTypes.INTEGER in map_feature_datatype else [])
        self.numeric_features.extend(
            map_feature_datatype[DataTypes.FLOAT] if DataTypes.FLOAT in map_feature_datatype else [])
        self.categorical_features = []
        self.categorical_features.extend(
            map_feature_datatype[DataTypes.STRING] if DataTypes.STRING in map_feature_datatype else [])
        self.categorical_features.extend(
            map_feature_datatype[DataTypes.BOOLEAN] if DataTypes.BOOLEAN in map_feature_datatype else [])
        self.categorical_features.extend(
            map_feature_datatype[DataTypes.CATEGORY] if DataTypes.CATEGORY in map_feature_datatype else [])
        self.date_features = []
        self.date_features.extend(
            map_feature_datatype[DataTypes.DATE] if DataTypes.DATE in map_feature_datatype else [])
        self.date_features.extend(
            map_feature_datatype[DataTypes.DATETIME] if DataTypes.DATETIME in map_feature_datatype else [])
        self.all_features = self.numeric_features + self.categorical_features + self.date_features

    def compute_features_profiles(self) -> None:
        # clear FeatureProfile table and create a unique index on <dataset, instantiates>
        # because merge requires a unique index on the merge keys
        self.database.drop_table(TableNames.FEATURE_PROFILE)
        self.database.create_unique_index(TableNames.FEATURE_PROFILE,
                                          columns={Record.DATASET_: 1, Record.INSTANTIATES_: 1})

        # ALL FEATURES
        operators = self.data_type_validity_query(features_ids=self.all_features)
        operators.extend(self.finalize_query(include_value=False))
        if PRINT_QUERIES:
            log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

        # NUMERIC FEATURES
        match_numeric_values = [Operators.match(field=None, value=Operators.or_operator([
            {Record.VALUE_: {"$type": "int"}},
            {Record.VALUE_: {"$type": "double"}},
            {Record.VALUE_: {"$type": "long"}},
            {Record.VALUE_: {"$type": "decimal"}}]), is_regex=False),
                                Operators.match(field=None, value={Record.VALUE_: {"$ne": np.nan}}, is_regex=False)]

        # 1. compute min, max, mean, median, std values for numerical features
        operators = copy.deepcopy(match_numeric_values)
        operators.extend(
            self.min_max_mean_median_std_query(features_ids=self.numeric_features, compute_min=True, compute_max=True,
                                               compute_mean=True, compute_median=True, compute_std=True))
        operators.extend(self.finalize_query(include_value=False))
        if PRINT_QUERIES:
            log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

        # 2. compute the Median Absolute Deviation
        operators = copy.deepcopy(match_numeric_values)
        operators.extend(self.abs_med_dev_query(features_ids=self.numeric_features))
        operators.extend(self.finalize_query(include_value=False))
        if PRINT_QUERIES:
            log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

        # 3. compute skewness and kurtosis for numerical features
        operators = copy.deepcopy(match_numeric_values)
        operators.extend(self.skewness_and_kurtosis_query(features_ids=self.numeric_features))
        operators.extend(self.finalize_query(include_value=False))
        if PRINT_QUERIES:
            log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

        # 4. compute IQR for numerical features
        operators = copy.deepcopy(match_numeric_values)
        operators.extend(self.iqr_query(features_ids=self.numeric_features))
        operators.extend(self.finalize_query(include_value=False))
        if PRINT_QUERIES:
            log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

        # 5. compute Pearson correlation coefficients
        operators = copy.deepcopy(match_numeric_values)
        operators.extend(self.pearson_correlation_query(features_ids=self.numeric_features, database=self.database))
        operators.extend(self.finalize_query(include_value=False))
        if PRINT_QUERIES:
            log.info(operators)
        self.database.db[TableNames.PAIRS_FEATURES].aggregate(operators)

        # DATE FEATURES
        match_date_values = [Operators.match(field=None, value=Operators.or_operator([
            {Record.VALUE_: {"$type": "date"}},
            {Record.VALUE_: {"$type": "timestamp"}}]), is_regex=False),
                             Operators.match(field=None, value={Record.VALUE_: {"$ne": np.nan}}, is_regex=False)]

        # 1. compute min, max, mean, median, std values for date features
        operators = copy.deepcopy(match_date_values)
        operators.extend(
            self.min_max_mean_median_std_query(features_ids=self.date_features, compute_min=True, compute_max=True,
                                               compute_mean=False, compute_median=True, compute_std=False))
        operators.extend(self.finalize_query(include_value=False))
        if PRINT_QUERIES:
            log.info(operators)
        # self.database.db[TableNames.RECORD].aggregate(operators)

        # 2. compute IQR for numerical features
        operators = copy.deepcopy(match_date_values)
        operators.extend(self.iqr_query(features_ids=self.date_features))
        operators.extend(self.finalize_query(include_value=False))
        if PRINT_QUERIES:
            log.info(operators)
        # self.database.db[TableNames.RECORD].aggregate(operators)

        # CATEGORICAL FEATURES
        match_categorical_values = [Operators.match(field=None, value=Operators.or_operator([
            {Record.VALUE_: {"$type": "object"}}]), is_regex=False),
                                    Operators.match(field=None, value={Record.VALUE_: {"$ne": np.nan}}, is_regex=False)]

        # 1. compute imbalance for categorical features
        operators = copy.deepcopy(match_categorical_values)
        operators.extend(self.imbalance_query(features_ids=self.categorical_features))
        operators.extend(self.finalize_query(include_value=False))
        if PRINT_QUERIES:
            log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

        # 2. compute constancy for categorical features
        operators = copy.deepcopy(match_categorical_values)
        operators.extend(self.constancy_query(features_ids=self.categorical_features))
        operators.extend(self.finalize_query(include_value=False))
        if PRINT_QUERIES:
            log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

        # 3. compute mode for categorical features
        operators = copy.deepcopy(match_categorical_values)
        operators.extend(self.mode_query(features_ids=self.categorical_features))
        operators.extend(self.finalize_query(include_value=False))
        if PRINT_QUERIES:
            log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

        # SHARED PROFILE FEATURES
        # uniqueness
        operators = self.uniqueness_query(features_ids=self.all_features)
        operators.extend(self.finalize_query(include_value=False))
        if PRINT_QUERIES:
            log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

        # entropy
        operators = self.entropy_query(features_ids=self.all_features)
        operators.extend(self.finalize_query(include_value=False))
        if PRINT_QUERIES:
            log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

        # density
        operators = self.density_query(features_ids=self.all_features)
        operators.extend(self.finalize_query(include_value=False))
        if PRINT_QUERIES:
            log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

        # values and counts
        operators = self.values_and_counts_query(features_ids=self.all_features)
        operators.extend(self.finalize_query(include_value=True))
        if PRINT_QUERIES:
            log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

        operators = self.missing_percentage_query_without_samples(features_ids=self.all_features)
        operators.extend(self.finalize_query(include_value=False))
        if PRINT_QUERIES:
            log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

        operators = self.missing_percentage_query_with_samples(features_ids=self.all_features)
        operators.extend(self.finalize_query(include_value=False))
        if PRINT_QUERIES:
            log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

    def min_max_mean_median_std_query(self, features_ids: list, compute_min: bool, compute_max: bool,
                                      compute_mean: bool, compute_median: bool, compute_std: bool) -> list:
        groups = []
        if compute_min:
            groups.append({"name": "min_value", "operator": "$min", "field": Record.VALUE__})
        if compute_max:
            groups.append({"name": "max_value", "operator": "$max", "field": Record.VALUE__})
        if compute_mean:
            groups.append({"name": "mean_value", "operator": "$avg", "field": Record.VALUE__})
        if compute_median:
            groups.append({"name": "median_value", "operator": "$median",
                           "field": {"input": Record.VALUE__, "method": "approximate"}})
        if compute_std:
            groups.append({"name": "std_value", "operator": "$stdDevPop", "field": Record.VALUE__})
        return [
            Operators.match(field=Record.INSTANTIATES_, value={"$in": features_ids}, is_regex=False),
            Operators.group_by(
                group_key={Record.DATASET_: Record.DATASET__, Record.INSTANTIATES_: Record.INSTANTIATES__},
                groups=groups),
        ]

    def abs_med_dev_query(self, features_ids: list) -> list:
        # i.e., EMA=median(|Xi-Y|) where Xi is a value, Y is the median, and L is the absolute value (no minus sign)
        # [{'$group': {'_id': {'_id': null}, 'originalValues': {'$push': '$value'}, 'mymedian': {'$median': {"input": "$value", "method": "approximate"}}}}, {'$unwind': '$originalValues'}, {'$project': {"absVal": {'$abs': {'$subtract': ['$originalValues', '$mymedian']}}}}, {'$group': {'_id': {'_id': null}, 'ema': {'$median': {"input": "$absVal", "method": "approximate"}}}}]
        return [
            Operators.match(field=Record.INSTANTIATES_, value={"$in": features_ids}, is_regex=False),
            Operators.group_by(
                group_key={Record.DATASET_: Record.DATASET__, Record.INSTANTIATES_: Record.INSTANTIATES__}, groups=[
                    {"name": "originalValues", "operator": "$push", "field": Record.VALUE__},
                    {"name": "mymedian", "operator": "$median",
                     "field": {"input": Record.VALUE__, "method": "approximate"}},
                ]),
            # after groupby, the access to individual elements is lost, so we need to use $push or $addToSet to keep track of original values
            Operators.unwind("originalValues"),
            Operators.project(field=None,
                              projected_value={"absVal": {"$abs": {"$subtract": ["$originalValues", "$mymedian"]}}}),
            Operators.group_by(
                group_key={Record.DATASET_: Record.DATASET___, Record.INSTANTIATES_: Record.INSTANTIATES___}, groups=[
                    {"name": "ema", "operator": "$median", "field": {"input": "$absVal", "method": "approximate"}}
                ])
        ]

    def skewness_and_kurtosis_query(self, features_ids: list) -> list:
        # kurtosis query
        # [ { "$group": { "_id": { "_id": null }, "originalValues": { "$push": "$value" }, "mymean": { "$avg": "$value" }, "mystdDev": { "$stdDevSamp": "$value" } } }, { "$unwind": "$originalValues" }, {"$project":{"thePowed":{"$pow":[{"$divide":[{"$subtract":["$originalValues","$mymean"]},"$mystdDev"]},4]}}},{"$group":{"_id":{"_id":null},"summed":{"$sum":"$thePowed"}, "summedValues": { "$push": "$thePowed" }}},{"$project":{"summed": 1, "summedValues": 1, "mysize": {"$size": "$summedValues"}}}, {"$project":{"kurtosis":{"$subtract":[{"$multiply":[{"$divide":[{"$multiply":["$mysize",{"$sum":["$mysize",1]}]},{"$multiply":[{"$sum":["$mysize",-1]},{"$sum":["$mysize",-2]},{"$sum":["$mysize",-3]}]}]},"$summed"]},{"$divide":[{"$multiply":[3,{"$pow":[{"$sum":["$mysize",-1]},2]}]},{"$multiply":[{"$sum":["$mysize",-2]},{"$sum":["$mysize",-3]}]}]}]}}}]
        # skewness query
        # [ { "$group": { "_id": { "_id": null }, "originalValues": { "$push": "$value" }, "mymean": { "$avg": "$value" }, "mystdDev": { "$stdDevSamp": "$value" } } }, { "$unwind": "$originalValues" }, {"$project":{"thePowed":{"$pow":[{"$divide":[{"$subtract":["$originalValues","$mymean"]},"$mystdDev"]},3]}}},{"$group":{"_id":{"_id":null},"summed":{"$sum":"$thePowed"}, "summedValues": { "$push": "$thePowed" }}},{"$project":{"summed": 1, "summedValues": 1, "mysize": {"$size": "$summedValues"}}}, {"$project":{"skewness":{"$multiply": [{"$divide": ["$mysize", {"$multiply": [{"$sum": ["$mysize", -1]}, {"$sum": ["$mysize", -2]}]}]}, "$summed"]}}}]
        # and we merge them to avoid repeating computation of mean, std, dev and the array with push
        # [ { "$group": { "_id": { "_id": null }, "originalValues": { "$push": "$value" }, "mymean": { "$avg": "$value" }, "mystdDev": { "$stdDevSamp": "$value" } } }, { "$unwind": "$originalValues" }, {"$project":{"theQuads":{"$pow":[{"$divide":[{"$subtract":["$originalValues","$mymean"]},"$mystdDev"]},4]}, "theSquares":{"$pow":[{"$divide":[{"$subtract":["$originalValues","$mymean"]},"$mystdDev"]},3]}}},{"$group":{"_id":{"_id":null},"sumQuads":{"$sum":"$theQuads"}, "sumSquares":{"$sum":"$theSquares"}, "summedValues": { "$push": "$theQuads" }}},{"$project":{"sumQuads": 1, "sumSquares": 1, "mysize": {"$size": "$summedValues"}}}, {"$project":{"kurtosis":{"$subtract":[{"$multiply":[{"$divide":[{"$multiply":["$mysize",{"$sum":["$mysize",1]}]},{"$multiply":[{"$sum":["$mysize",-1]},{"$sum":["$mysize",-2]},{"$sum":["$mysize",-3]}]}]},"$sumQuads"]},{"$divide":[{"$multiply":[3,{"$pow":[{"$sum":["$mysize",-1]},2]}]},{"$multiply":[{"$sum":["$mysize",-2]},{"$sum":["$mysize",-3]}]}]}]}, "skewness":{"$multiply": [{"$divide": ["$mysize", {"$multiply": [{"$sum": ["$mysize", -1]}, {"$sum": ["$mysize", -2]}]}]}, "$sumSquares"]}}}]
        return [
            Operators.match(field=Record.INSTANTIATES_, value={"$in": features_ids}, is_regex=False),
            Operators.group_by(
                group_key={Record.DATASET_: Record.DATASET__, Record.INSTANTIATES_: Record.INSTANTIATES__}, groups=[
                    {"name": "originalValues", "operator": "$push", "field": Record.VALUE__},
                    {"name": "mymean", "operator": "$avg", "field": Record.VALUE__},
                    {"name": "mystdDev", "operator": "$stdDevSamp", "field": Record.VALUE__}
                ]),
            # after groupby, the access to individual elements is lost, so we need to use $push or $addToSet to keep track of original values
            Operators.unwind("originalValues"),
            # to be able to apply $subtract to each element of the array -- this adds the $ in front of the variable name
            Operators.project(field=None, projected_value={
                "theQuads": {
                    "$cond": {
                        "if": {"$eq": ["$mystdDev", 0]},
                        "then": [0],
                        "else": {
                            "$pow": [{"$divide": [{"$subtract": ["$originalValues", "$mymean"]}, "$mystdDev"]}, 4]
                        }
                    }
                },
                "theSquares": {
                    "$cond": {
                        "if": {"$eq": ["$mystdDev", 0]},
                        "then": [0],
                        "else": {
                            "$pow": [{"$divide": [{"$subtract": ["$originalValues", "$mymean"]}, "$mystdDev"]}, 3]
                        }
                    }
                }}),
            Operators.group_by(group_key="$_id", groups=[
                {"name": "sumQuads", "operator": "$sum", "field": "$theQuads"},
                {"name": "sumSquares", "operator": "$sum", "field": "$theSquares"},
                {"name": "summedValues", "operator": "$push", "field": "$theQuads"}
                # to later compute the number of values, we keep track of all the elements that have been summed
            ]),
            Operators.project(field=None,
                              projected_value={"sumQuads": 1, "sumSquares": 1, "n": {"$size": "$summedValues"}}),
            Operators.project(field=None, projected_value={
                "kurtosis": {
                    "$cond": {
                        "if": {"$or": [
                            {"$eq": ["$n", 0]},
                            {"$eq": ["sumQuads", [0]]},
                            {"$eq": [{"$sum": ["$n", -1]}, 0]},
                            {"$eq": [{"$sum": ["$n", -2]}, 0]},
                            {"$eq": [{"$sum": ["$n", -3]}, 0]}
                        ]},
                        "then": 0,
                        "else": {
                            "$subtract": [
                                {"$multiply": [
                                    {"$divide": [
                                        {"$multiply": ["$n", {"$sum": ["$n", 1]}]},
                                        {"$multiply": [{"$sum": ["$n", -1]}, {"$sum": ["$n", -2]},
                                                       {"$sum": ["$n", -3]}]}
                                    ]},
                                    "$sumQuads"
                                ]}
                                , {"$divide": [
                                    {"$multiply": [3, {"$pow": [{"$sum": ["$n", -1]}, 2]}]},
                                    {"$multiply": [{"$sum": ["$n", -2]}, {"$sum": ["$n", -3]}]}
                                ]}
                            ]
                        }
                    }
                },
                "skewness": {
                    "$cond": {
                        "if": {"$or": [
                            {"$eq": ["$n", 0]},
                            {"$eq": ["sumSquares", [0]]},
                            {"$eq": [{"$sum": ["$n", -1]}, 0]},
                            {"$eq": [{"$sum": ["$n", -2]}, 0]}
                        ]},
                        "then": 0,
                        "else": {
                            "$multiply": [
                                {"$divide": [
                                    "$n", {"$multiply": [{"$sum": ["$n", -1]}, {"$sum": ["$n", -2]}]}
                                ]},
                                "$sumSquares"
                            ]
                        }
                    }
                }
            })
        ]

    def iqr_query(self, features_ids: list) -> list:
        return [
            Operators.match(field=Record.INSTANTIATES_, value={"$in": features_ids}, is_regex=False),
            Operators.group_by(
                group_key={Record.DATASET_: Record.DATASET__, Record.INSTANTIATES_: Record.INSTANTIATES__}, groups=[
                    {"name": "thevalues", "operator": "$push", "field": Record.VALUE__},
                    {"name": "themedian", "operator": "$median",
                     "field": {"input": Record.VALUE__, "method": "approximate"}}
                ]),
            Operators.set_variables(
                variables=[{"name": "thevalues", "operation": {"$sortArray": {"input": "$thevalues", "sortBy": 1}}}]),
            Operators.project(field=None, projected_value={"thevalues": {
                "$filter": {"input": "$thevalues", "as": "item", "cond": {"$ne": ["$$item", "$themedian"]}}}}),
            Operators.add_fields(key="thevalues", value={
                "$cond": {
                    "if": {"$eq": [{"$size": "$thevalues"}, 0]},
                    "then": [[], []],
                    "else": {
                        "$cond": {
                            "if": {"$eq": [{"mod": [{"$size": "$thevalues"}, 2]}, 0]},
                            "then": {
                                "$map": {
                                    "input": {"$range": [0, {"$size": "$thevalues"},
                                                         {"$divide": [{"$size": "$thevalues"}, 2]}]},
                                    "as": "index",
                                    "in": {"$slice": ["$thevalues", "$$index", 2]}
                                }
                            },
                            "else": {
                                "$map": {
                                    "input": {"$range": [0, {"$size": "$thevalues"},
                                                         {"$floor": {"$divide": [{"$size": "$thevalues"}, 2]}}]},
                                    "as": "index",
                                    "in": {"$slice": ["$thevalues", "$$index",
                                                      {"$floor": {"$divide": [{"$size": "$thevalues"}, 2]}}]}
                                }
                            }
                        }
                    }
                }
            }),
            Operators.set_variables(variables=[
                {"name": "q1",
                 "operation": {"$median": {"input": {"$arrayElemAt": ["$thevalues", 0]}, "method": "approximate"}}},
                {"name": "q3",
                 "operation": {"$median": {"input": {"$arrayElemAt": ["$thevalues", 1]}, "method": "approximate"}}},
            ]),
            Operators.set_variables(variables=[
                {"name": "iqr", "operation": {"$subtract": ["$q3", "$q1"]}}  # compute the IQR as Q3-Q1
            ]),
            Operators.unset_variables(["thevalues", "q1", "q3"])
        ]

    def pearson_correlation_query(self, features_ids: list, database: Database) -> list:
        # drop existing temporary tables
        database.drop_table(TableNames.TOP10_FEATURES)
        database.drop_table(TableNames.PAIRS_FEATURES)
        # first part: create pairs of features for which pearson coefficient will be computed
        # we only take the top-10 of numeric features, i.e.,
        # the 10 numeric features with most values (if tied, take the ones with the lowest ID)
        top_10_features = [
            Operators.match(field=Record.INSTANTIATES_, value={"$in": features_ids}, is_regex=False),
            Operators.group_by(
                group_key={Record.DATASET_: Record.DATASET__, Record.INSTANTIATES_: Record.INSTANTIATES__}, groups=[
                    {"name": "frequency", "operator": "$sum", "field": 1}
                ]),
            Operators.sort(field=f"_id.{Record.DATASET_}", sort_order=-1),
            Operators.sort(field="frequency", sort_order=-1),
            Operators.sort(field=f"_id.{Record.INSTANTIATES_}", sort_order=1),
            Operators.group_by(group_key={Record.DATASET_: Record.DATASET___}, groups=[
                {"name": "orderedfeatures", "operator": "$push", "field": Record.INSTANTIATES___}
            ]),
            Operators.set_variables(variables=[
                {"name": "top10",
                 "operation": Operators.filter_array(input_array_name="$orderedfeatures", element="item", cond={},
                                                     limit=10)}
            ]),
            # Operators.limit(10),
            Operators.project(field=None, projected_value={
                Record.DATASET_: Record.DATASET___,
                "top10": 1,
                "_id": 0
            }),
            Operators.unwind(field="top10"),
            Operators.project(field=None, projected_value={Record.INSTANTIATES_: "$top10", Record.DATASET_: 1}),
            Operators.write_to_table(table_name=TableNames.TOP10_FEATURES)
        ]
        log.info(top_10_features)
        database.db[TableNames.RECORD].aggregate(top_10_features)

        pairs_features = [
            Operators.cartesian_product(join_table_name=TableNames.TOP10_FEATURES, lookup_field_name="features_b",
                                        filter_dict={}),
            Operators.unwind(field="features_b"),
            Operators.match(field=None, value={"$expr": {
                "$and": [
                    {"$eq": [Record.DATASET__, f"$features_b.{Record.DATASET_}"]},
                    # created pairs of features in a single dataset
                    {"$lt": [Record.INSTANTIATES__, f"$features_b.{Record.INSTANTIATES_}"]}
                ]
            }}, is_regex=False),
            Operators.project(field=None,
                              projected_value={Record.DATASET_: Record.DATASET__, "feat_a": Record.INSTANTIATES__,
                                               "feat_b": f"$features_b.{Record.INSTANTIATES_}", "_id": 0}),
            # this allows to not export the _id in the table (which contains duplicated due to unwind), and _id is regenerated when creating the table
            Operators.write_to_table(TableNames.PAIRS_FEATURES)
        ]
        log.info(pairs_features)
        database.db[TableNames.TOP10_FEATURES].aggregate(pairs_features)

        # second part, compute the Pearson coefficient for each pair of features stored above within the collection
        return [
            Operators.lookup(
                join_table_name=TableNames.RECORD,
                local_field=None,
                foreign_field=None,
                let={"feat_a": "$feat_a", "a_dataset": Record.DATASET__},
                pipeline=[Operators.match(field=None, value={"$expr": {"$and": [
                    {"$eq": [Record.INSTANTIATES__, "$$feat_a"]},
                    {"$eq": [Record.DATASET__, "$$a_dataset"]}
                ]}}, is_regex=False)],
                lookup_field_name="x"
            ),
            Operators.lookup(
                join_table_name=TableNames.RECORD,
                local_field=None,
                foreign_field=None,
                let={"feat_b": "$feat_b", "b_dataset": Record.DATASET__},
                pipeline=[Operators.match(field=None, value={"$expr": {
                    "$and": [
                        {"$eq": [Record.INSTANTIATES__, "$$feat_b"]},
                        {"$eq": [Record.DATASET__, "$$b_dataset"]}
                    ]}}, is_regex=False)],
                lookup_field_name="y"
            ),
            Operators.project(field=None, projected_value={"values": {"$zip": {"inputs": ["$x", "$y"]}}}),
            Operators.unwind(field="values"),
            Operators.project(field=None, projected_value={"x": {"$arrayElemAt": ["$values", 0]},
                                                           "y": {"$arrayElemAt": ["$values", 1]}}),
            Operators.project(field=None, projected_value={"x": "$x.value", "y": f"$y.{Record.VALUE_}",
                                                           "feat_a": f"$x.{Record.INSTANTIATES_}",
                                                           "feat_b": f"$y.{Record.INSTANTIATES_}",
                                                           Record.DATASET_: f"$x.{Record.DATASET_}"}),
            Operators.group_by(group_key={"feat_a": "$feat_a", "feat_b": "$feat_b", Record.DATASET_: Record.DATASET__},
                               groups=[
                                   {"name": "originalX", "operator": "$push", "field": "$x"},
                                   {"name": "originalY", "operator": "$push", "field": "$y"},
                                   {"name": "avgX", "operator": "$avg", "field": "$x"},
                                   {"name": "avgY", "operator": "$avg", "field": "$y"},
                               ]),
            Operators.project(field=None, projected_value={
                # to subtract/map a number to each element of an array, the syntax is a bit more complex than "Xsubs": {"$subtract": ["$originalX", "$avgX"]}
                # it requires a map
                "subsX": {"$map": {"input": "$originalX", "as": "originalXval",
                                   "in": {"$subtract": ["$$originalXval", "$avgX"]}}},
                "subsY": {"$map": {"input": "$originalY", "as": "originalYval",
                                   "in": {"$subtract": ["$$originalYval", "$avgY"]}}}
            }),
            Operators.project(field=None, projected_value={
                "subsX": 1,
                "subsY": 1,
                "subsSquareX": {"$map": {"input": "$subsX", "as": "subsXVal", "in": {"$pow": ["$$subsXVal", 2]}}},
                "subsSquareY": {"$map": {"input": "$subsY", "as": "subsYVal", "in": {"$pow": ["$$subsYVal", 2]}}},
                "multXY": {"$map": {"input": {"$zip": {"inputs": ["$subsX", "$subsY"]}}, "as": "pair",
                                    "in": {
                                        "$multiply": [{"$arrayElemAt": ["$$pair", 0]}, {"$arrayElemAt": ["$$pair", 1]}]}
                                    }}
            }),
            Operators.project(field=None, projected_value={
                "sumXsubsSquare": {"$sum": "$subsSquareX"},
                # no need to group by in this case (and the group by does not work)
                "sumYsubsSquare": {"$sum": "$subsSquareY"},
                "sumXY": {"$sum": "$multXY"}
            }),
            Operators.project(field=None, projected_value={
                "pearson": {
                    "$cond": {
                        "if": {"$or": [{"$eq": ["$sumXsubsSquare", 0]}, {"$eq": ["$sumYsubsSquare", 0]}]},
                        "then": 0,
                        "else": {
                            "$divide": ["$sumXY", {"$sqrt": {"$multiply": ["$sumXsubsSquare", "$sumYsubsSquare"]}}]
                        }
                    }
                }
            }),
            # the last group by and project groups all features b within the feature a for easier profiles
            Operators.group_by(group_key={Record.INSTANTIATES_: "$_id.feat_a", Record.DATASET_: Record.DATASET___},
                               groups=[
                                   {"name": "feat_b_dict", "operator": "$push",
                                    "field": {"k": "$_id.feat_b", "v": "$pearson"}}
                               ]),
            Operators.project(field=None, projected_value={
                Record.INSTANTIATES_: 1,
                "pearson": {
                    "$arrayToObject": {
                        "$map": {
                            "input": "$feat_b_dict",
                            "as": "item",
                            "in": [{"$toString": "$$item.k"}, {"coefficient": "$$item.v"}]
                        }
                    }
                }
            })
        ]

    def imbalance_query(self, features_ids: list) -> list:
        # Ratio between the number of appearances of the most frequent value and the least frequent value.
        return [
            Operators.match(field=Record.INSTANTIATES_, value={"$in": features_ids}, is_regex=False),
            Operators.group_by(
                group_key={Record.DATASET_: Record.DATASET__, Record.INSTANTIATES_: Record.INSTANTIATES__,
                           Record.VALUE_: Record.VALUE__}, groups=[
                    {"name": "frequency", "operator": "$sum", "field": 1}
                ]),
            Operators.group_by(
                group_key={Record.INSTANTIATES_: Record.INSTANTIATES___, Record.DATASET_: Record.DATASET___}, groups=[
                    {"name": "max_freq", "operator": "$max", "field": "$frequency"},
                    {"name": "min_freq", "operator": "$min", "field": "$frequency"},
                ]),
            Operators.project(field=None, projected_value={
                Record.DATASET_: Record.DATASET___,
                Record.INSTANTIATES_: Record.INSTANTIATES___,
                "imbalance": {
                    "$cond": {
                        "if": {"$eq": ["$min_freq", 0]},
                        "then": 0,
                        "else": {"$divide": ["$max_freq", "$min_freq"]}
                    }
                }
            })
        ]

    def constancy_query(self, features_ids: list) -> list:
        # Ratio between the number of appearances of the most frequent value and the number of non-null values.
        # However, we never have null values because we do not create Records for them
        # thus, the constancy is always 1 (for practical reasons, I divided the max freq by itself, to always obtain 1)
        return [
            Operators.match(field=Record.INSTANTIATES_, value={"$in": features_ids}, is_regex=False),
            Operators.group_by(
                group_key={Record.INSTANTIATES_: Record.INSTANTIATES__, Record.DATASET_: Record.DATASET__,
                           Record.VALUE_: Record.VALUE__}, groups=[
                    {"name": "frequency", "operator": "$sum", "field": 1},
                    {"name": "frequencyNonNull", "operator": "$sum", "field": 1}
                ]),
            Operators.group_by(
                group_key={Record.INSTANTIATES_: Record.INSTANTIATES___, Record.DATASET_: Record.DATASET___}, groups=[
                    {"name": "max_freq", "operator": "$max", "field": "$frequency"}
                ]),
            Operators.project(field=None, projected_value={
                Record.DATASET_: Record.DATASET___,
                Record.INSTANTIATES_: Record.INSTANTIATES___,
                "constancy": {
                    "$cond": {
                        "if": {"$eq": ["$max_freq", 0]},
                        "then": 0,
                        "else": {"$divide": ["$max_freq", "$max_freq"]}
                    }
                }
            })
        ]

    def mode_query(self, features_ids: list) -> list:
        # the most frequent value
        return [
            Operators.match(field=Record.INSTANTIATES_, value={"$in": features_ids}, is_regex=False),
            Operators.group_by(
                group_key={Record.INSTANTIATES_: Record.INSTANTIATES__, Record.DATASET_: Record.DATASET__,
                           Record.VALUE_: Record.VALUE__}, groups=[
                    {"name": "frequency", "operator": "$sum", "field": 1},
                ]),
            Operators.sort(field=f"_id.{Record.INSTANTIATES_}", sort_order=1),
            Operators.sort(field="frequency", sort_order=-1),
            Operators.group_by(
                group_key={Record.INSTANTIATES_: Record.INSTANTIATES___, Record.DATASET_: Record.DATASET___}, groups=[
                    {"name": "values", "operator": "$push", "field": Record.VALUE___}
                ]),
            Operators.project(field=None, projected_value={
                Record.DATASET_: Record.DATASET___,
                Record.INSTANTIATES_: Record.INSTANTIATES___,
                "mode": {"$arrayElemAt": ["$values", 0]}
            })
        ]

    def uniqueness_query(self, features_ids: list) -> list:
        # Percentage of distinct values with respect to the total amount of non-null values
        return [
            Operators.match(field=Record.INSTANTIATES_, value={"$in": features_ids}, is_regex=False),
            Operators.group_by(
                group_key={Record.INSTANTIATES_: Record.INSTANTIATES__, Record.DATASET_: Record.DATASET__,
                           Record.VALUE_: Record.VALUE__}, groups=[
                    {"name": "frequency", "operator": "$sum", "field": 1}]),
            Operators.group_by(
                group_key={Record.INSTANTIATES_: Record.INSTANTIATES___, Record.DATASET_: Record.DATASET___}, groups=[
                    {"name": "count", "operator": "$sum", "field": "$frequency"},
                    {"name": "distinct_count", "operator": "$sum", "field": 1}
                ]
                ),
            Operators.project(field=None, projected_value={
                Record.DATASET_: Record.DATASET___,
                Record.INSTANTIATES_: Record.INSTANTIATES___,
                "uniqueness": {
                    "$cond": {
                        "if": {"$eq": ["$count", 0]},
                        "then": 0,
                        "else": {
                            "$divide": ["$distinct_count", "$count"]
                        }
                    }
                }
            })
        ]

    def entropy_query(self, features_ids: list) -> list:
        # Measure of uncertainty and disorder within the values of the column.
        # A large entropy means that the values are highly heterogeneous.
        return [
            Operators.match(field=Record.INSTANTIATES_, value={"$in": features_ids}, is_regex=False),
            Operators.group_by(
                group_key={Record.INSTANTIATES_: Record.INSTANTIATES__, Record.DATASET_: Record.DATASET__,
                           Record.VALUE_: Record.VALUE__}, groups=[
                    {"name": "frequency", "operator": "$sum", "field": 1}]),
            Operators.group_by(
                group_key={Record.INSTANTIATES_: Record.INSTANTIATES___, Record.DATASET_: Record.DATASET___}, groups=[
                    {"name": "total", "operator": "$sum", "field": "$frequency"},
                    {"name": "frequencies", "operator": "$push", "field": "$frequency"}
                ]),
            Operators.unwind("frequencies"),
            Operators.project(field=None, projected_value={
                Record.DATASET_: Record.DATASET___,
                Record.INSTANTIATES_: Record.INSTANTIATES___,
                "prob": {
                    "$cond": {
                        "if": {"$eq": ["$total", 0]},
                        "then": [0],
                        "else": {"$divide": ["$frequencies", "$total"]}
                    }
                }
            }),
            Operators.project(field=None, projected_value={
                "_id": "$_id",
                "entropy_value": {"$multiply": ["$prob", {"$log": ["$prob", 2]}]}
            }),
            Operators.group_by(
                group_key={Record.INSTANTIATES_: Record.INSTANTIATES___, Record.DATASET_: Record.DATASET___}, groups=[
                    {"name": "entropy", "operator": "$sum", "field": "$entropy_value"}
                ]),
            Operators.project(field=None, projected_value={
                Record.DATASET_: Record.DATASET___,
                Record.INSTANTIATES_: Record.INSTANTIATES___,
                "entropy": {"$abs": "$entropy"}
            })
        ]

    def density_query(self, features_ids: list) -> list:
        # a measure of appropriate numerosity and intensity between different real-world entities available in the data
        return [
            Operators.match(field=Record.INSTANTIATES_, value={"$in": features_ids}, is_regex=False),
            Operators.group_by(
                group_key={Record.INSTANTIATES_: Record.INSTANTIATES__, Record.DATASET_: Record.DATASET__,
                           Record.VALUE_: Record.VALUE__}, groups=[
                    {"name": "frequency", "operator": "$sum", "field": 1}]),
            Operators.group_by(
                group_key={Record.INSTANTIATES_: Record.INSTANTIATES___, Record.DATASET_: Record.DATASET___}, groups=[
                    {"name": "total", "operator": "$sum", "field": "$frequency"},
                    {"name": "distinct_count", "operator": "$sum", "field": 1},
                    {"name": "frequencies", "operator": "$push", "field": "$frequency"}
                ]),
            Operators.unwind("frequencies"),
            Operators.project(field=None, projected_value={
                "dataset": Record.DATASET___,
                Record.INSTANTIATES_: Record.INSTANTIATES___,
                "prob": {
                    "$cond": {
                        "if": {"$eq": ["$total", 0]},
                        "then": [0],
                        "else": {"$divide": ["$frequencies", "$total"]}
                    }
                },
                "distinct_count": "$distinct_count"
            }),
            Operators.group_by(
                group_key={Record.INSTANTIATES_: Record.INSTANTIATES___, Record.DATASET_: Record.DATASET___}, groups=[
                    {"name": "avg_dens_value", "operator": "$avg", "field": "$prob"},
                    {"name": "probs", "operator": "$push", "field": "$prob"},
                    {"name": "distinct_count", "operator": "$first", "field": "$distinct_count"}
                ]),
            Operators.unwind("probs"),
            Operators.project(field=None, projected_value={
                Record.DATASET_: Record.DATASET___,
                Record.INSTANTIATES_: Record.INSTANTIATES___,
                "density_value": {"$abs": {"$subtract": ["$probs", "$avg_dens_value"]}},
                "distinct_count": "$distinct_count"
            }),
            Operators.group_by(
                group_key={Record.INSTANTIATES_: Record.INSTANTIATES___, Record.DATASET_: Record.DATASET___}, groups=[
                    {"name": "densities_sum", "operator": "$sum", "field": "$density_value"},
                    {"name": "distinct_count", "operator": "$first", "field": "$distinct_count"}
                ]),
            Operators.project(field=None, projected_value={
                Record.DATASET_: Record.DATASET___,
                Record.INSTANTIATES_: Record.INSTANTIATES___,
                "density": {
                    "$cond": {
                        "if": {"$eq": ["$distinct_count", 0]},
                        "then": 0,
                        "else": {"$divide": ["$densities_sum", "$distinct_count"]}
                    }
                }
            })
        ]

    def values_and_counts_query(self, features_ids: list) -> list:
        return [
            Operators.match(field=Record.INSTANTIATES_, value={"$in": features_ids}, is_regex=False),
            Operators.group_by(
                group_key={Record.INSTANTIATES_: Record.INSTANTIATES__, Record.DATASET_: Record.DATASET__,
                           Record.VALUE_: Record.VALUE__},
                groups=[{"name": "counts", "operator": "$sum", "field": 1}]),
            Operators.group_by(
                group_key={Record.INSTANTIATES_: Record.INSTANTIATES___, Record.DATASET_: Record.DATASET___}, groups=[
                    # push in an array of maps of two elements (value and counts) the elements of the  pair (dataset, feature)
                    {"name": "values_and_counts", "operator": "$push", "field": {Record.VALUE_: {
                        "$cond": {"if": {"$eq": [Record.VALUE___, DEFAULT_NAN_VALUE]}, "then": None,
                                  "else": Record.VALUE___}}, "count": "$counts"}}
                ])
        ]

    def data_type_validity_query(self, features_ids: list) -> list:
        reverse_mapping = {"int": "int", "double": "float", "string": "str"}
        type_switch = {
            "$switch": {
                "branches": [
                    {
                        "case": {"$eq": [{"$type": "$value"}, bson_type]},
                        "then": user_alias
                    }
                    for bson_type, user_alias in reverse_mapping.items()
                ],
                "default": "unknown"
            }
        }

        # db["Record"].aggregate([
        # { "$addFields": { "featureType": { "$type": "$value" } } },
        # { "$group": { "_id": { "instantiates": "$instantiates", "dataset": "$dataset", "featureType": "$featureType" }, "count": { "$sum": 1 } } },
        # { "$set": {"featureTypeAlias":{"$switch":{"branches":[{"case":{"$eq":["$_id.featureType","int"]},"then":"int"},{"case":{"$eq":["$_id.featureType","double"]},"then":"float"},{"case":{"$eq":["$_id.featureType","string"]},"then":"str"},{"case":{"$eq":["$_id.featureType","date"]},"then":"date"},{"case":{"$eq":["$_id.featureType","timestamp"]},"then":"datetime"},{"case":{"$eq":["$_id.featureType","bool"]},"then":"bool"},{"case":{"$eq":["$_id.featureType","object"]},"then":"category"}],"default":"unknown"}}}}
        # { "$lookup": { "from": "Feature", "localField": "_id.instantiates", "foreignField": "identifier", "let": { "featureTypess": "$featureTypeAlias" }, "pipeline": [{ "$match": { "$expr": { "$eq": ["$data_type", "$$featureTypess"] } } }], "as": "common_fid" } },
        # { "$lookup": { "from": "TMP_CountsFeatures", "localField": "_id.instantiates", "foreignField": "identifier", "as": "counts_fid" } },
        # { "$set": { "f_all_counts": { "$arrayElemAt": ["$counts_fid.count_all_values", 0]}}},
        # { "$project": {"instantiates": "$_id.instantiates", "dt_validity": {"$divide": ["$count", "$f_all_counts"]}}}
        # ] )
        return [
            Operators.match(field=Record.INSTANTIATES_, value={"$in": features_ids}, is_regex=False),
            Operators.add_fields(key="featureType", value={"$type": Record.VALUE__}),
            Operators.group_by(
                group_key={Record.INSTANTIATES_: Record.INSTANTIATES__, Record.DATASET_: Record.DATASET__,
                           "featureType": "$featureType"},
                groups=[{"name": "count", "operator": "$sum", "field": 1}]),
            Operators.set_variables(variables=[{"name": "featureTypeAlias", "operation": type_switch}]),
            Operators.lookup(join_table_name=TableNames.FEATURE, local_field=f"_id.{Record.INSTANTIATES_}",
                             foreign_field=Feature.IDENTIFIER_, let={"featureTypess": "$featureTypeAlias"},
                             pipeline=[{"$match": {"$expr": {"$eq": [Feature.DT__, "$$featureTypess"]}}}],
                             lookup_field_name="common_fid"),
            Operators.lookup(join_table_name=TableNames.COUNTS_FEATURES, local_field=f"_id.{Record.INSTANTIATES_}", foreign_field=Feature.IDENTIFIER_, lookup_field_name="counts_fid", let=None, pipeline=None),
            Operators.set_variables(variables=[{"name": "f_all_counts", "operation": {"$arrayElemAt": ["$counts_fid.count_all_values", 0]}}]),
            Operators.project(field={"data_type_validity": {"$divide": ["$count", "$f_all_counts"]}}, projected_value=None)
        ]

    def missing_percentage_query_without_samples(self, features_ids: list) -> list:
        return [
            # first compute the missing percentage of each feature by dividing the number of values and the number of patients
            Operators.match(field=None, value={"$and": [{Record.INSTANTIATES_: {"$in": features_ids}}, {
                Record.ENTITY_TYPE_: {"$ne": f"{Profile.CLINICAL}{TableNames.RECORD}"}}]}, is_regex=False),
            Operators.group_by(
                group_key={Record.DATASET_: Record.DATASET__, Record.INSTANTIATES_: Record.INSTANTIATES__}, groups=[
                    {"name": "count_db_values", "operator": "$sum", "field": 1}
                ]),
            Operators.lookup(join_table_name=TableNames.COUNTS_PATIENTS, foreign_field=f"_id.{Record.DATASET_}",
                             local_field=f"_id.{Record.DATASET_}", lookup_field_name="joined", let=None, pipeline=None),
            Operators.project(field=None, projected_value={
                "missing_percentage": {
                    "$cond": {
                        "if": {"$eq": [{"$arrayElemAt": ["$joined.distinct_nb_patients", 0]}, 0]},
                        "then": 0,
                        "else": {"$subtract": [1, {
                            "$divide": ["$count_db_values", {"$arrayElemAt": ["$joined.distinct_nb_patients", 0]}]}]}
                    }
                }
            })
        ]

    def missing_percentage_query_with_samples(self, features_ids: list) -> list:
        return [
            # first compute the missing percentage of each feature by dividing the number of values and the number of patients
            Operators.match(field=None, value={"$and": [{Record.INSTANTIATES_: {"$in": features_ids}}, {
                Record.ENTITY_TYPE_: {"$eq": f"{Profile.CLINICAL}{TableNames.RECORD}"}}]}, is_regex=False),
            Operators.group_by(
                group_key={Record.DATASET_: Record.DATASET__, Record.INSTANTIATES_: Record.INSTANTIATES__}, groups=[
                    {"name": "count_db_values", "operator": "$sum", "field": 1}
                ]),
            Operators.lookup(join_table_name=TableNames.COUNTS_SAMPLES, foreign_field=Record.DATASET_,
                             local_field=Record.DATASET_, lookup_field_name="joined", let=None, pipeline=None),
            Operators.project(field=None, projected_value={
                "missing_percentage": {
                    "$cond": {
                        "if": {"$eq": [{"$arrayElemAt": ["$joined.distinct_nb_samples", 0]}, 0]},
                        "then": 0,
                        "else": {"$subtract": [1, {
                            "$divide": ["$count_db_values", {"$arrayElemAt": ["$joined.distinct_nb_samples", 0]}]}]}
                    }
                }
            })
        ]

    def finalize_query(self, include_value: bool) -> list:
        # get dataset and instantiates from the group key and keep remaining fields, except the group key _id
        # project requires to specify all fields to keep, set and unset work only on those of interest and keep others
        if include_value:
            last_fields = [{"$set": {Record.DATASET_: Record.DATASET___, Record.INSTANTIATES_: Record.INSTANTIATES___,
                                     Record.VALUE_: Record.VALUE___}}]
        else:
            last_fields = [{"$set": {Record.DATASET_: Record.DATASET___, Record.INSTANTIATES_: Record.INSTANTIATES___}}]
        last_fields.append({"$unset": ["_id"]})
        # merge the profile into the profile table
        last_fields.append(
            Operators.merge(table_name=TableNames.FEATURE_PROFILE,
                            on_attribute=[Record.DATASET_, Record.INSTANTIATES_],
                            when_matched="merge",
                            when_not_matched="insert"))
        return last_fields

    def to_json(self):
        return dataclasses.asdict(self, dict_factory=factory)

    def __str__(self):
        return json.dumps(self.to_json())
