from database.Database import Database
from enums.DataTypes import DataTypes
from enums.TableNames import TableNames
from database.Operators import Operators
from utils.setup_logger import log


class ProfileComputation:
    def __init__(self, database: Database, dataset_gid: str):
        self.database = database
        self.dataset_gid = dataset_gid

    def compute_features_profiles(self) -> None:
        # the filter {"datasets": self.dataset_gid} means that the array "datasets" contains the element self.dataset_gid
        cursor = self.database.find_operation(table_name=TableNames.FEATURE, filter_dict={"datasets": self.dataset_gid},
                                              projection={"identifier": 1, "data_type": 1, "_id": 0})
        map_feature_datatype = {}
        for element in cursor:
            if element["data_type"] not in map_feature_datatype:
                map_feature_datatype[element["data_type"]] = []
            map_feature_datatype[element["data_type"]].append(element["identifier"])
        log.info(map_feature_datatype)

        numeric_features = []
        numeric_features.extend(map_feature_datatype[DataTypes.INTEGER] if DataTypes.INTEGER in map_feature_datatype else [])
        numeric_features.extend(map_feature_datatype[DataTypes.FLOAT] if DataTypes.FLOAT in map_feature_datatype else [])
        log.info(numeric_features)
        categorical_features = []
        categorical_features.extend(map_feature_datatype[DataTypes.STRING] if DataTypes.STRING in map_feature_datatype else [])
        categorical_features.extend(map_feature_datatype[DataTypes.BOOLEAN] if DataTypes.BOOLEAN in map_feature_datatype else [])
        categorical_features.extend(map_feature_datatype[DataTypes.CATEGORY] if DataTypes.CATEGORY in map_feature_datatype else [])
        log.info(categorical_features)
        date_features = []
        date_features.extend(map_feature_datatype[DataTypes.DATE] if DataTypes.DATE in map_feature_datatype else [])
        date_features.extend(map_feature_datatype[DataTypes.DATETIME] if DataTypes.DATETIME in map_feature_datatype else [])
        log.info(date_features)
        all_features = numeric_features + categorical_features + date_features

        # NUMERIC FEATURES

        # 1. compute min, max, mean, median, std values for numerical features
        operators = ProfileComputation.min_max_mean_median_std_query(numeric_features, compute_min=True, compute_max=True, compute_mean=True, compute_median=True, compute_std=True)
        operators.append(Operators.merge(table_name=TableNames.FEATURE_PROFILE, on_attribute="_id", when_matched="merge", when_not_matched="insert"))
        log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

        # 2. compute the Median Absolute Deviation
        operators = ProfileComputation.abs_med_dev_query(numeric_features)
        operators.append(Operators.merge(table_name=TableNames.FEATURE_PROFILE, on_attribute="_id", when_matched="merge", when_not_matched="insert"))
        log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

        # 3. compute skewness and kurtosis for numerical features
        operators = ProfileComputation.skewness_and_kurtosis_query(numeric_features)
        operators.append(Operators.merge(table_name=TableNames.FEATURE_PROFILE, on_attribute="_id", when_matched="merge", when_not_matched="insert"))
        log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

        # 4. compute IQR for numerical features
        operators = ProfileComputation.iqr_query(numeric_features)
        operators.append(Operators.merge(table_name=TableNames.FEATURE_PROFILE, on_attribute="_id", when_matched="merge", when_not_matched="insert"))
        log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

        # 5. compute Pearson correlation coefficients
        operators = ProfileComputation.pearson_correlation_query(numeric_features, self.database)
        operators.append(Operators.merge(table_name=TableNames.FEATURE_PROFILE, on_attribute="_id", when_matched="merge", when_not_matched="insert"))
        log.info(operators)
        self.database.db[TableNames.PAIRS_FEATURES].aggregate(operators)

        # DATE FEATURES

        # 1. compute min, max, mean, median, std values for numerical features
        operators = ProfileComputation.min_max_mean_median_std_query(date_features, compute_min=True, compute_max=True, compute_mean=False, compute_median=True, compute_std=False)
        operators.append(Operators.merge(table_name=TableNames.FEATURE_PROFILE, on_attribute="_id", when_matched="merge", when_not_matched="insert"))
        log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

        # 2. compute IQR for numerical features
        operators = ProfileComputation.iqr_query(date_features)
        operators.append(Operators.merge(table_name=TableNames.FEATURE_PROFILE, on_attribute="_id", when_matched="merge", when_not_matched="insert"))
        log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

        # CATEGORICAL FEATURES
        # 1. compute imbalance for categorical features
        operators = ProfileComputation.imbalance_query(categorical_features)
        operators.append(Operators.merge(table_name=TableNames.FEATURE_PROFILE, on_attribute="_id", when_matched="merge", when_not_matched="insert"))
        log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

        # 2. compute constancy for categorical features
        operators = ProfileComputation.constancy_query(categorical_features)
        operators.append(Operators.merge(table_name=TableNames.FEATURE_PROFILE, on_attribute="_id", when_matched="merge", when_not_matched="insert"))
        log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

        # 3. compute mode for categorical features
        operators = ProfileComputation.mode_query(categorical_features)
        operators.append(Operators.merge(table_name=TableNames.FEATURE_PROFILE, on_attribute="_id", when_matched="merge", when_not_matched="insert"))
        log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

        # SHARED PROFILE FEATURES

        # uniqueness
        operators = ProfileComputation.uniqueness_query(all_features)
        operators.append(Operators.merge(table_name=TableNames.FEATURE_PROFILE, on_attribute="_id", when_matched="merge", when_not_matched="insert"))
        log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

        # entropy
        operators = ProfileComputation.entropy_query(all_features)
        operators.append(Operators.merge(table_name=TableNames.FEATURE_PROFILE, on_attribute="_id", when_matched="merge", when_not_matched="insert"))
        log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

        # density
        operators = ProfileComputation.density_query(all_features)
        operators.append(Operators.merge(table_name=TableNames.FEATURE_PROFILE, on_attribute="_id", when_matched="merge", when_not_matched="insert"))
        log.info(operators)
        self.database.db[TableNames.RECORD].aggregate(operators)

    @classmethod
    def min_max_mean_median_std_query(cls, features_ids: list, compute_min: bool, compute_max: bool, compute_mean: bool, compute_median: bool, compute_std: bool) -> list:
        groups = []
        if compute_min:
            groups.append({"name": "min_value", "operator": "$min", "field": "$value"})
        if compute_max:
            groups.append({"name": "max_value", "operator": "$max", "field": "$value"})
        if compute_mean:
            groups.append({"name": "mean_value", "operator": "$avg", "field": "$value"})
        if compute_median:
            groups.append({"name": "median_value", "operator": "$median", "field": {"input": "$value", "method": "approximate"}})
        if compute_std:
            groups.append({"name": "std_value", "operator": "$stdDevPop", "field": "$value"})
        return [
            Operators.match(field="instantiates", value={"$in": features_ids}, is_regex=False),
            Operators.group_by(group_key="$instantiates", groups=groups)
        ]

    @classmethod
    def abs_med_dev_query(cls, features_ids: list) -> list:
        # i.e., EMA=median(|Xi-Y|) where Xi is a value, Y is the median, and L is the absolute value (no minus sign)
        # [{'$group': {'_id': {'_id': null}, 'originalValues': {'$push': '$value'}, 'mymedian': {'$median': {"input": "$value", "method": "approximate"}}}}, {'$unwind': '$originalValues'}, {'$project': {"absVal": {'$abs': {'$subtract': ['$originalValues', '$mymedian']}}}}, {'$group': {'_id': {'_id': null}, 'ema': {'$median': {"input": "$absVal", "method": "approximate"}}}}]
        return [
            Operators.match(field="instantiates", value={"$in": features_ids}, is_regex=False),
            Operators.group_by(group_key="$instantiates", groups=[
                {"name": "originalValues", "operator": "$push", "field": "$value"},
                {"name": "mymedian", "operator": "$median", "field": {"input": "$value", "method": "approximate"}},
            ]),
            # after groupby, the access to individual elements is lost, so we need to use $push or $addToSet to keep track of original values
            Operators.unwind("originalValues"),
            Operators.project(field=None, projected_value={"absVal": {"$abs": {"$subtract": ["$originalValues", "$mymedian"]}}}),
            Operators.group_by(group_key="$_id", groups=[
                {"name": "ema", "operator": "$median", "field": {"input": "$absVal", "method": "approximate"}}
            ])
        ]

    @classmethod
    def skewness_and_kurtosis_query(cls, features_ids: list) -> list:
        # kurtosis query
        # [ { "$group": { "_id": { "_id": null }, "originalValues": { "$push": "$value" }, "mymean": { "$avg": "$value" }, "mystdDev": { "$stdDevSamp": "$value" } } }, { "$unwind": "$originalValues" }, {"$project":{"thePowed":{"$pow":[{"$divide":[{"$subtract":["$originalValues","$mymean"]},"$mystdDev"]},4]}}},{"$group":{"_id":{"_id":null},"summed":{"$sum":"$thePowed"}, "summedValues": { "$push": "$thePowed" }}},{"$project":{"summed": 1, "summedValues": 1, "mysize": {"$size": "$summedValues"}}}, {"$project":{"kurtosis":{"$subtract":[{"$multiply":[{"$divide":[{"$multiply":["$mysize",{"$sum":["$mysize",1]}]},{"$multiply":[{"$sum":["$mysize",-1]},{"$sum":["$mysize",-2]},{"$sum":["$mysize",-3]}]}]},"$summed"]},{"$divide":[{"$multiply":[3,{"$pow":[{"$sum":["$mysize",-1]},2]}]},{"$multiply":[{"$sum":["$mysize",-2]},{"$sum":["$mysize",-3]}]}]}]}}}]
        # skewness query
        # [ { "$group": { "_id": { "_id": null }, "originalValues": { "$push": "$value" }, "mymean": { "$avg": "$value" }, "mystdDev": { "$stdDevSamp": "$value" } } }, { "$unwind": "$originalValues" }, {"$project":{"thePowed":{"$pow":[{"$divide":[{"$subtract":["$originalValues","$mymean"]},"$mystdDev"]},3]}}},{"$group":{"_id":{"_id":null},"summed":{"$sum":"$thePowed"}, "summedValues": { "$push": "$thePowed" }}},{"$project":{"summed": 1, "summedValues": 1, "mysize": {"$size": "$summedValues"}}}, {"$project":{"skewness":{"$multiply": [{"$divide": ["$mysize", {"$multiply": [{"$sum": ["$mysize", -1]}, {"$sum": ["$mysize", -2]}]}]}, "$summed"]}}}]
        # and we merge them to avoid repeating computation of mean, std, dev and the array with push
        # [ { "$group": { "_id": { "_id": null }, "originalValues": { "$push": "$value" }, "mymean": { "$avg": "$value" }, "mystdDev": { "$stdDevSamp": "$value" } } }, { "$unwind": "$originalValues" }, {"$project":{"theQuads":{"$pow":[{"$divide":[{"$subtract":["$originalValues","$mymean"]},"$mystdDev"]},4]}, "theSquares":{"$pow":[{"$divide":[{"$subtract":["$originalValues","$mymean"]},"$mystdDev"]},3]}}},{"$group":{"_id":{"_id":null},"sumQuads":{"$sum":"$theQuads"}, "sumSquares":{"$sum":"$theSquares"}, "summedValues": { "$push": "$theQuads" }}},{"$project":{"sumQuads": 1, "sumSquares": 1, "mysize": {"$size": "$summedValues"}}}, {"$project":{"kurtosis":{"$subtract":[{"$multiply":[{"$divide":[{"$multiply":["$mysize",{"$sum":["$mysize",1]}]},{"$multiply":[{"$sum":["$mysize",-1]},{"$sum":["$mysize",-2]},{"$sum":["$mysize",-3]}]}]},"$sumQuads"]},{"$divide":[{"$multiply":[3,{"$pow":[{"$sum":["$mysize",-1]},2]}]},{"$multiply":[{"$sum":["$mysize",-2]},{"$sum":["$mysize",-3]}]}]}]}, "skewness":{"$multiply": [{"$divide": ["$mysize", {"$multiply": [{"$sum": ["$mysize", -1]}, {"$sum": ["$mysize", -2]}]}]}, "$sumSquares"]}}}]
        return [
            Operators.match(field="instantiates", value={"$in": features_ids}, is_regex=False),
            Operators.group_by(group_key="$instantiates", groups=[
                {"name": "originalValues", "operator": "$push", "field": "$value"},
                {"name": "mymean", "operator": "$avg", "field": "$value"},
                {"name": "mystdDev", "operator": "$stdDevSamp", "field": "$value"}
            ]),
            # after groupby, the access to individual elements is lost, so we need to use $push or $addToSet to keep track of original values
            Operators.unwind("originalValues"),
            # to be able to apply $subtract to each element of the array -- this adds the $ in front of the variable name
            Operators.project(field=None, projected_value={
                "theQuads": {"$pow": [{"$divide": [{"$subtract": ["$originalValues", "$mymean"]}, "$mystdDev"]}, 4]},
                "theSquares": {"$pow": [{"$divide": [{"$subtract": ["$originalValues", "$mymean"]}, "$mystdDev"]}, 3]}}),
            Operators.group_by(group_key="$_id", groups=[
                {"name": "sumQuads", "operator": "$sum", "field": "$theQuads"},
                {"name": "sumSquares", "operator": "$sum", "field": "$theSquares"},
                {"name": "summedValues", "operator": "$push", "field": "$theQuads"}
                # to later compute the number of values, we keep track of all the elements that have been summed
            ]),
            Operators.project(field=None, projected_value={"sumQuads": 1, "sumSquares": 1, "n": {"$size": "$summedValues"}}),
            Operators.project(field=None, projected_value={
                "kurtosis": {
                    "$cond": {
                        "if": {"$eq": ["$n", 0]},
                        "then": 0,
                        "else": {
                            "$subtract": [
                                {"$multiply": [
                                    {"$divide": [
                                        {"$multiply": ["$n", {"$sum": ["$n", 1]}]},
                                        {"$multiply": [{"$sum": ["$n", -1]}, {"$sum": ["$n", -2]}, {"$sum": ["$n", -3]}]}
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
                        "if": {"$eq": ["$n", 0]},
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

    @classmethod
    def iqr_query(cls, features_ids: list) -> list:
        return [
            Operators.match(field="instantiates", value={"$in": features_ids}, is_regex=False),
            Operators.sort(field="value", sort_order=1),
            Operators.group_by(group_key="$instantiates", groups=[{"name": "values", "operator": "$push", "field": "$value"}]),  # Collect all values in an array
            Operators.project(field=None, projected_value={"size": {"$size": "$values"},  "values": 1}),
            Operators.project(field=None, projected_value={
                "q1_index": {"$floor": {"$multiply": ["$size", 0.25]}},   # Calculate the Q1 index(floor(size / 4))
                "q3_index": {"$floor": {"$multiply": [{"$size": "$values"}, 0.75]}},  # Calculate the Q3 index(floor(3 * size / 4))
                "values": 1
            }),
            Operators.project(field=None, projected_value={
                "q1": {"$arrayElemAt": ["$values", "$q1_index"]},  # Get the Q1 value
                "q3": {"$arrayElemAt": ["$values", "$q3_index"]},  # Get the Q3 value
                "values": 1
            }),
            Operators.project(field=None, projected_value={"iqr": {"$subtract": ["$q3", "$q1"]}})  # compute the IQR as Q3-Q1
        ]

    @classmethod
    def pearson_correlation_query(cls, features_ids: list, database: Database) -> list:
        # first part: create pairs of features for which pearson coefficient will be computed
        # we only take the top-10 of numeric features, i.e.,
        # the 10 numeric features with most values (if tied, take the ones with the lowest ID)
        top_10_features = [
            Operators.match(field="instantiates", value={"$in": features_ids}, is_regex=False),
            Operators.group_by(group_key="$instantiates", groups=[
                {"name": "frequency", "operator": "$sum", "field": 1},
                {"name": "thesum", "operator": "$sum", "field": "$value"},
            ]),
            Operators.sort_many(map_field_order={"frequency": -1, "thesum": -1, "identifier": 1}),
            Operators.limit(10),
            Operators.project(field=None, projected_value={"identifier": "$_id", "_id": 0}),
            Operators.write_to_table(table_name=TableNames.TOP10_FEATURES)
        ]
        log.info(top_10_features)
        database.db[TableNames.RECORD].aggregate(top_10_features)

        pairs_features = [
            Operators.cartesian_product(join_table_name=TableNames.TOP10_FEATURES, field_b="_id",  # the view contains the Feature IDS in the field _id
                                        lookup_field_name="features_b", filter_dict={}),
            # Operators.project(field=None, projected_value={"feat_a": "$identifier", "feat_b": "$features_b.identifier"}),
            Operators.unwind(field="features_b"),
            Operators.project(field=None, projected_value={
                "feat_a": "$identifier",
                "feat_b": "$features_b.identifier"
            }),
            # Operators.group_by(group_key="$ordered_pair", groups=[]),
            Operators.match(field=None, value={"$expr": {"$lt": ["$feat_a", "$feat_b"]}}, is_regex=False),
            Operators.project(field=None, projected_value={"_id": 0}),  # this allows to not export the _id in the table (which contains duplicated due to unwind), and _id is regenerated when creating the table
            Operators.write_to_table(TableNames.PAIRS_FEATURES)
        ]
        log.info(pairs_features)
        database.db[TableNames.TOP10_FEATURES].aggregate(pairs_features)

        # second part, compute the Pearson coefficient for each pair of features stored above within the collection
        return [
            Operators.lookup(join_table_name=TableNames.RECORD, field_table_1="instantiates", field_table_2="feat_a", lookup_field_name="x"),
            Operators.lookup(join_table_name=TableNames.RECORD, field_table_1="instantiates", field_table_2="feat_b", lookup_field_name="y"),
            Operators.project(field=None, projected_value={"values": {"$zip": {"inputs": ["$x", "$y"]}}}),
            Operators.unwind(field="values"),
            Operators.project(field=None, projected_value={"x": {"$arrayElemAt": ["$values", 0]}, "y": {"$arrayElemAt": ["$values", 1]}}),
            Operators.project(field=None, projected_value={"x": "$x.value", "y": "$y.value", "feat_a": "$x.instantiates", "feat_b": "$y.instantiates"}),
            Operators.group_by(group_key={"feat_a": "$feat_a", "feat_b": "$feat_b"}, groups=[
                {"name": "originalX", "operator": "$push", "field": "$x"},
                {"name": "originalY", "operator": "$push", "field": "$y"},
                {"name": "avgX", "operator": "$avg", "field": "$x"},
                {"name": "avgY", "operator": "$avg", "field": "$y"},
            ]),
            Operators.project(field=None, projected_value={
                # to subtract/map a number to each element of an array, the syntax is a bit more complex than "Xsubs": {"$subtract": ["$originalX", "$avgX"]}
                # it requires a map
                "subsX": {"$map": {"input": "$originalX", "as": "originalXval", "in": {"$subtract": ["$$originalXval", "$avgX"]}}},
                "subsY": {"$map": {"input": "$originalY", "as": "originalYval", "in": {"$subtract": ["$$originalYval", "$avgY"]}}}
            }),
            Operators.project(field=None, projected_value={
                "subsX": 1,
                "subsY": 1,
                "subsSquareX": {"$map": {"input": "$subsX", "as": "subsXVal", "in": {"$pow": ["$$subsXVal", 2]}}},
                "subsSquareY": {"$map": {"input": "$subsY", "as": "subsYVal", "in": {"$pow": ["$$subsYVal", 2]}}},
                "multXY": {"$map": {"input": {"$zip": {"inputs": ["$subsX", "$subsY"]}}, "as": "pair",
                    "in": {"$multiply": [{"$arrayElemAt": ["$$pair", 0]}, {"$arrayElemAt": ["$$pair", 1]}]}
                }}
            }),
            Operators.project(field=None, projected_value={
                "sumXsubsSquare": {"$sum": "$subsSquareX"},  # no need to group by in this case (and the group by does not work)
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
            Operators.group_by(group_key="$_id.feat_a", groups=[
                {"name": "feat_b_dict", "operator": "$push", "field": {"k": "$_id.feat_b", "v": "$pearson"}}
            ]),
            Operators.project(field=None, projected_value={
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

    @classmethod
    def imbalance_query(cls, features_ids: list) -> list:
        # Ratio between the number of appearances of the most frequent value and the least frequent value.
        return [
            Operators.match(field="instantiates", value={"$in": features_ids}, is_regex=False),
            Operators.group_by(group_key={"instantiates": '$instantiates', "value": "$value"}, groups=[
                {"name": "frequency", "operator": "$sum", "field": 1}
            ]),
            Operators.group_by(group_key={"_id": "$_id.instantiates"}, groups=[
                {"name": "max_freq", "operator": "$max", "field": "$frequency"},
                {"name": "min_freq", "operator": "$min", "field": "$frequency"},
            ]),
            Operators.project(field=None, projected_value={
                "_id": "$_id._id",
                "imbalance": {"$divide": ["$max_freq", "$min_freq"]}
            })
        ]

    @classmethod
    def constancy_query(cls, features_ids: list) -> list:
        # Ratio between the number of appearances of the most frequent value and the number of non-null values.
        # However, we never have null values because we do not create Records for them
        # thus, the constancy is always 1 (for practical reasons, I divided the max freq by itself, to always obtain 1)
        return [
            Operators.match(field="instantiates", value={"$in": features_ids}, is_regex=False),
            Operators.group_by(group_key={"instantiates": '$instantiates', "value": "$value"}, groups=[
                {"name": "frequency", "operator": "$sum", "field": 1},
                {"name": "frequencyNonNull", "operator": "$sum", "field": 1}
            ]),
            Operators.group_by(group_key={"_id": "$_id.instantiates"}, groups=[
                {"name": "max_freq", "operator": "$max", "field": "$frequency"}
            ]),
            Operators.project(field=None, projected_value={
                "_id": "$_id._id",
                "constancy": {"$divide": ["$max_freq", "$max_freq"]}
            })
        ]

    @classmethod
    def mode_query(cls, features_ids: list) -> list:
        # the most frequent value
        return [
            Operators.match(field="instantiates", value={"$in": features_ids}, is_regex=False),
            Operators.group_by(group_key={"instantiates": '$instantiates', "value": "$value"}, groups=[
                {"name": "frequency", "operator": "$sum", "field": 1},
            ]),
            Operators.sort_many({"_id.instantiates": 1, "frequency": -1}),
            Operators.group_by(group_key={"_id": "$_id.instantiates"}, groups=[
                {"name": "values", "operator": "$push", "field": "$_id.value"}
            ]),
            Operators.project(field=None, projected_value={
                "_id": "$_id._id",
                "mode": {"$arrayElemAt": ["$values", 0]}
            })
        ]

    @classmethod
    def uniqueness_query(cls, features_ids: list) -> list:
        # Percentage of distinct values with respect to the total amount of non-null values
        return [
            Operators.match(field="instantiates", value={"$in": features_ids}, is_regex=False),
            Operators.group_by(group_key={"instantiates": '$instantiates', "value": "$value"}, groups=[
                {"name": "frequency", "operator": "$sum", "field": 1}]),
            Operators.group_by(group_key={"_id": "$_id.instantiates"}, groups=[
                {"name": "count", "operator": "$sum", "field": "$frequency"},
                {"name": "distinct_count", "operator": "$sum", "field": 1}
                ]
            ),
            Operators.project(field=None, projected_value={
                "_id": "$_id._id",
                "uniqueness": {"$divide": ["$distinct_count", "$count"]}
            })
        ]

    @classmethod
    def entropy_query(cls, features_ids: list) -> list:
        # Measure of uncertainty and disorder within the values of the column.
        # A large entropy means that the values are highly heterogeneous.
        return [
            Operators.match(field="instantiates", value={"$in": features_ids}, is_regex=False),
            Operators.group_by(group_key={"instantiates": '$instantiates', "value": "$value"}, groups=[
                {"name": "frequency", "operator": "$sum", "field": 1}]),
            Operators.group_by(group_key={"_id":"$_id.instantiates"}, groups=[
                {"name": "total", "operator": "$sum", "field": "$frequency"},
                {"name": "frequencies", "operator": "$push", "field": "$frequency"}
            ]),
            Operators.unwind("frequencies"),
            Operators.project(field=None, projected_value={
                "_id": "$_id._id",
                "prob": {"$divide": ["$frequencies", "$total"]}
            }),
            Operators.project(field=None, projected_value={
                "_id": "$_id",
                "entropy_value": {"$multiply": ["$prob", {"$log":["$prob", 2]}]}
            }),
            Operators.group_by(group_key={"_id": "$_id"}, groups=[
                {"name": "entropy", "operator": "$sum", "field": "$entropy_value"}
            ]),
            Operators.project(field=None, projected_value={
                "_id": "$_id._id",
                "entropy": {"$abs": "$entropy"}
            })
        ]

    @classmethod
    def density_query(cls, features_ids: list) -> list:
        # a measure of appropriate numerosity and intensity between different real-world entities available in the data
        return [
            Operators.match(field="instantiates", value={"$in": features_ids}, is_regex=False),
            Operators.group_by(group_key={"instantiates": '$instantiates', "value": "$value"}, groups=[
                {"name": "frequency", "operator": "$sum", "field": 1}]),
            Operators.group_by(group_key={"_id": "$_id.instantiates"}, groups=[
                {"name": "total", "operator": "$sum", "field": "$frequency"},
                {"name": "distinct_count", "operator": "$sum", "field": 1},
                {"name": "frequencies", "operator": "$push", "field": "$frequency"}
            ]),
            Operators.unwind("frequencies"),
            Operators.project(field=None, projected_value={
                "_id": "$_id._id",
                "prob": {"$divide": ["$frequencies", "$total"]},
                "distinct_count": "$distinct_count"
            }),
            Operators.group_by(group_key={"_id":"$_id"}, groups=[
                {"name": "avg_dens_value", "operator": "$avg", "field": "$prob"},
                {"name": "probs", "operator": "$push", "field": "$prob"},
                {"name": "distinct_count", "operator": "$first", "field":"$distinct_count"}
            ]),
            Operators.unwind("probs"),
            Operators.project(field=None, projected_value={
                "_id": "$_id._id",
                "density_value": {"$abs": {"$subtract": ["$probs", "$avg_dens_value"]}},
                "distinct_count": "$distinct_count"
            }),
            Operators.group_by(group_key={"_id": "$_id"}, groups=[
                {"name": "densities_sum", "operator": "$sum", "field": "$density_value"},
                {"name": "distinct_count", "operator": "$first", "field": "$distinct_count"}
            ]),
            Operators.project(field=None, projected_value={
                "_id": "$_id._id",
                "density": {"$divide": ["$densities_sum", "$distinct_count"]}
            })
        ]

