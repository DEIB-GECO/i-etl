import bson
import pandas as pd
import pymongo
from pymongo.mongo_client import MongoClient

from enums.DataTypes import DataTypes
from enums.MetadataColumns import MetadataColumns


def main_load_json_from_file_as_bson():
    with MongoClient() as mongo:
        db = mongo.get_database("mydb")
        with open("../datasets/test_dates/data.json", "r") as data_file:
            my_tuples = bson.json_util.loads(data_file.read())
            db["mycollectionIX"].insert_many(my_tuples, ordered=False)


def main_build_upsert():
    tuples = [{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "d": 6}]
    unique_variables = ["a", "d"]

    operations = [pymongo.UpdateOne(
        filter={unique_variable: one_tuple[unique_variable] for unique_variable in unique_variables if unique_variable in one_tuple},
        update={"$set": one_tuple}, upsert=True) for one_tuple in tuples
    ]
    print(operations)


def main_na_pandas():
    na_values = ["no information", "No information", "No Information", "-", "0", "0.0", "-0", "-0.0"]
    df = pd.read_csv("/Users/nelly/Documents/boulot/postdoc-polimi/etl/datasets/test/orig-data-clin.csv", index_col=False, dtype=str, na_values=[], keep_default_na=False)
    print(df)
    print(df.columns)

    print(df)
    for column in df:
        df.loc[:, column] = df[column].apply(lambda x: MetadataColumns.normalize_value(column_value=x))
    print(df)

    for row in df.itertuples(index=False):
        print(row)
        for column_name in df.columns:
            value = row[df.columns.get_loc(column_name)]
            print(f"      '{value}' (type={type(value)}, None={value is None}, Null={pd.isnull(value)}, empty={value == ""})")


def main_build_mongodb_types():
    # key is the Mongodb type, value is the ETL type
    reverse_mapping = {
        "int": DataTypes.INTEGER,
        "double": DataTypes.FLOAT,
        "string": DataTypes.STRING,
        "date": DataTypes.DATE,
        "timestamp": DataTypes.DATETIME,
        "bool": DataTypes.BOOLEAN,
        "object": DataTypes.CATEGORY,
        # "string": Datatypes.IMAGE,
        # "string": Datatypes.REGEX,
        # "string": Datatypes.API,
        # "string": Datatypes.LIST,
    }
    type_switch = {
        "$switch": {
            "branches": [
                {
                    "case": {"$eq": ["$_id.featureType", bson_type]},
                    "then": user_alias
                }
                for bson_type, user_alias in reverse_mapping.items()
            ],
            "default": "unknown"
        }
    }
    print(type_switch)


if __name__ == '__main__':
    main_build_mongodb_types()
    print("Done.")
