import os
import logging
import requests
import pandas as pd
import warnings
import pymongo
from pymongo import MongoClient

warnings.filterwarnings("ignore")
# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def load_model():
    """
    Load the existing csv file with the feature counts if it exists.
    Otherwise create an empty dataframe with the header (feature code, feature name, count)
    """
    try:
        logging.info("Loading model")
        path = os.environ.get("FEDERATED_MODEL_PATH", ".")
        model_file = "model.csv"
        fed_model_path = os.path.join(path, model_file)

        if os.path.exists(fed_model_path):
            logging.info("Model exists, loading model from file")
            df = pd.read_csv(fed_model_path)
        else:
            logging.info("Model does not exist, initializing model")
            # If the loaded model DataFrame is empty, initialize it with the structure of data
            df = pd.DataFrame(columns=["Feature_code", "Feature_label", "Count"])
            logging.info("Model created")

        return df, fed_model_path
    except Exception as e:
        logging.error("Error loading model: %s", e)
        raise


def load_data(mongo_url, db_name):
    """
    Load the FAIR data of the current database (station) into a pandas DataFrame.
    In this train, we load integer features and compute the number of values for each of them.
    """
    try:
        logging.info(f"Loading data from {mongo_url}. Will access database {db_name}.")

        client = MongoClient(mongo_url)
        db = client[db_name]

        features = db['Feature']
        the_features = []
        for feature in features.find():
            if "ontology_resource" in feature:
                # for features that are not mapped to an ontology resource
                the_features.append((feature['identifier'], feature['ontology_resource']['code'],
                                     feature['ontology_resource']['label']))
            else:
                the_features.append((feature['identifier'], None, ""))

        data_to_append = []
        records = db['Record']

        for feature_id, feature_code, feature_label in the_features:
            v = [x['value'] for x in records.find({"instantiates": feature_id})]
            if len(v) > 0:
                data_to_append.append({"Feature_code": feature_code, "Feature_label": feature_label, "Count": len(v)})

        df = pd.DataFrame(data_to_append)

        return df
    except Exception as e:
        logging.error("Error loading data: %s", e)
        raise


def train_model(df, data):
    """
    Merge the current data within the existing model (previous dataframes computed in other stations)
    """
    try:
        # return data
        logging.info("Training model (adding numbers)")

        # Merge the two DataFrames on the feature code, filling missing counts with 0
        print(df)
        print(data)
        merged_df = pd.merge(
            df, data, on=["Feature_code", "Feature_label"], how="outer", suffixes=("_model", "_data")
        )
        print(merged_df)
        merged_df["Count_model"].fillna(0, inplace=True)
        merged_df["Count_data"].fillna(0, inplace=True)
        print(merged_df)

        # Sum the 'Count' values from both DataFrames
        merged_df["Total Count"] = merged_df["Count_model"] + merged_df["Count_data"]
        print(merged_df)

        # Return the merged DataFrame with the new 'Total Count'
        return merged_df[["Feature_code", "Feature_label", "Total Count"]].rename(
            columns={"Total Count": "Count"}
        )
    except Exception as e:
        logging.error("Error occurred during model training: %s", e)
        raise


def save_model(df, fed_model_path):
    """
    Save the merged dataframe as the new model
    """
    try:
        logging.info("Saving model")
        df.to_csv(fed_model_path, index=False)
        logging.info("Model saved successfully")
    except Exception as e:
        logging.error("Error occurred while saving model: %s", e)
        raise


if __name__ == "__main__":
    try:
        # Read FHIR URL from environment variable
        mongo_url = os.environ.get("MONGO_URL")
        mongo_db = os.environ.get("MONGO_DB_NAME")

        # Load initial model (df) from file
        df, fed_model_path = load_model()

        # Load data (df) from MongoDB database in the station
        data = load_data(mongo_url, mongo_db)

        # Train the model (add the fetaure counts)
        result = train_model(df, data)

        # Save the model (df) to file
        save_model(result, fed_model_path)

        logging.info("Process completed successfully")
    except Exception:
        logging.error("An error occurred. Exiting...")
        exit()
