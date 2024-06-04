import configparser
import os.path
import sys
import argparse
from datetime import datetime
import shutil

from src.etl.ETL import ETL
from src.utils.HospitalNames import HospitalNames
from src.utils.utils import current_milli_time
from utils.setup_logger import log


if __name__ == '__main__':
    # create a config file using properties.ini
    config = configparser.ConfigParser()
    config.read("properties.ini")

    # the code is supposed to be run like this:
    # python3 main.py <hospital_name> <path/to/data.csv> <reset_db>
    parser = argparse.ArgumentParser()
    parser.add_argument("--hospital_name", help="Set the hospital name among " + str([hn.value for hn in HospitalNames]), choices={hn.value for hn in HospitalNames})
    parser.add_argument("--metadata_filepath", help="Set the absolute path to the metadata file.")
    parser.add_argument("--data_filepath", help="Set the absolute path to the data file.")
    parser.add_argument("--database_name", help="Set the database name.")
    parser.add_argument("--reset", help="Whether to reset the database.", choices={"True", "False"})
    parser.add_argument("--connection", help="The connection string to the mongodb server.")

    args = parser.parse_args()
    if args.hospital_name is not None:
        config.set("HOSPITAL", "name", args.hospital_name)
    if args.connection is not None:
        config.set("DATABASE", "connection", args.connection)
    if args.database_name is not None:
        config.set("DATABASE", "name", args.database_name)
    if args.reset is not None:
        config.set("DATABASE", "reset", args.reset)

    # create a new folder within the tmp dir to store the current execution tmp files and config
    # this folder is named after the DB name (instead of a timestamp, which will create one folder at each run)
    working_folder = os.path.join(config.get("FILES", "working_dir"), config.get("DATABASE", "name"))
    config.set("FILES", "working_dir_current", working_folder)
    if os.path.exists(working_folder):
        shutil.rmtree(working_folder)  # empty the current working directory if it exists
    os.makedirs(working_folder)  # create the working folder (labelled with the DB name)

    # get metadata and data filepaths
    if args.metadata_filepath is None:
        log.error("No metadata file path has been provided. Please provide one.")
        exit()
    elif not os.path.isfile(args.metadata_filepath):
        log.error("The specified metadata file does not seem to exist. Please check the path.")
        exit()
    else:
        metadata_filename = "metadata-" + args.hospital_name + ".csv"
        metadata_filepath = os.path.join(config.get("FILES", "working_dir_current"), metadata_filename)
        shutil.copyfile(args.metadata_filepath, metadata_filepath)
        config.set("FILES", "metadata_filepath", metadata_filepath)

    if args.data_filepath is None:
        log.error("No data file path has been provided. Please provide one.")
        exit()
    elif not os.path.isfile(args.data_filepath):
        log.error("The specified data file does not seem to exist. Please check the path.")
        exit()
    else:
        # we do not copy the data in our working dir because it is too large to be copied
        data_filepath = args.data_filepath
        config.set("FILES", "data_filepath", data_filepath)

    # save the config file in the current working directory
    saved_config_file = os.path.join(working_folder, "command-line.txt")
    with open(saved_config_file, 'w') as f:
        config.write(f)

    # print the main parameters of the current run
    log.info("Selected hospital name: %s", config.get("HOSPITAL", "name"))
    log.info("The database name is %s", config.get("DATABASE", "name"))
    log.info("The connection string is: %s", config.get("DATABASE", "connection"))
    log.info("The database will be reset: %s", config.get("DATABASE", "reset"))
    log.info("The metadata file is located at: %s", config.get("FILES", "metadata_filepath"))
    log.info("The data file is located at: %s", config.get("FILES", "data_filepath"))

    etl = ETL(config=config, metadata_filepath=metadata_filepath, data_filepath=data_filepath)
    etl.run()

    log.info("Goodbye!")
