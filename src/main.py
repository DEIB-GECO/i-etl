import configparser
import getpass
import os.path
import platform
import sys
import argparse
from datetime import datetime
import shutil

from src.config.BetterConfig import BetterConfig
from src.etl.ETL import ETL
from src.utils.HospitalNames import HospitalNames
from utils.setup_logger import log


if __name__ == '__main__':
    # create a config file using properties.ini
    config = BetterConfig()

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
        config.set_hospital_name(args.hospital_name)
    if args.connection is not None:
        config.set_db_connection(args.connection)
    if args.database_name is not None:
        config.set_db_name(args.database_name)
    if args.reset is not None:
        config.set_db_reset(args.reset)

    # create a new folder within the tmp dir to store the current execution tmp files and config
    # this folder is named after the DB name (instead of a timestamp, which will create one folder at each run)
    working_folder = os.path.join(config.get_working_dir(), config.get_db_name())
    config.set_working_dir_current(working_folder)
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
        metadata_filepath = os.path.join(config.get_working_dir_current(), metadata_filename)
        shutil.copyfile(args.metadata_filepath, metadata_filepath)
        config.set_metadata_filepath(metadata_filepath)

    if args.data_filepath is None:
        log.error("No data file path has been provided. Please provide one.")
        exit()
    elif not os.path.isfile(args.data_filepath):
        log.error("The specified data file does not seem to exist. Please check the path.")
        exit()
    else:
        # we do not copy the data in our working dir because it is too large to be copied
        config.set_data_filepath(args.data_filepath)

    # write more information about the current run in the config
    config.set_python_version(str(sys.version))
    config.set_execution_date(str(datetime.now()))
    config.set_platform(platform.platform())
    config.set_platform_version(platform.version())
    config.set_user(getpass.getuser())

    # save the config file in the current working directory
    saved_config_file = os.path.join(working_folder, "parameter.ini")
    config.write_to_file(saved_config_file)

    # print the main parameters of the current run
    log.info("Selected hospital name: %s", config.get_hospital_name())
    log.info("The database name is %s", config.get_db_name())
    log.info("The connection string is: %s", config.get_db_connection())
    log.info("The database will be reset: %s", config.get_db_reset())
    log.info("The metadata file is located at: %s", config.get_metadata_filepath())
    log.info("The data file is located at: %s", config.get_data_filepath())

    etl = ETL(config=config)
    etl.run()

    log.info("Goodbye!")
