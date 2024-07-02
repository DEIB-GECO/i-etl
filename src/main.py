import logging
import os.path
import pathlib
import sys
import argparse
import shutil
import traceback

from database.Database import Database
from database.Execution import Execution

sys.path.append('.')  # add the current project to the python path to be runnable in cmd-line

from etl.ETL import ETL
from utils.HospitalNames import HospitalNames
from utils.setup_logger import log

if __name__ == '__main__':

    # the code is supposed to be run like this:
    # python3 main.py <hospital_name> <path/to/data.csv> <drop_db>
    # all parameters are required to avoid running with default (undesired configuration)
    parser = argparse.ArgumentParser()
    parser.add_argument("--hospital_name",
                        help="Set the hospital name among " + str([hn.value for hn in HospitalNames]),
                        choices={hn.value for hn in HospitalNames}, required=True)
    parser.add_argument("--metadata_filepath", help="Set the absolute path to the metadata file.", required=True)
    parser.add_argument("--data_filepaths",
                        help="Set the absolute path to one or several data files, separated with commas (,) if applicable.",
                        required=True)
    parser.add_argument("--use_en_locale",
                        help="Whether to use the en_US locale instead of the one automatically assigned by the ETL.",
                        choices={"True", "False"}, required=True)
    parser.add_argument("--connection", help="The connection string to the mongodb server.", required=True)
    parser.add_argument("--database_name", help="Set the database name.", required=True)
    parser.add_argument("--drop", help="Whether to drop the database.", choices={"True", "False"}, required=True)
    parser.add_argument("--no_index",
                        help="Whether to NOT compute the indexes after the data is loaded in the database.",
                        choices={"True", "False"}, required=True)
    parser.add_argument("--extract", help="Whether to perform the Extract step of the ETL.", choices={"True", "False"},
                        required=True)
    parser.add_argument("--analysis", help="Whether to perform a data analysis on the provided files.",
                        choices={"True", "False"}, required=True)
    parser.add_argument("--transform", help="Whether to perform the Transform step of the ETL.",
                        choices={"True", "False"}, required=True)
    parser.add_argument("--load", help="Whether to perform the Load step of the ETL.", choices={"True", "False"},
                        required=True)

    args = parser.parse_args()
    execution = Execution()
    execution.set_up(args)
    database = Database(execution=execution)

    if database.client is not None and database.db is not None and execution.has_no_none_attributes():
        etl = ETL(execution=execution, database=database)
        etl.run()
        log.info("ETL has finished. Writing logs in files before exiting.")
    else:
        log.error("The initial setup of the database has failed. Writing logs in files before exiting.")

    # everything has been written in the log file,
    # so we move it (the file with the latest timestamp) to its respective database folder in working-dir
    # first: close handlers that were writing the log files
    log.handlers.clear()
    # now we can move the latest log file to its destination
    latest_log_filename = max([f for f in pathlib.Path('.').glob('*.log')], key=os.path.getctime)
    shutil.move(latest_log_filename, os.path.join(execution.get_working_dir_current(), latest_log_filename))
    log.info("Writing logs in files has finished. Exiting.")
