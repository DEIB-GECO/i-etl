import os.path
import sys
import argparse

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
    parser.add_argument("--clinical_metadata_filepath", help="Set the absolute path to the metadata file.", required=True)
    parser.add_argument("--clinical_filepaths",
                        help="Set the absolute path to one or several data files, separated with commas (,) if applicable.",
                        required=True)
    parser.add_argument("--use_en_locale",
                        help="Whether to use the en_US locale instead of the one automatically assigned by the ETL.",
                        choices={"True", "False"}, required=True)
    parser.add_argument("--db_connection", help="The connection string to the mongodb server.", required=True)
    parser.add_argument("--db_name", help="Set the database name.", required=True)
    parser.add_argument("--db_drop", help="Whether to drop the database.", choices={"True", "False"}, required=True)
    parser.add_argument("--db_no_index",
                        help="Whether to NOT compute the indexes after the data is loaded in the database.",
                        choices={"True", "False"}, required=True)
    parser.add_argument("--extract", help="Whether to perform the Extract step of the ETL.", choices={"True", "False"},
                        required=True)
    parser.add_argument("--analyze", help="Whether to perform a data analysis on the provided files.",
                        choices={"True", "False"}, required=True)
    parser.add_argument("--transform", help="Whether to perform the Transform step of the ETL.",
                        choices={"True", "False"}, required=True)
    parser.add_argument("--load", help="Whether to perform the Load step of the ETL.", choices={"True", "False"},
                        required=True)

    args = parser.parse_args()
    execution = Execution(args.db_name)
    try:
        execution.set_up_with_user_params(args)
        database = Database(execution=execution)

        # if database.client is not None and database.db is not None and execution.has_no_none_attributes():
        etl = ETL(execution=execution, database=database)
        etl.run()
        log.info(f"ETL has finished. Writing logs in files before exiting.")
        # else:
        #    log.error("The initial setup of the database has failed. Writing logs in files before exiting.")
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        log.error(f"{type(e).__name__} exception: {e}")
        raise
