import os.path
import sys
import argparse

from database.Database import Database
from database.Execution import Execution
from enums.UpsertPolicy import UpsertPolicy

sys.path.append('.')  # add the current project to the python path to be runnable in cmd-line

from etl.ETL import ETL
from enums.HospitalNames import HospitalNames
from utils.setup_logger import log

if __name__ == '__main__':

    # the code is supposed to be run like this:
    # python3 main.py <hospital_name> <path/to/data.csv> <drop_db>
    # all parameters are required to avoid running with default (undesired configuration)
    parser = argparse.ArgumentParser()
    parser.add_argument("--"+Execution.HOSPITAL_NAME_KEY,
                        help="Set the hospital name among " + str(),
                        choices={hn for hn in HospitalNames.values()},
                        required=True)
    parser.add_argument("--"+Execution.CLINICAL_METADATA_PATH_KEY,
                        help="Set the absolute path to the metadata file.",
                        required=True)
    parser.add_argument("--"+Execution.CLINICAL_DATA_PATHS_KEY,
                        help="Set the absolute path to one or several data files, separated with commas (,) if applicable.",
                        required=True)
    parser.add_argument("--"+Execution.USE_EN_LOCALE_KEY,
                        help="Whether to use the en_US locale instead of the one automatically assigned by the ETL.",
                        choices={"True", "False"},
                        required=True)
    parser.add_argument("--"+Execution.DB_CONNECTION_KEY,
                        help="The connection string to the mongodb server.",
                        required=True)
    parser.add_argument("--"+Execution.DB_NAME_KEY,
                        help="Set the database name.",
                        required=True)
    parser.add_argument("--"+Execution.DB_DROP_KEY,
                        help="Whether to drop the database.",
                        choices={"True", "False"},
                        required=True)
    parser.add_argument("--"+Execution.IS_EXTRACT_KEY,
                        help="Whether to perform the Extract step of the ETL.",
                        choices={"True", "False"},
                        required=True)
    parser.add_argument("--"+Execution.IS_ANALYZE_KEY,
                        help="Whether to perform a data analysis on the provided files.",
                        choices={"True", "False"},
                        required=True)
    parser.add_argument("--"+Execution.IS_TRANSFORM_KEY,
                        help="Whether to perform the Transform step of the ETL.",
                        choices={"True", "False"},
                        required=True)
    parser.add_argument("--"+Execution.IS_LOAD_KEY,
                        help="Whether to perform the Load step of the ETL.",
                        choices={"True", "False"},
                        required=True)
    parser.add_argument("--"+Execution.DB_UPSERT_POLICY_KEY,
                        help="Whether to update or do nothing when upserting tuples.",
                        choices=[upsert_policy for upsert_policy in UpsertPolicy.values()],
                        required=True)

    args = parser.parse_args()
    execution = Execution(args.db_name)
    database = None
    try:
        execution.set_up_with_user_params(args)
        database = Database(execution=execution)

        # if database.client is not None and database.db is not None and execution.has_no_none_attributes():
        etl = ETL(execution=execution, database=database)
        etl.run()
        log.info(f"ETL has finished. Writing logs in files before exiting.")
        database.close()
        # else:
        #    log.error("The initial setup of the database has failed. Writing logs in files before exiting.")
    except Exception as e:
        if database is not None:
            # we initiated a connection, something failed, so now we need to release the connection
            database.close()
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        log.error(f"{type(e).__name__} exception: {e}")
        raise
