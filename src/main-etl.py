import os.path
import sys

from dotenv import load_dotenv

from database.Database import Database
from database.Execution import Execution
from preprocessing.PreprocessingTask import PreprocessingTask

sys.path.append('.')  # add the current project to the python path to be runnable in cmd-line

from etl.ETL import ETL
from utils.setup_logger import log

if __name__ == "__main__":

    # the code is supposed to be run like this:
    # python3 main.py
    # the code supposes to have a .env file next to main.py
    try:
        log.info("Load environment file")
        # A. load the env. variables defined in .env.
        # note: specifying the .env in the compose.yml only gives access to those env. var. to Docker (not to Python)
        load_dotenv(os.environ["MY_ENV_FILE"])

        # create a new execution instance for that run
        log.info("Create execution")
        execution = Execution()
        execution.internals_set_up()
        execution.file_set_up(setup_files=True)

        # create the database instance (incl. connection)
        database = Database(execution=execution)

        # if database.client is not None and database.db is not None and execution.has_no_none_attributes():
        etl = ETL(execution=execution, database=database)
        etl.run()
        log.info(f"ETL has finished. Writing logs in files before exiting.")
        database.close()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        log.error(f"{type(e).__name__} exception: {e}")
        raise
