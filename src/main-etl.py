import os.path
import sys

from database.Database import Database
from database.Execution import Execution

sys.path.append('.')  # add the current project to the python path to be runnable in cmd-line

from etl.ETL import ETL
from utils.setup_logger import log

if __name__ == "__main__":

    # the code is supposed to be run like this:
    # python3 main.py
    # the code supposes to have a .env file next to main.py
    try:
        execution = Execution()
        execution.set_up(setup_data_files=True)
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
