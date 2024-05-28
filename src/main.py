import sys

from src.analysis.ValueAnalysis import ValueAnalysis
from src.etl.ETL import ETL
from utils.setup_logger import log

if __name__ == '__main__':

    log.info("Hello!")

    argList = sys.argv
    log.info("Argument list is: " + str(argList))

    # the code is supposed to be run like this:
    # python3 main.py Buzzi path/to/metadata.csv path/to/data.csv true
    if len(argList) != 5:
        raise RuntimeError(
            "This script expects 4 arguments: hospital name, file path to variable description, file path to "
            "samples, and a boolean indicating whether to reset the database. "
            "Please make sure that you provided them correctly.")

    # argList[0] is the script filename, i.e., main.py
    hospital_name = argList[1]
    variables_filepath = argList[2]
    samples_filepath = argList[3]
    reset_db_str = argList[4]  # cannot use bool(argList[4]) because this always returns True
    if reset_db_str.lower() == "true":
        reset_db = True
    else:
        reset_db = False

    etl = ETL(hospital_name=hospital_name, metadata_filepath=variables_filepath, samples_filepath=samples_filepath,
              reset_db=reset_db)
    etl.run()

    log.info("Goodbye!")
