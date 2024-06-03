import os.path
import sys

from src.etl.ETL import ETL
from src.utils.HospitalNames import HospitalNames
from src.utils.prepare_for_etl import prepare_for_etl
from utils.setup_logger import log


if __name__ == '__main__':

    log.info("Hello!")

    argList = sys.argv
    log.info("Argument list is: " + str(argList))

    # the code is supposed to be run like this:
    # python3 main.py <hospital_name> <path/to/data.csv> <reset_db>
    if len(argList) != 4:
        raise RuntimeError(
            "This script expects 3 arguments: hospital name, file path to variable description, file path to "
            "samples, and a boolean indicating whether to reset the database. "
            "Please make sure that you provided them correctly.")

    # argList[0] is the script filename, i.e., main.py
    input_hospital_name = argList[1].upper()
    if input_hospital_name not in HospitalNames:
        log.error("The hospital name %s is not recognised.",  input_hospital_name)
        log.error("Please choose among: %s.", [hn.value for hn in HospitalNames])
        exit()
    samples_filepath = argList[2]
    reset_db_str = argList[3]  # cannot use bool(argList[3]) because this always returns True
    if reset_db_str.lower() == "true":
        reset_db = True
    else:
        reset_db = False

    processed_hospital_name = prepare_for_etl(metadata_filename=os.path.join("working-dir", "metadata", "metadata-UC2.csv"), hospital_name=input_hospital_name)

    # metadata_filepath =
    # etl = ETL(hospital_name=input_hospital_name, metadata_filepath=variables_filepath, samples_filepath=samples_filepath,
    #           reset_db=reset_db)
    # etl.run()

    log.info("Goodbye!")
